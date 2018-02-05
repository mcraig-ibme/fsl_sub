#!/usr/bin/env fslpython
import argparse
import copy
import getpass
import importlib
import logging
import os
import pkgutil
import re
import shutil
import socket
import subprocess
import sys
import yaml
from collections import defaultdict
from functools import lru_cache
from operator import itemgetter
from math import ceil

# ============================
# Configuration ends here
# ============================

VERSION = '2.0'


# Custom Exceptions
class ArgumentError(Exception):
    pass


class LoadModuleError(Exception):
    pass


class NoModule(Exception):
    pass


class PluginError(Exception):
    pass


class BadCoprocessor(Exception):
    pass


class BadConfiguration(Exception):
    pass


class UnrecognisedModule(Exception):
    pass


@lru_cache
def parallel_envs(queues):
    '''Return the list of configured parallel environments
    in the supplied queue definition dict'''
    ll_envs = []
    for q in queues:
        try:
            ll_envs.extend(q['parallel_envs'])
        except KeyError:
            pass
    return list(set(ll_envs))


@lru_cache
def find_module_cmd():
    '''Locate the 'modulecmd' binary'''
    return shutil.which('modulecmd')


def read_module_environment(lines):
    '''Given output of modulecmd python add ... convert this to a dict'''
    module_env = {}
    regex = re.compile(r"os.environ\['(?P<variable>.*)'\] = '(?P<value>.*)'$")
    for line in lines:
        matches = regex.match(line.strip())
        if matches:
            module_env[matches.group('variable')] = matches.group('value')
    return module_env


def module_add(module_name):
    '''Returns a dict of variable: value describing the environment variables
    necessary to load a shell module into the current environment'''
    module_cmd = find_module_cmd()

    if module_cmd:
        try:
            environment = system_stdout(
                (module_cmd, "python", "add", module_name, ), shell=True)
        except subprocess.CalledProcessError as e:
            raise LoadModuleError from e
        return read_module_environment(environment)
    else:
        return False


def load_module(module_name):
    '''Load a module into the environment of this python process.'''
    environment = module_add(module_name)
    if environment:
        for k, v in environment.items():
            os.environ[k] = v
        return True
    else:
        return False


def unload_module(module_name):
    '''Remove environment variables associated with module module_name
     from the environment of python process.'''
    environment = module_add(module_name)
    if environment:
        for k in environment:
            del os.environ[k]
        return True
    else:
        return False


def loaded_modules():
    '''Get list of loaded ShellModules'''
    # Modules stored in environment variable LOADEDMODULES
    try:
        modules_string = os.environ['LOADEDMODULES']
    except KeyError:
        return []
    return modules_string.split(':')


@lru_cache
def get_modules(module_parent):
    '''Returns a list of available Shell Modules that setup the
    co-processor environment'''

    modules = []
    try:
        available_modules = system_stdout(
            ["module", "-t", "avail", module_parent],
            shell=True)
        for line in available_modules:
            line = line.strip()
            if ':' in line:
                continue
            if '/' in line:
                modules += line.split('/')[1]
            else:
                modules += None
    except subprocess.CalledProcessError as e:
        raise NoModule(module_parent)
    return modules.sort()


def latest_module(module_parent):
    '''Return the module string that would load the latest version of a module.
    Returns False if module is determined to be not versioned and raises
    NoModule if module is not found.'''
    try:
        modules = get_modules(module_parent)
        if modules is None:
            return False
        else:
            return modules[-1]
    except NoModule as e:
        raise


def module_string(module_parent, module_version):
    if module_version:
        return "/".join((module_parent, module_version))
    else:
        return module_parent


@lru_cache
def list_coprocessors(config):
    '''Return a list of coprocessors found in the queue definitions'''

    avail_cops = []

    for q in config['queues']:
        try:
            avail_cops.extend(q['copros'].keys())
        except KeyError:
            pass

    return avail_cops


@lru_cache
def max_coprocessors(config, coprocessor):
    '''Return the maximum number of coprocessors per node from the
    queue definitions'''

    num_cops = 0

    for q in config['queues']:
        if 'copros' in q:
            try:
                num_cops = max(
                    num_cops,
                    q['copros'][coprocessor]['max_quantity'])
            except KeyError:
                pass

    return num_cops


@lru_cache
def copro_classes(config, coprocessor):
    '''Return whether a coprocessor supports multiple classes of hardware.
    Classes are sorted by capability'''
    classes = defaultdict(lambda: 1)
    copro_opts = config['copro_opts'][coprocessor]
    for q in config['queues']:
        if 'copros' in q:
            try:
                for c in q['copros'][coprocessor]['classes']:
                    classes[c] = copro_opts[c]['capability']
            except KeyError:
                continue
    if not classes:
        return None
    return sorted(classes.keys(), key=classes.get)


def coprocessor_toolkits(config, coprocessor):
    '''Return list of coprocessor toolkit versions.'''
    if coprocessor not in config['copro_opts']:
        raise BadConfiguration(
            "Coprocessor {} not configured".format(coprocessor))
    # Check that we have queues configured for this coproceesor
    if not all([q for q in config['queues'] if (
            'copros' in q and coprocessor in q['copros'])]):
        raise BadConfiguration(
            "Coprocessor {} not available in any queues".format(
                coprocessor
            )
        )
    copro_conf = config['copro_opts'][coprocessor]
    if not copro_conf['uses_modules']:
        return None
    return get_modules(copro_conf['module_parent']).sort

def coproc_class(coproc_class, coproc_classes):
    try:
        for c, i in enumerate(coproc_classes):
            if c['shortcut'] == coproc_class:
                break
    except KeyError:
        raise BadConfiguration(
            "Co-processor class {} not configured".format(coproc_class),
            file=sys.stderr)
    return coproc_classes[:i]


def coproc_load_module(coproc, module_version):
    if coproc['uses_modules']:
        modules_avail = get_modules(coproc['module_parent'])
        if modules_avail:
            if module_version not in modules_avail:
                raise UnrecognisedModule(module_version)
            else:
                load_module("/".join(
                    (coproc['module_parent'], module_version)))



def build_parser(config):
    '''Parse the command line, returns a dict keyed on option'''

    available_coprocessors = list_coprocessors(config)
    coprocessor_classes = {
        c: copro_classes(config, c) for c in available_coprocessors}
    coprocessor_toolkit = coprocessor_toolkits(config)
    ll_envs = parallel_envs(config['queues'])

    # Build the epilog...
    epilog = []
    if config['method'] != 'None':
        epilog += '''
Queues

There are several batch queues configured on the cluster:
        '''
        for qname, q in config.queues.items():
            epilog += (
                "{qname}: {timelimit} max run time; {q[slot_size]}GB "
                "per slot; {q[maxram]}GB total".format(
                    qname=qname,
                    timelimit=minutes_to_human(q['time']),
                    q=q,
                ))
            padding = " " * len(qname)
            if 'copros' in q:
                epilog += (
                    padding + "Coprocessors available: " +
                    "; ".join(q['copros'])
                )
            if 'parallel_envs' in q:
                epilog += (
                    padding + "Parallel environments available: " +
                    "; ".join(q['parallel_envs'])
                )
            if 'map_ram' in q and q['map_ram']:
                epilog += (
                    padding + "Supports splitting into multiple slots."
                )
    mconf = config[config['method']]
    if available_coprocessors:
        cp_versions = []
        for cp in available_coprocessors:
            try:
                cp_def = config['copro_opts'][cp]
            except KeyError:
                continue
            if find_module_cmd():
                if cp_def['uses_modules']:
                    try:
                        cp_versions.append(
                            "{copro}: {versions}".format(
                                copro=cp,
                                versions=','.join(
                                    get_modules('module_parent'))))
                    except NoModule as e:
                        raise BadConfiguration from e
            if cp_versions:
                epilog += (
                    "Co-processor toolkit versions available: "
                    "; ".join(cp))
            cp_classes = copro_classes(config, cp)
            if cp_classes:
                epilog += (
                    "Co-processor classes available: "
                )
                for cpclass in cp_classes:
                    epilog += (
                        ": ".join(
                            (cpclass, cp_def[cpclass]['doc'])
                        )
                    )

    parser = argparse.ArgumentParser(
        prog="fsl_sub",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description='FSL cluster submission.',
        epilog=epilog)
    job_mutex = parser.add_mutually_exclusive_group(required=True)
    advanced_g = parser.add_argument_group(
        'Advanced',
        'Advanced queueing options not typically required.')
    if mconf['mail_support']:
        email_g = parser.add_argument_group(
            'Emailing',
            'Email notification options.')
    copro_g = parser.add_argument_group(
        'Co-processors',
        'Options for requesting co-processors, e.g. GPUs')
    array_g = job_mutex.add_argument_group(
        'Array Tasks',
        'Options for sumitting and controlling array tasks.'
    )
    if mconf['architecture']:
        advanced_g.add_argument(
            '-a', '--arch',
            action='append',
            default=None,
            help="Architecture [e.g., lx-amd64].")
    copro_g.add_argument(
        '-c', '--coprocessor',
        action='append',
        default=None,
        choices=available_coprocessors,
        help="Request a co-processor, further details below.")
    copro_g.add_argument(
        '--coprocessor_class',
        default=None,
        choices=coprocessor_classes,
        help="Request a specific co-processor hardware class. "
        "Details of which classes are available for each co-processor "
        "are below."
    )
    copro_g.add_argument(
        '--coprocessor_class_strict',
        action='store_true',
        help="If set will only allow running on this class. "
        "The default is to use this class and all more capable devices."
    )
    copro_g.add_argument(
        '--coprocessor_toolkit',
        default=None,
        choices=coprocessor_toolkit,
        help="Request a specific version of the co-processor software "
        "tools. Will default to the latest version available. "
        "If you wish to use the toolkit defined in your current "
        " environment, give the value '-1' to this argument."
    )
    copro_g.add_argument(
        '--coprocessor_multi',
        default=1,
        help="Request multiple co-processors for a job. This make take "
        "the form of simple number to a complex definition of devices. "
        "See your cluster documentation for details."
    )
    advanced_g.add_argument(
        '-F', '--usescript',
        action='store_true',
        help="Use flags embedded in scripts to set queuing options - "
        "all other options ignored."
    )
    parser.add_argument(
        '-j', '--jobhold',
        default=None,
        help="Place a hold on this task until specified job id has "
        "completed."
    )
    array_g.add_argument(
        '--parallel_hold',
        default=None,
        help="Place a parallel hold on the specified array task. Each"
        "sub-task is held until the equivalent sub-task in the"
        "parent array task completes."
    )
    parser.add_argument(
        '-l', '--logdir',
        default=os.getcwd(),
        help="Where to output logfiles."
    )
    if mconf['mail_support']:
        email_g.add_argument(
            '-m', '--mailoptions',
            default=None,
            help="Specify job mail options, see your queuing software for "
            "details."
        )
        email_g.add_argument(
            '-M', '--mailto',
            default="{username}@{hostname}.".format(
                        username=getpass.getuser(),
                        hostname=socket.gethostname()
                    ),
            help="Who to email."
        )
    parser.add_argument(
        '-n', '--novalidation',
        action='store_true',
        help="Don't check for presence of script/binary in your search"
        "path (use where the software is only available on the "
        "compute node)."
    )
    parser.add_argument(
        '-N', '--name',
        default=None,
        help="Specify jobname as it will appear on queue. If not specified "
        "then the job name will be the name of the script/binary submitted."
    )
    advanced_g.add_argument(
        '-p', '--priority',
        default=0,
        choices=range(0, -1024),
        help="Specify a lower job priority (where supported)."
        "Takes a negative integer."
    )
    parser.add_argument(
        '-q', '--queue',
        default=None,
        help="Select a particular queue - see below for details. "
        "Instead of choosing a queue try to specify the time required."
    )
    advanced_g.add_arguemt(
        '-r', '--resource',
        default=None,
        action='append',
        help="Pass a resource request string through to the job "
        "scheduler. See your scheduler's instructions for details"
    )
    parser.add_argument(
        '-R', '--jobram',
        default=None,
        help="Max total RAM to use for job (integer in GB). "
        "This is very important to set if your job requires more "
        "than the queue slot memory limit as then you job can be "
        "split over multiple slots automatically - see autoslotsbyram."
    )
    advanced_g.add_argument(
        '-s', '--parallelenv',
        default=None,
        help="Takes a comma-separated argument <pename>,<threads>."
        "Submit a multi-threaded (or resource) task - requires a "
        "parallel environment (<pename>) to be configured on the "
        "requested queues. <threads> specifies the number of "
        "threads/hosts required. e.g. '{pe_name},2'.".format(
            pe_name=ll_envs[0])
    )
    parser.add_argument(
        '-S', '--noramsplit',
        action='store_true',
        help="Disable the automatic requesting of a parallel "
        "environment with sufficient slots to allow your job to run."
    )
    array_g.add_argument(
        '-t', '--paralleltask',
        type=argparse.FileType('r'),
        help="Specify a task file of commands to execute in parallel."
    )
    array_g.add_argument(
        '--parallel_stride',
        default=1,
        help="For parallel task files, increment of sub-task ID between "
        "sub-tasks"
    )
    parser.add_argument(
        '-T', '--jobtime',
        default=None,
        help="Estimated job length in minutes, used to auto-choose the queue "
        "name."
    )
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help="Verbose mode."
    )
    parser.add_argument(
        '-V', '--version',
        action='version',
        version='%(prog)s ' + VERSION
    )
    advanced_g.add_argument(
        '-x', '--parallel_limit',
        default=None,
        help="Specify the maximum number of parallel job sub-tasks to run "
        "concurrently."
    )
    parser.add_argument(
        '-z', '--fileisimage',
        default=None,
        metavar='file',
        help="If the <file> file already exists, do nothing and exit."
    )
    job_mutex.add_argument('args', nargs=argparse.REMAINDER)
    return parser


def minutes_to_human(minutes):
    if minutes < 60:
        return "{}m".format(minutes)
    if minutes < 60 * 24:
        return "{:.1f}h".format(minutes/60)
    return "{:.1f}d".format(minutes/(60 * 24))


def main():
    logger = logging.getLogger('__name__')

    config = read_config()

    fsl_sub_plugins = {
        name: importlib.import_module(name)
        for finder, name, ispkg
        in pkgutil.iter_modules()
        if name.startswith('fsl_sub_')
    }

    try:
        already_run = os.environ['FSLSUBALREADYRUN']
    except KeyError:
        already_run = 'false'
    if config['method'] != 'None':
        if affirmative(already_run):
            config['method'] == 'None'
            print(
                'Warning: job on queue attempted to submit parallel jobs -'
                'running jobs serially instead.',
                file=sys.stderr
            )

    os.environ['FSLSUBALREADYRUN'] = 'true'

    grid_module = 'fsl_sub_' + config['method']
    if grid_module not in fsl_sub_plugins:
        raise BadConfiguration(
            "{} not a supported method".format(config['method']))

    try:
        submit = fsl_sub_plugins[grid_module].submit
        qfind = fsl_sub_plugins[grid_module].qfind
        BadSubmission = fsl_sub_plugins[grid_module].BadSubmission
    except AttributeError as e:
        raise BadConfiguration(
            "Failed to load plugin " + grid_module
        )

    config['qtest'] = qfind()
    if config['qtest'] is None:
        config['method'] == 'None'
        print(
            'Warning: fsl_sub configured for {0} but {0}'
            ' software not found.'.format(config['method']),
            file=sys.stderr
        )

    cmd_parser = build_parser(config)
    options = vars(cmd_parser.parse_args())

    if options['paralleltask']:
        command = ''
        if not options['novalidation']:
            options['task_numbers'] = check_command_file(
                options['paralleltask'])
        options['paralleltask'].close()
        options['paralleltask'] = options['paralleltask'].name
        task_name = os.path.basename(options['paralleltask'])

    if options['args']:
        command = options['args'][0]
        if not options['novalidation']:
            check_command(command)
        task_name = os.path.basename(command)

    if not options['jobname']:
        options['jobname'] = task_name

    if options['args']:
        logger.info(
            "METHOD={0} : args={1}".format(
                config['method'],
                " ".join(options['args'])
            ))
    else:
        logger.info(
            "METHOD={0} : parallel task file={1}".format(
                config['method'],
                options['paralleltask'])
            )

    method_options = options['method_opts'][config['method']]

    split_on_ram = method_options['map_ram'] and not options['noramsplit']
    if options['parallelenv']:
        options['pe'] = process_pe_def(
            options['parallelenv'], config['queues'])
        ll_env = options['pe']['name']
        slots = options['pe']['slots']
    else:
        if split_on_ram:
            ll_env = method_options['large_job_split_pe']
            slots = 1
        else:
            ll_env = None
            slots = 1

    if options['queue'] is None:
        (options['queue'], slots_required) = getq_and_slots(
                job_time=options['jobtime'],
                job_ram=options['jobram'],
                job_threads=slots,
                queues=config['queues'],
                coprocessor=options['coprocessor'],
                ll_env=ll_env
                )

        if options['queue'] is None:
            cmd_parser.error("Unable to find a queue with these parameters")
    else:
        slots_required = slots

    if not queue_exists(options['queue'], config['qtest']):
        cmd_parser.error("Invalid queue name specified!")

    if split_on_ram:
        options['jobram'] = split_ram_by_slots(
            options['jobram'],
            slots_required)

    control_threads(
        config['thread_control'],
        slots_required)

    if slots_required > 1:
        if options['pe']:
            if options['pe']['slots'] < slots_required:
                options['pe']['slots'] = slots_required
        elif ll_env:
            options['pe'] = {
                'name': ll_env,
                'slots': slots_required, }
        else:
            cmd_parser.error(
                "Job requires {} slots but no parallel envrionment "
                "available or requested".format(slots_required))

    if options['coprocessor']:
        coproc_load_module(
            config['copro_opts'][options['coprocessor']],
            options['coprocessor_toolkit'])

    try:
        job_id = submit(method_options, options)
    except BadSubmission as e:
        cmd_parser.error("Error submitting job:" + str(e))
    print(job_id)


def process_pe_def(pe_def, queues):
    '''Convert specified pe,slots into a tuples'''
    pes_defined = parallel_envs(queues)
    try:
        pe = pe_def.split(',')
    except ValueError:
        raise ArgumentError(
            "Parallel environment must be name,slots"
        )
    if pe[0] not in pes_defined:
        raise ArgumentError(
            "Parallel environment name {} "
            "not recognised".format(pe[0])
        )
    return {'name': pe[0], 'slots': pe[1], }


def control_threads(env_vars, threads):
    '''Set the specified environment variables to the number of
    threads.'''

    for ev in env_vars:
        os.environ[ev] = threads


def check_command(cmd):
    if shutil.which(cmd) is None:
        raise ArgumentError("Cannot find script/binary " + cmd)


def check_command_file(cmd_file):
    for lineno, line in enumerate(cmd_file.readlines()):
        try:
            check_command(line[0])
        except ArgumentError:
            raise ArgumentError(
                "Cannot find script/binary {0} on line {1}"
                "of {2}".format(line[0], lineno + 1, cmd_file.name))
    return lineno + 1


def queue_exists(qname, qtest):
    try:
        system([qtest, '-sq', qname])
    except subprocess.CalledProcessError:
        return False


def split_ram_by_slots(jram, jslots):
    return int(ceil(jram / jslots))


def affirmative(astring):
    answer = astring.lower()
    if answer == 'yes' or answer == 'y' or answer == 'true':
        return True
    else:
        return False


def negative(astring):
    answer = astring.lower()
    if answer == 'no' or answer == 'n' or answer == 'false':
        return True
    else:
        return False


def find_config_file():
    # Find most appropriate config file
    user_home = os.path.expanduser("~")
    personal_config = os.path.join(user_home, '.fsl_sub.yml')
    if os.path.exists(personal_config):
        return personal_config

    try:
        env_config = os.environ['FSLSUB_CONF']
        return env_config
    except KeyError:
        pass
    try:
        default_conf = os.path.join(
                os.environ['FSLDIR'], 'etc',
                'fslconf', 'fsl_sub.yml')
        if os.path.exists(default_conf):
            return default_conf
    except KeyError:
        raise BadConfiguration("Unable to find fsl_sub config")


def read_config():
    try:
        with open(find_config_file(), 'r') as yaml_source:
            config_dict = yaml.load(yaml_source)
    except Exception as e:
        raise BadConfiguration(
            "Unable to load configuration: " + str(e))
    return config_dict


def system_stdout(
        command, shell=False, cwd=None, timeout=None, check=True):
    result = subprocess.run(
            command,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            shell=shell, cwd=cwd, timeout=timeout,
            check=check, universal_newlines=True)

    return result.stdout


def system(
        command, shell=False, cwd=None, timeout=None, check=True):
    subprocess.run(
            command,
            stderr=subprocess.PIPE,
            shell=shell, cwd=cwd, timeout=timeout,
            check=check, universal_newlines=True)




def getq_and_slots(
        queues, job_time=None, job_ram=None,
        job_threads=1, coprocessor=None,
        ll_env=None):
    '''Calculate which queue to run the job on
    Still needs job splitting across slots'''
    logger = logging.getLogger('__name__')

    if job_time is None:
        queue_list = [
            q for q in queues if 'default' in q and q['default']]
    else:
        queue_list = copy.deepcopy(queues)
    # Filter on coprocessor availability
    if coprocessor:
        queue_list = [
            q for q in queue_list if 'copros' in q and
            coprocessor in q['copros']]
    # Filter on parallel environment availability
    if ll_env:
        queue_list = [
            q for q in queue_list if 'parallel_envs' in q and
            ll_env in q['parallel_envs']
        ]

    # For each queue calculate how many slots would be necessary...
    def calc_slots(job_ram, slot_size, job_threads):
        # No ram specified
        if job_ram is None:
            return max(1, job_threads)
        else:
            return max(int(ceil(job_ram / slot_size)), job_threads)

    for queue in queue_list:
        queue_list[queue]['slots_required'] = calc_slots(
            job_ram, queue['slot_size'], job_threads)

    sql = sorted(
        queue_list,
        key=itemgetter('group', 'priority', 'slots_required'))

    ql = [
        q['name'] for q in sql if q['time'] >= job_time and
        q['memory'] >= job_ram and
        q['max_slots'] <= job_threads]

    logger.info(
        "Estimated RAM was {0} GBm, runtime was {1} minutes.\n".format(
            job_ram, job_time
        ))
    if coprocessor:
        logger.info("Co-processor {} was requested".format(coprocessor))
    logger.info(
        "Appropriate queue is {}".format(ql[0]))
    return (ql[0], ql[0]['slots_required'])


if __name__ == "__main__":
    main()
