#!/usr/bin/env fslpython
import argparse
from collections import defaultdict
import getpass
import importlib
from operator import itemgetter
import os
import pkgutil
import platform
import re
import shlex
import shutil
import subprocess
import sys
import yaml

config_yaml = '''
# Job submission method to use. Supported values are 'SGE' and 'NONE'
method: SGE

# Job submission options
submission_cmd: qsub

# List all parallel environments configured on your cluster here
parallel_envs:
    - shmem
# List all shared memory (must run on same node) PEs here
same_node_pes:
    - shmem

method_opts:
    SGE:
        # Replicate user's shell environment to running job
        copy_environment: True
        # Method used to bind to CPUs
        affinity_type: linear
        # How to configure this affinity options are:
        #   threads - set to number of threads required
        #   slots - let GE sort it out automatically (not Univa Grid Engine)
        affinity_control: threads
        # When to email user:
        #   a - on abort
        mail_mode: a
        # Whether to split large memory jobs into shared memory PE slots
        map_ram: True
        # Queue complexes that specify RAM usage of a job
        ram_resources:
            - m_mem_free
            - h_vmem
        # Units for RAM given....?????
        ram_units: G
# The following defines configuration options for co-processor queues
# Define queues with a copro key set to the name of the appropriate option
# set and ensure that your queue method has a way of interpreting this
copro_opts:
    cuda:
        # Whether to split large memory jobs into shared memory PE slots
        map_ram: True
        # Which scheduler resource requests GPU facilities
        resource: gpu
        # Whether there are multiple coprocessor classes/types
        classes: True
        # Which scheduler resource requests a coprocessor class
        class_resource: gputype
        # This defines the short code for the types and the resource
        # which will be requested and a documentation string for the help
        # text
        class_types:
            G
                resource: TitanX
                doc: TitanX. No-ECC, single-precision workloads
                capability: 1
            K
                resource: k80
                doc: Kepler. ECC, double- or single-precision workloads
                capability: 2
            P
                resource: v100
                doc: Pascal. ECC, double-, single- and half-precision workloads
                capability: 3
            V
                resource: v100
                doc: Volta. ECC, double-, single-, half- and quarter-precision workloads"
                capability: 4
        default_class: K
        # Should we also allow running on more capable hardware?
        include_more_capable: True
        # Should we use Shell modules to load the environment settings for
        # the hardware?
        uses_modules: True
        # What is the name of the parent module for this co-processor?
        module_parent: cuda

queues:
    gpu.q:
        time: 18000
        max_gb: 250
        gb_per_slot: 64
        max_slots: 20
        copros:
            cuda:
                max_quantity: 4
                classes:
                    - K
                    - P
                    - V
        map_ram: true
        parallel_envs:
            - shmem
        priority: 1
        group: 0
        default: true
    short.qf,short.qe,short.qc:
        time: 1440
        max_gb: 160
        gb_per_slot: 4
        max_slots: 16
        map_ram: true
        parallel_envs:
            - shmem
        priority: 3
        group: 1
        default: true
    short.qe,short.qc:
        time: 1440
        max_gb: 240
        gb_per_slot: 16
        max_slots: 16
        map_ram: true
        parallel_envs:
            - shmem
        priority: 2
        group: 1
        default: true
    short.qc:
        time: 1440
        max_gb: 368
        gb_per_slot: 16
        max_slots: 24
        map_ram: true
        parallel_envs:
            - shmem
        priority: 1
        group: 1
        default: true
    long.qf,long.qe,long.qc:
        time: 10080
        max_gb: 160
        gb_per_slot: 4
        max_slots: 16
        map_ram: true
        parallel_envs:
            - shmem
        priority: 3
        group: 2
    long.qe,long.qc:
        time: 10080
        max_gb: 240
        gb_per_slot: 16
        max_slots: 16
        map_ram: true
        parallel_envs:
            - shmem
        priority: 2
        group: 2
    long.qc:
        time: 10080
        max_gb: 368
        gb_per_slot: 16
        max_slots: 24
        map_ram: true
        parallel_envs:
            - shmem
        priority: 1
        group: 2
default_queues:
    - short.qf,short,qe,short.qc
    - short.qe,short.qc
    - short.qc
'''

# ============================
# Configuration ends here
# ============================

VERSION = '2.0'

if __name__ == "__main__":
    main()


class ArgumentError(Exception):
    pass


@memoize
def list_coprocessors(config):
    '''Return a list of coprocessors found in the queue definitions'''

    avail_cops = []

    for q in config['queues']:
        try:
            avail_cops.extend(q['copros'].keys())
        except KeyError:
            pass

    return avail_cops


@memoize
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


@memoize
def copro_classes(config, coprocessor):
    '''Return whether a coprocessor supports multiple classes of hardware'''
    classes = defaultdict(lambda: 1)
    for q in config['queues']:
        if 'copros' in q:
            try:
                for c in q['copros'][coprocessor]['classes']:
                    classes[c] += 1
            except KeyError:
                continue
    return clases.keys()


def parse_arguments(config):
    available_coprocessors = list_coprocessors(config)
    max_coprocessors = {c: max_coprocessors(c) for c in available_coprocessors}

    # Build the epilog...
    epilog = []
    if config['method'] != 'None':
        epilog += '''
Queues

There are several batch queues configured on the cluster:
        '''
        for qname, q in config.queues.items():
            epilog += (
                "{qname}: {timelimit} max run time; {q[gb_per_slot]}GB "
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
    parser.add_argument(
        '-a', '--arch',
        action='append',
        default=None,
        help="Architecture [e.g., lx-amd64].")
    parser.add_argument(
        '-c', '--coprocessor',
        action='append',
        help="Request a co-processor, expects a comma separated list "
        "of coprocessor name, hardware type, software version, number/"
        "layout of coprocessors required (see your cluster documentation)."
        " e.g. cuda,K,9.0,2. Any one of the last three arguments may be "
        "omitted.")
    parser.add_argument(
        '--coprocessor_varient',
        default=None,
    )
    parser.add_argument(
        '-F', '--usescript',
        action='store_false',
        help="Use flags embedded in scripts to set queuing options."
    )
    parser.add_argument(
        '-j', '--jobhold',
        default=None,
        help="Place a hold on this task until specified job id has "
        "completed."
    )
    parser.add_argument(
        '--parallelhold',
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
    parser.add_argument(
        '-m', '--mailoptions',
        default=None,
        help="Specify job mail options, see your queuing software for details."
    )
    parser.add_argument(
        '-M', '--mailto',
        default="{username}@{hostname}.".format(
                    username=getpass.getuser(),
                    hostname=socket.gethostname()
                ),
        help="Who to email."
    )
    parser.add_argument(
        '-n', '--novalidation',
        action='store_false',
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
    parser.add_argument(
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
    parser.add_arguemt(
        '-r', '--resource',
        default=None,
        help="Pass a resource request string through to the job scheduler. "
        "See your scheduler's instructions for details"
    )
    parser.add_argument(
        '-R', '--jobram',
        default=None,
        help="Max total RAM to use for job (integer in GB). "
        "This is very important to set if your job requires more "
        "than the queue slot memory limit as then you job can be "
        "split over multiple slots automatically - see autoslotsbyram."
    )
    parser.add_argument(
        '-s', '--parallelenv',
        default=None,
        help="Takes a comma-separated argument <pename>,<threads>."
        "Submit a multi-threaded (or resource) task - requires a "
        "parallel environment (<pename>) to be configured on the "
        "requested queues. <threads> specifies the number of "
        "threads/hosts required. e.g. '{pe_name},2'.".format(
            pe_name=config['parallel_envs'][0])
    )
    parser.add_argument(
        '-S', '--autoslotsbyram',
        action='store_true',
        help="Use the memory requirement specified by -R|--jobram to "
        "request a parallel environment with sufficient slots to allow "
        "your job to run."
    )
    parser.add_argument(
        '-t', '--paralleltask',
        default=None,
        help="Specify a task file of commands to execute in parallel."
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
    parser.add_argument(
        '-x', '--parallellimit',
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
    parser.add_argument('args', nargs=argparse.REMAINDER)


def minutes_to_human(minutes):
    if minutes < 60:
        return "{}m".format(minutes)
    if minutes < 60 * 24:
        return "{:.1f}h".format(minutes/60)
    return "{:.1f}d".format(minutes/(60 * 24))

class PluginError(Exception):
    pass


def main():
    logger = logging.getLogger('__name__')
    config = read_config(config_yaml)

    fsl_sub_plugins = {
        name: importlib.import_module(name)
        for finder, name, ispkg
        in pkgutil.iter_modules()
        if name.startswith('fsl_sub_')
    }

    if config['method'] != 'None':
        if affirmative(os.environ['FSLSUBALREADYRUN']):
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
    except AttributeError as e:
        raise BadConfiguration(
            "Failed to load plugin " + grid_module
        )

    if config['method'] == 'SGE':
        config['qtest'] = find_conf()
        if config['qtest'] is None:
            config['method'] == 'None'
            print(
                'Warning: fsl_sub configured for Grid Engine but Grid'
                ' Engine software not found.',
                file=sys.stderr
            )

    

    options, commands = parse_arguments(sys.argv)
    calc_opts = {}

    if not commands and not options.taskfile:
        usage()

    if options.taskfile and commands:
        raise ArgumentError("Can't submit both parallel and serial tasks")

    if options.taskfile:
        check_command_file(options.taskfile)
        calc_opts['parallel_task'] = True
        task_name = os.path.basename(options.taskfile)

    if commands:
        check_command(commands[0])
        calc_opts['parallel_task'] = False
        task_name = os.path.basename(commands[0])
        if options.parallel_hold:
            raise ArgumentError(
                "Cannot apply a parallel job hold on a non-array task")

    if options.jobname:
        calc_opts['job_name'] = options.jobname
    else:
        calc_opts['job_name'] = task_name

    if options.pe:
        calc_opts['pes'] = []
        for pe in options.pe:
            try:
                (pe_name, pe_slots) = pe.split(',')
                calc_opts['pes'].append((pe_name, pe_slots))
            except ValueError:
                raise ArgumentError(
                    "Parallel environment must be name, slots"
                )

    logger.info(
        "METHOD={0} : args={1}".format(
            config['method'],
            " ".join(commands)
        ))

    if options.coprocs:
        # options.coprocs should be a list of tuples
        #  (coprocessor name, module version, coprocessor class)
        # or sub-set thereof
        coprocessors = parse_coprocessors(
            options.coprocs,
            config['copro_opts'])

    queue = getq(
            job_time=options.jobtime,
            job_ram=options.jobram,
            job_threads=options.threads,
            queues=config['queues'],
            coprocessors=coprocessors)

    if not queue:
        failed("Unable to find a queue with these parameters")
    if not queue_exists(queue, config['qtest']):
        failed("Invalid queue name specified!")

    job_ram = split_ram_by_slots(job_ram, queue['slots_required'])

    submit_job(config)


def check_command(cmd):
    if shutil.which(cmd) is None:
        raise ArgumentError("Cannot find script/binary " + cmd)


def check_command_file(cmd_file):
    with open(cmd_file, 'r') as cmds:
        for line, lineno in enumerate(cmds.readlines()):
            try:
                check_command(line[0])
            except ArgumentError(
                "Cannot find script/binary {0} on line {1}"
                "of {2}".format(line[0], lineno, cmd_file))


class BadCoprocessor(Exception):
    pass


def parse_coprocessors(copro_options, coprocessors):
    '''Takes a list of co-processor configuration options of form:
    coprocessor name,coprocessor version,coprocessor class
    returns a list of tuples (name, version, class) or raises BadCoprocessor'''
    cop_defs = []
    for cop in copro_options:
        cop_name = ''
        cop_version = ''
        cop_class = ''
        options = cop.count(',')
        if options > 2:
            raise BadCoprocessor(
                "Too many options passed for co-processor: " + cop)
        if options == 2:
            (cop_name, cop_version, cop_class) = cop.split(',')
        elif options == 1:
            (cop_name, other) = cop.split(',')
        else:
            cop_name = cop

        if cop_name not in coprocessors:
            raise BadCoprocessor(
                "{} not recognised as configured co-processor".format(
                    cop_name))

        copro_def = coprocessors[cop_name]
        if other:
            if other in copro_def['class_types']:
                cop_class = other
                cop_version = latest_module(
                    copro_def['module_parent'])
            else:
                cop_version = other
                if cop_version not in get_modules(copro_def['module_parent']):
                    raise BadCoprocessor(
                        "{0} not recognised as an "
                        "available version for {1}".format(
                            cop_version, cop_name
                        )
                    )
                cop_class = coprocessors[cop_name]['default_class']
        else:
            cop_version = latest_module(
                coprocessors[cop_name]['module_parent'])
            cop_class = coprocessors[cop_name]['default_class']

        cop_defs.append((cop_name, cop_version, cop_class, ))


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


def read_config(yaml_source):
    return yaml.load(yaml_source)


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
    result = subprocess.run(
            command,
            stderr=subprocess.PIPE,
            shell=shell, cwd=cwd, timeout=timeout,
            check=check, universal_newlines=True)


class LoadModuleError(Exception):
    pass


class NoModule(Exception):
    pass


def memoize(f, cache={}):
    def g(*args, **kwargs):
        key = (f, tuple(args), frozenset(kwargs.items()))
        if key not in cache:
            cache[key] = f(*args, **kwargs)
        return cache[key]
    return g


@memoize
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
        for k, v in enviroment.items():
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
        return ()
    return modules_string.split(':')


@memoize
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
    return modules


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


class BadConfiguration(Exception):
    pass


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


class UnrecognisedModule(Exception):
    pass


def coproc_load_module(coproc, module_version):
    if coproc['uses_modules']:
        modules_avail = get_modules(coproc['module_parent'])
        if modules_avail:
            if module_version not in modules_avail:
                raise UnrecognisedModule(module_version)
            else:
                load_module("/".join(
                    (coproc['module_parent'], module_version)))


def find_qconf():
    return shutil.which('qconf')


def getq(queues, job_time=None, job_ram=None, job_threads=1, coprocessors=None):
    '''Calculate which queue to run the job on
    Still needs job splitting across slots'''
    logger = logging.getLogger('__name__')

    if job_time is None:
        queue_list = [
            q for q in queue_list if 'default' in q and q['default']]
    else:
        queue_list = copy.deepcopy(queues)

    if coprocessors:
        queue_list = [
            q for q in queue_list if 'copros' in q and
            coprocessor in q['copros']]

    # For each queue calculate how many slots would be necessary...
    def calc_slots(job_ram, slot_size, job_threads):
        if job_ram is None:
            return max(1, job_threads)
        else:
            return max(int(ceil(job_ram / slot_size)), job_threads)

    queue_list = [dict(
            item, slots_required=calc_slots(job_ram, slot_size)
            ) for item in queue_list]

    sql = sorted(
        queue_list,
        key=itemgetter('group', 'priority', 'slots_required'))

    ql = [
        q['name'] for q in sql if q['time'] >= t and
        q['memory'] >= m and
        q['max_slots'] <= job_threads]

    logger.info(
        "Estimated RAM was {0} GBm, runtime was {1} minutes.\n".format(
            job_ram, job_time
        ))
    if coprocessor:
        logger.info("Co-processor {} was requested".format(coprocessor)
    logger.info(
        "Appropriate queue is {}".format(ql[0]))
    return ql[0]
