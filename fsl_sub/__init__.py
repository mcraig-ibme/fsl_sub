#!/usr/bin/env fslpython
import argparse
import copy
import getpass
import logging
import os
import socket
import shlex
import subprocess
import sys
import warnings
from operator import itemgetter
from math import ceil
from fsl_sub.exceptions import (
    BadConfiguration,
    ArgumentError,
    BadSubmission,
    NoModule,
    LoadModuleError,
)
from fsl_sub.coprocessors import (
    list_coprocessors,
    coproc_classes,
    coproc_toolkits,
    coproc_load_module
)
from fsl_sub.config import (
    read_config,
    method_config,
    coprocessor_config
)
from fsl_sub.shell_modules import (
    find_module_cmd,
    get_modules
)
from fsl_sub.system import system_stdout

from fsl_sub.utils import (
    minutes_to_human,
    load_plugins,
    affirmative,
    check_command,
    check_command_file,
    control_threads
)

VERSION = '2.0'
PLUGINS = load_plugins()


def fsl_sub_warnings_formatter(
        message, category, filename, lineno, file=None, line=None):
    return str(message)


warnings.formatwarning = fsl_sub_warnings_formatter
warnings.simplefilter('always', UserWarning)


def parallel_envs(queues):
    '''Return the list of configured parallel environments
    in the supplied queue definition dict'''
    ll_envs = []
    for q in queues.values():
        try:
            ll_envs.extend(q['parallel_envs'])
        except KeyError:
            pass
    return list(set(ll_envs))


def submit(
    command,
    name=None,
    threads=1,
    queue=None,
    jobhold=None,
    array_task=False,
    array_hold=None,
    array_limit=None,
    array_stride=1,
    parallel_env=None,
    jobram=None,
    jobtime=None,
    resources=None,
    ramsplit=True,
    priority=None,
    validate_command=True,
    mail_on=None,
    mailto="{username}@{hostname}.".format(
                            username=getpass.getuser(),
                            hostname=socket.gethostname()),
    logdir=os.getcwd(),
    coprocessor=None,
    coprocessor_toolkit=None,
    coprocessor_class=None,
    coprocessor_class_strict=False,
    coprocessor_multi="1",
    usesscript=False,
    architecture=None,
):
    '''Submit job(s) to a queue'''
    logger = logging.getLogger('__name__')
    global PLUGINS

    config = read_config()

    try:
        already_run = os.environ['FSLSUBALREADYRUN']
    except KeyError:
        already_run = 'false'
    os.environ['FSLSUBALREADYRUN'] = 'true'

    if config['method'] != 'None':
        if affirmative(already_run):
            config['method'] == 'None'
            warnings.warn(
                'Warning: job on queue attempted to submit parallel jobs -'
                'running jobs serially instead.'
            )

    grid_module = 'fsl_sub_' + config['method']
    if grid_module not in PLUGINS:
        raise BadConfiguration(
            "{} not a supported method".format(config['method']))

    try:
        queue_submit = PLUGINS[grid_module].submit
        qfind = PLUGINS[grid_module].qfind
        queue_exists = PLUGINS[grid_module].queue_exists
        BadSubmission = PLUGINS[grid_module].BadSubmission
    except AttributeError as e:
        raise BadConfiguration(
            "Failed to load plugin " + grid_module
        )

    config['qtest'] = qfind()
    if config['qtest'] is None:
        config['method'] == 'None'
        warnings.warn(
            'Warning: fsl_sub configured for {} but {}'
            ' software not found.'.format(config['method'])
        )

    if method_config['mail_support'] is True:
        if mail_on is None:
            try:
                mail_on = method_config['mail_mode']
            except KeyError:
                warnings.warn(
                    "Mail not configured but enabled in configuration for " +
                    config['method'])
        else:
            for m_opt in method_config['mail_modes'].split(','):
                if m_opt not in method_config['mail_modes']:
                    raise BadSubmission(
                        "Unrecognised mail mode " + mail_on)

    if array_task is False:
        if isinstance(command, list):
            # command is the command line to run as a list
            job_type = 'single'
        elif isinstance(command, str):
            # command is a basic string
            command = shlex.split(command)
            job_type = 'single'
        else:
            raise BadSubmission("Command should be a list or string")
        if validate_command:
            check_command(command[0])
    else:
        job_type = 'array'
        if (
                array_hold is not None or
                array_limit is not None or
                array_stride is not None):
            raise BadSubmission(
                "Array controls not applicable to non-array tasks")
        if validate_command:
            check_command_file(command)
        if name is None:
            name = os.path.basename(command)
    logger.info(
        "METHOD={0} : TYPE={1} : args={2}".format(
            config['method'],
            job_type,
            " ".join(command)
        ))
    task_name = os.path.basename(command)

    m_config = method_config(config['method'])

    split_on_ram = m_config['map_ram'] and ramsplit

    if (split_on_ram and
            parallel_env is None and
            'large_job_split_pe' in m_config):
        parallel_env = m_config['large_job_split_pe']

    if queue is None:
        (queue, slots_required) = getq_and_slots(
                job_time=jobtime,
                job_ram=jobram,
                job_threads=threads,
                queues=config['queues'],
                coprocessor=coprocessor,
                ll_env=parallel_env
                )

    if queue is None:
        raise BadSubmission("Unable to find a queue with these parameters")

    if not queue_exists(queue):
        raise BadSubmission("Unrecognised queue " + queue)

    threads = max(slots_required, threads)

    control_threads(config['thread_control'], threads)

    if threads == 1 and parallel_env is not None:
        parallel_env = None
    if threads > 1 and parallel_env is None:
        raise BadSubmission(
                "Job requires {} slots but no parallel envrionment "
                "available or requested".format(threads))

    if coprocessor:
        try:
            coproc_load_module(
                coprocessor_config(coprocessor),
                coprocessor_toolkit)
        except LoadModuleError:
            raise BadSubmission(
                "Unable to load requested coprocessor toolkit"
            )
    job_id = queue_submit(
        command,
        job_name=task_name,
        threads=threads,
        queue=queue,
        jobhold=jobhold,
        array_task=array_task,
        array_hold=array_hold,
        array_limit=array_limit,
        array_stride=array_stride,
        parallel_env=parallel_env,
        jobram=jobram,
        jobtime=jobtime,
        resources=resources,
        ramsplit=split_on_ram,
        prority=priority,
        mail_on=mail_on,
        mailto=mailto,
        logdir=logdir,
        coprocessor=coprocessor,
        coprocessor_toolkit=coprocessor_toolkit,
        coprocessor_class=coprocessor_class,
        coprocessor_class_strict=coprocessor_class_strict,
        coprocessor_multi=coprocessor_multi,
        usesscript=usesscript,
        architecture=architecture)

    return job_id


def split_ram_by_slots(jram, jslots):
    return int(ceil(jram / jslots))


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


def build_parser(config):
    '''Parse the command line, returns a dict keyed on option'''

    available_coprocessors = list_coprocessors(config)
    coprocessor_classes = {
        c: coproc_classes(config, c) for c in available_coprocessors}
    coprocessor_toolkit = coproc_toolkits(config)
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
            cp_classes = coproc_classes(config, cp)
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
        '--array_hold',
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
        '-t', '--array_task',
        help="Specify a task file of commands to execute in parallel."
    )
    array_g.add_argument(
        '--array_stride',
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
        '-x', '--array_limit',
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


def main():
    logger = logging.getLogger('__name__')

    config = read_config()

    cmd_parser = build_parser(config)
    options = vars(cmd_parser.parse_args())

    if options['verbose']:
        logger.set_level(logging.INFO)

    if options['file-is-image']:
        logger.debug("Check file is image requested")
        if os.path.isfile(options['file-is-image']):
            try:
                if system_stdout(
                    command=[
                        os.path.join(
                            os.environ['FSLDIR'],
                            'bin',
                            'imtest'),
                        options['file-is-image']
                        ]) == '1':
                    logger.info("File is an image")
                    sys.exit(0)
            except subprocess.CalledProcessError as e:
                cmd_parser.error(
                    "Error trying to check image file - " +
                    str(e))

    if options['parallelenv']:
        try:
            pe_name, threads = process_pe_def(
                options['parallelenv'], config['queues'])
        except ArgumentError as e:
            cmd_parser.error(str(e))

    array_task = True
    if options['array_task'] is None:
        array_task = False
        if (
                options['array_hold'] is not None or
                options['array_limit'] is not None or
                options['array_stride'] is not None):
            cmd_parser.error(
                "Array controls not applicable to non-array tasks")
    if 'mailoptions' not in options:
        options['mailoptions'] = None
    if 'mailto' not in options:
        options['mailto'] = None

    try:
        job_id = submit(
            name=options['name'],
            queue=options['queue'],
            parallel_env=pe_name,
            threads=threads,
            jobhold=options['jobhold'],
            jobram=options['jobram'],
            jobtime=options['jobtime'],
            logdir=options['logdir'],
            mail_on=options['mailoptions'],
            mailto=options['mailto'],
            array_hold=options['array_hold'],
            array_limit=options['array_limit'],
            array_stride=options['array_stride'],
            array_task=array_task,

        )
    except BadSubmission as e:
        cmd_parser.error("Error submitting job:" + str(e))
    except Exception as e:
        cmd_parser.error("Unexpected error: " + str(e))
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
    try:
        slots = int(pe[1])
    except TypeError:
        raise ArgumentError(
            "Slots requested not an integer"
        )
    return (pe[0], slots, )
