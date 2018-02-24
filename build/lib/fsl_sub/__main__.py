import argparse
import getpass
import logging
import os
import socket
import subprocess
import sys
from fsl_sub.exceptions import (
    BadConfiguration,
    ArgumentError,
    BadSubmission,
    NoModule
)
from fsl_sub.coprocessors import (
    list_coprocessors,
    coproc_classes,
    coproc_toolkits
)
from fsl_sub import (
    parallel_envs,
    submit,
    VERSION
)
from fsl_sub.config import read_config
from fsl_sub.utils import minutes_to_human
from fsl_sub.shell_modules import (
    find_module_cmd,
    get_modules
)
from fsl_sub.system import system_stdout


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
    if options['paralleltask'] is None:
        array_task = False

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
            parallel_hold=options['parallel_hold'],
            parallel_limit=options['parallel_limit'],
            parallel_stride=options['parallel_stride'],
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


if __name__ == "__main__":
    main()
