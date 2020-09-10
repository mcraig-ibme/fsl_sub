#!/usr/bin/python

# fsl_sub python module
# Copyright (c) 2018, University of Oxford (Duncan Mortimer)

import argparse
import getpass
import logging
import os
import socket
import sys
import traceback
from fsl_sub import (
    submit,
    report,
)
from fsl_sub.config import (
    read_config,
    method_config,
    coprocessor_config,
    has_queues,
    uses_projects,
)
from fsl_sub.config import example_config as e_conf

import fsl_sub.consts
from fsl_sub.coprocessors import (
    coproc_info,
    coproc_classes,
)
from fsl_sub.exceptions import (
    ArgumentError,
    CommandError,
    BadConfiguration,
    BadSubmission,
    GridOutputError,
    InstallError,
    NoFsl,
    NoModule,
    PackageError,
    UpdateError,
    CONFIG_ERROR,
    SUBMISSION_ERROR,
    RUNNER_ERROR,
)
from fsl_sub.shell_modules import (
    get_modules,
    find_module_cmd,
)
from fsl_sub.parallel import (
    parallel_envs,
    process_pe_def,
)
from fsl_sub.projects import (
    get_project_env,
)
from fsl_sub.utils import (
    available_plugins,
    available_plugin_packages,
    blank_none,
    conda_check_update,
    conda_find_packages,
    conda_install,
    conda_update,
    file_is_image,
    find_fsldir,
    load_plugins,
    minutes_to_human,
    titlize_key,
    user_input,
)
from fsl_sub.version import VERSION


class MyArgParseFormatter(
        argparse.ArgumentDefaultsHelpFormatter,
        argparse.RawDescriptionHelpFormatter):
    pass


def build_parser(
        config=None, cp_info=None,
        plugin_name=None, plugin_version=None):
    '''Parse the command line, returns a dict keyed on option'''
    logger = logging.getLogger(__name__)
    if config is None:
        config = read_config()
    if cp_info is None:
        cp_info = coproc_info()
    if has_queues():
        ll_envs = parallel_envs(config['queues'])
    else:
        ll_envs = None

    # Build the epilog...
    epilog = ''
    mconf = method_config(config['method'])
    if mconf['queues']:
        epilog += '''
Queues:

There are several batch queues configured on the cluster:
'''
        q_defs = []
        for qname, q in config['queues'].items():
            q_defs.append((qname, q))
        q_defs.sort(key=lambda x: x[0])
        for qname, q in q_defs:
            pad = " " * 10
            epilog += (
                "{qname}:\n{q_pad}{timelimit} max run time; {q[slot_size]}GB "
                "per slot; {q[max_size]}GB total\n".format(
                    qname=qname,
                    q_pad=pad,
                    timelimit=minutes_to_human(q['time']),
                    q=q,
                ))
            if 'copros' in q:
                epilog += (
                    pad + "Coprocessors available: "
                    + "; ".join(q['copros']) + '\n'
                )
            if 'parallel_envs' in q:
                epilog += (
                    pad + "Parallel environments available: "
                    + "; ".join(q['parallel_envs']) + '\n'
                )
            if 'map_ram' in q and q['map_ram']:
                epilog += (
                    pad + "Supports splitting into multiple slots." + '\n'
                )
            epilog += '\n'
    if cp_info['available']:
        epilog += "Co-processors available:"
        for cp in cp_info['available']:
            epilog += '\n' + cp + '\n'
            try:
                cp_def = coprocessor_config(cp)
            except BadConfiguration:
                continue
            if find_module_cmd():
                if cp_def['uses_modules']:
                    epilog += "    Available toolkits:" + '\n'
                    try:
                        module_list = get_modules(cp_def['module_parent'])
                    except NoModule as e:
                        raise BadConfiguration from e
                    epilog += "      " + ', '.join(module_list) + '\n'
            cp_classes = coproc_classes(cp)
            if cp_classes:
                epilog += (
                    "    Co-processor classes available: " + '\n'
                )
                for cpclass in cp_classes:
                    epilog += (
                        "      " + ": ".join(
                            (cpclass, cp_def['class_types'][cpclass]['doc']))
                        + '\n'
                    )
    logger.debug(epilog)
    parser = argparse.ArgumentParser(
        prog="fsl_sub",
        formatter_class=MyArgParseFormatter,
        description='FSL cluster submission.',
        epilog=epilog,
    )
    single_g = parser.add_argument_group(
        'Simple Tasks',
        'Options for submitting individual tasks.'
    )
    array_g = parser.add_argument_group(
        'Array Tasks',
        'Options for submitting and controlling array tasks.'
    )
    basic_g = parser.add_argument_group(
        'Basic options',
        'Options that specify individual and array tasks.'
    )
    advanced_g = parser.add_argument_group(
        'Advanced',
        'Advanced queueing options not typically required.')
    email_g = parser.add_argument_group(
        'Emailing',
        'Email notification options.')
    copro_g = parser.add_argument_group(
        'Co-processors',
        'Options for requesting co-processors, e.g. GPUs')
    if 'architecture' in mconf and mconf['architecture']:
        advanced_g.add_argument(
            '-a', '--arch',
            action='append',
            default=None,
            help="Architecture [e.g., lx-amd64].")
    else:
        advanced_g.add_argument(
            '-a', '--arch',
            action='append',
            default=None,
            help="Architectures not available.")
    if cp_info['available']:
        copro_g.add_argument(
            '-c', '--coprocessor',
            default=None,
            choices=cp_info['available'],
            help="Request a co-processor, further details below.")
        copro_g.add_argument(
            '--coprocessor_multi',
            default=1,
            help="Request multiple co-processors for a job. This may take "
            "the form of simple number or a complex definition of devices. "
            "See your cluster documentation for details."
        )
    else:
        copro_g.add_argument(
            '-c', '--coprocessor',
            default=None,
            help="No co-processor configured - ignored.")
        copro_g.add_argument(
            '--coprocessor_multi',
            default=1,
            help="No co-processor configured - ignored"
        )
    if cp_info['classes']:
        copro_g.add_argument(
            '--coprocessor_class',
            default=None,
            choices=cp_info['classes'],
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
    else:
        copro_g.add_argument(
            '--coprocessor_class',
            default=None,
            help="No co-processor classes configured - ignored."
        )
        copro_g.add_argument(
            '--coprocessor_class_strict',
            action='store_true',
            help="No co-processor classes configured - ignored."
        )
    if cp_info['toolkits']:
        copro_g.add_argument(
            '--coprocessor_toolkit',
            default=None,
            choices=cp_info['toolkits'],
            help="Request a specific version of the co-processor software "
            "tools. Will default to the latest version available. "
            "If you wish to use the toolkit defined in your current "
            " environment, give the value '-1' to this argument."
        )
    else:
        copro_g.add_argument(
            '--coprocessor_toolkit',
            default=None,
            help="No co-processor toolkits configured - ignored."
        )
    advanced_g.add_argument(
        '--debug',
        action='store_true',
        help=argparse.SUPPRESS
    )
    if 'script_conf' in mconf and mconf['script_conf']:
        advanced_g.add_argument(
            '-F', '--usescript',
            action='store_true',
            help="Use flags embedded in scripts to set queuing options - "
            "all other options ignored."
        )
    else:
        advanced_g.add_argument(
            '-F', '--usescript',
            action='store_true',
            help="Use flags embedded in scripts to set queuing options - "
            "not supported"
        )
    basic_g.add_argument(
        '-j', '--jobhold',
        default=None,
        help="Place a hold on this task until specified job id has "
        "completed."
    )
    basic_g.add_argument(
        '--not_requeueable',
        action='store_true',
        help="Job cannot be requeued in the event of a node failure"
    )
    if 'array_holds' in mconf and mconf['array_holds']:
        array_g.add_argument(
            '--array_hold',
            default=None,
            help="Place a parallel hold on the specified array task. Each"
            "sub-task is held until the equivalent sub-task in the"
            "parent array task completes."
        )
    else:
        array_g.add_argument(
            '--array_hold',
            default=None,
            help="Not supported - will be converted to simple job hold"
        )
    basic_g.add_argument(
        '-l', '--logdir',
        default=None,
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
            default="{username}@{hostname}".format(
                username=getpass.getuser(),
                hostname=socket.gethostname()
            ),
            help="Who to email."
        )
    else:
        email_g.add_argument(
            '-m', '--mailoptions',
            default=None,
            help="Not supported - will be ignored"
        )
        email_g.add_argument(
            '-M', '--mailto',
            default="{username}@{hostname}".format(
                username=getpass.getuser(),
                hostname=socket.gethostname()
            ),
            help="Not supported - will be ignored"
        )
    basic_g.add_argument(
        '-n', '--novalidation',
        action='store_true',
        help="Don't check for presence of script/binary in your search"
        "path (use where the software is only available on the "
        "compute node)."
    )
    basic_g.add_argument(
        '-N', '--name',
        default=None,
        help="Specify jobname as it will appear on queue. If not specified "
        "then the job name will be the name of the script/binary submitted."
    )
    if 'job_priorities' in mconf and mconf['job_priorities']:
        min = mconf['min_priority']
        max = mconf['max_priority']
        if min > max:
            min = max
            max = mconf['min_priority']
        advanced_g.add_argument(
            '-p', '--priority',
            default=None,
            type=int,
            metavar="-".join((
                str(min),
                str(max)
            )),
            choices=range(min, max),
            help="Specify a lower job priority (where supported)."
            "Takes a negative integer."
        )
    else:
        advanced_g.add_argument(
            '-p', '--priority',
            default=None,
            type=int,
            help="Not supported on this platform."
        )
    if has_queues():
        basic_g.add_argument(
            '-q', '--queue',
            default=None,
            help="Select a particular queue - see below for details. "
            "Instead of choosing a queue try to specify the time required."
        )
    else:
        basic_g.add_argument(
            '-q', '--queue',
            default=None,
            help="Not relevant when not running in a cluster environment"
        )
    advanced_g.add_argument(
        '-r', '--resource',
        default=None,
        action='append',
        help="Pass a resource request or constraint string through to the job "
        "scheduler. See your scheduler's instructions for details."
    )
    basic_g.add_argument(
        '-R', '--jobram',
        default=None,
        type=int,
        help="Max total RAM required for job (integer in "
        + config['ram_units'] + "B). "
        "This is very important if your job requires more "
        "than the queue slot memory limit as then your job can be "
        "split over multiple slots automatically - see autoslotsbyram."
    )
    if ll_envs is not None:
        advanced_g.add_argument(
            '-s', '--parallelenv',
            default=None,
            help="Takes a comma-separated argument <pename>,<threads>."
            "Submit a multi-threaded (or resource limited) task - requires a "
            "parallel environment (<pename>) to be configured on the "
            "requested queues. <threads> specifies the number of "
            "threads/hosts required. e.g. '{pe_name},2'.\n"
            "Some schedulers only support the threads part so specify "
            "'threads' as a <pename>.".format(
                pe_name=ll_envs[0])
        )
    else:
        advanced_g.add_argument(
            '-s', '--parallelenv',
            default=None,
            help="No parallel environments configured"
        )
    array_g.add_argument(
        '-t', '--array_task',
        default=None,
        help="Specify a task file of commands to execute in parallel."
    )
    array_g.add_argument(
        '--array_native',
        default=None,
        help="Binary/Script will handle array task internally. "
        "Mutually exclusive with --array_task. Requires "
        "an argument n[-m[:s]] which provides number of tasks (n) or "
        "start (n), end (m) and increment of sub-task ID between sub-"
        "tasks (s). Binary/script can use FSLSUB_JOBID_VAR, "
        "FSLSUB_ARRAYTASKID_VAR, FSLSUB_ARRAYSTARTID_VAR, "
        "FSLSUB_ARRAYENDID_VAR, FSLSUB_ARRAYSTEPSIZE_VAR, "
        "FSLSUB_ARRAYCOUNT_VAR environment variables to identify the "
        "environment variables that are set by the cluster manager to "
        "identify the sub-task that is running."
    )
    advanced_g.add_argument(
        '-x', '--array_limit',
        default=None,
        type=int,
        help="Specify the maximum number of parallel job sub-tasks to run "
        "concurrently."
    )
    if has_queues():
        advanced_g.add_argument(
            '--keep_jobscript',
            action="store_true",
            help="Whether to create and save a job submission script that records "
            "the submission and command arguments. This is not supported for the basic "
            "'shell' plugin so is ignored"
        )
        advanced_g.add_argument(
            '--export',
            action='append',
            default=[],
            help="Job will inherit this environment variable. Repeat this option "
            "for as many variables as you require or configure your ~/.fsl_sub.yml "
            "file:\nexport_envvars:\n  - MYENVVAR\n  - ANOTHERENVVAR\n"
            "If you need to change the value of an environment variable then this may "
            "be possible by providing:\n--export=VARNAME=NEWVALUE\nThis is cluster backend "
            "dependent so may not be supported."
        )
        if uses_projects():
            advanced_g.add_argument(
                '--project',
                default=None,
                help="Request job is run against the specified project/account"
            )
        else:
            advanced_g.add_argument(
                '--project',
                default=None,
                help="Projects not used"
            )
        advanced_g.add_argument(
            '--native_holds',
            action="store_true",
            help="If specified then the hold/parallel hold are taken to be "
            "cluster native hold specifiers and will not be interpreted by "
            "fsl_sub."
        )
        basic_g.add_argument(
            '-S', '--noramsplit',
            action='store_true',
            help="Disable the automatic requesting of multiple threads "
            "sufficient to allow your job to run within the RAM constraints."
        )
    else:
        advanced_g.add_argument(
            '--project',
            default=None,
            type=str,
            help="Not relevant when not running in a cluster environment"
        )
        advanced_g.add_argument(
            '--native_holds',
            action="store_true",
            help="Not relevant when not running in a cluster environment"
        )
        basic_g.add_argument(
            '-S', '--noramsplit',
            action='store_true',
            help="Not relevant when not running in a cluster environment"
        )
    basic_g.add_argument(
        '-T', '--jobtime',
        default=None,
        type=int,
        help="Estimated job length in minutes, used to automatically choose "
        "the queue name."
    )
    advanced_g.add_argument(
        '--show_config',
        action="store_true",
        help="Display the configuration currently in force"
    )
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help="Verbose mode."
    )
    version_string = '%(prog)s ' + VERSION
    if plugin_name is not None:
        version_string += " ({0} {1})".format(
            plugin_name, plugin_version
        )
    parser.add_argument(
        '-V', '--version',
        action='version',
        version=version_string)
    parser.add_argument(
        '-z', '--fileisimage',
        default=None,
        metavar='file',
        help="If <file> already exists and is an MRI image file, do nothing "
        "and exit."
    )
    single_g.add_argument(
        'args',
        nargs=argparse.REMAINDER,
        default=None,
        help="Program (and arguments) to submit to queue "
        "(not used with array tasks).")
    return parser


def example_config_parser(parser_class=argparse.ArgumentParser):
    '''Parse the command line, returns a dict keyed on option'''
    logger = logging.getLogger(__name__)
    plug_ins = available_plugins()
    if len(plug_ins) == 1:
        default = plug_ins[0]
    else:
        default = plug_ins[-1]
    logger.debug("plugins found:")
    logger.debug(plug_ins)

    parser = parser_class(
        prog="fsl_sub_config",
        description='FSL cluster submission configuration examples.',
    )
    parser.add_argument(
        'plugin',
        choices=plug_ins,
        nargs='?',
        default=default,
        help="Output an example fsl_sub configuration which may be "
        "customised for your system."
    )
    return parser


def report_parser(parser_class=argparse.ArgumentParser):
    '''Parse the command line, returns a dict keyed on option'''

    parser = parser_class(
        prog="fsl_sub_report",
        description='FSL cluster job reporting.',
    )
    parser.add_argument(
        'job_id',
        type=int,
        help="Report job details for this job ID."
    )
    parser.add_argument(
        '--subjob_id',
        type=int,
        help="Report job details for this subjob ID's only."
    )
    parser.add_argument(
        '--parseable',
        action="store_true",
        help="Include all output '|' separated"
    )
    return parser


class LogFormatter(logging.Formatter):

    default_fmt = logging.Formatter('%(levelname)s:%(name)s: %(message)s')
    info_fmt = logging.Formatter('%(message)s')

    def format(self, record):
        if record.levelno >= logging.INFO:
            return self.info_fmt.format(record)
        else:
            return self.default_fmt.format(record)


def example_config(args=None):
    lhdr = logging.StreamHandler()
    fmt = LogFormatter()
    lhdr.setFormatter(fmt)
    logger = logging.getLogger('fsl_sub')
    logger.addHandler(lhdr)
    example_parser = example_config_parser()
    options = example_parser.parse_args(args=args)
    print(e_conf(options.plugin))


def report_cmd(args=None):
    lhdr = logging.StreamHandler()
    fmt = LogFormatter()
    lhdr.setFormatter(fmt)
    logger = logging.getLogger('fsl_sub')
    plugin_logger = logging.getLogger('fsl_sub.plugins')
    logger.addHandler(lhdr)
    plugin_logger.addHandler(lhdr)
    cmd_parser = report_parser()
    options = cmd_parser.parse_args(args=args)
    try:
        job_details = report(options.job_id, options.subjob_id)
    except BadConfiguration as e:
        cmd_parser.error("Bad configuration: " + str(e))
    order = [
        'id', 'name',
        'script', 'arguments',
        'submission_time', 'parents',
        'children', 'job_directory', 'tasks',
    ]
    task_order = [
        'status', 'start_time',
        'end_time', 'sub_time',
        'utime', 'stime',
        'exit_status', 'error_messages',
        'maxmemory'
    ]
    if job_details is None:
        cmd_parser.error(
            "Unrecognised job id " + str(options.job_id))
    if not options.parseable:
        if 'fake' in job_details:
            print("No queuing software configured")
            return
        print("Job Details\n===========")
        for key in order:
            try:
                detail = job_details[key]
            except KeyError:
                continue
            if key != 'tasks':
                if detail is None:
                    continue
                print("{0}: ".format(titlize_key(key)), end='')
                if key == 'submission_time':
                    print("{0}".format(
                        detail.strftime('%d/%m/%Y %H:%M:%S')))
                elif detail is not None:
                    print("{0}".format(
                        str(detail)
                    ))
            else:
                sub_tasks = False
                if len(detail.items()) > 1:
                    sub_tasks = True
                    print("Tasks...")
                else:
                    print("Task...")
                for task, task_info in detail.items():
                    if sub_tasks:
                        print("Array ID: " + str(task))
                    for task_key in task_order:
                        try:
                            task_detail = task_info[task_key]
                        except KeyError:
                            continue
                        if task_detail is None:
                            continue
                        if task_key == 'status':
                            print(
                                "Job state: " + fsl_sub.consts.REPORTING[
                                    task_detail])
                        else:
                            print("{}: ".format(titlize_key(task_key)), end='')
                            if task_key in ['utime', 'stime', ]:
                                print("{0}s".format(task_detail))
                            elif task_key in ['maxmemory']:
                                print("{0}MB".format(task_detail))
                            elif task_key in [
                                    'sub_time', 'start_time', 'end_time']:
                                print(task_detail.strftime(
                                    '%d/%m/%Y %H:%M:%S'))
                            elif isinstance(task_detail, (list, tuple)):
                                print(', '.join([str(a) for a in task_detail]))
                            else:
                                print(str(task_detail))
    else:
        for sub_task, td in job_details['tasks'].items():
            line = []
            line.append(job_details['id'])
            line.append(sub_task)
            for key in order:
                if key == 'id':
                    continue
                if key == 'submission_time':
                    line.append(
                        job_details[key].strftime('%d/%m/%Y %H:%M:%S'))
                else:
                    line.append(blank_none(job_details[key]))
            for key in task_order:
                if key == 'status':
                    print(
                        "Job state: " + fsl_sub.consts.REPORTING[
                            td[key]])
                else:
                    print("{}: ".format(titlize_key(td), end=''))
                    if key in ['utime', 'stime', ]:
                        print("{0}s".format(blank_none(task_detail)))
                    if key in ['maxmemory']:
                        print("{0}MB".format(blank_none(task_detail)))
                    if key in ['sub_time', 'start_time', 'end_time']:
                        print(task_detail[key].strftime(
                            '%d/%m/%Y %H:%M:%S'))
                    else:
                        print(blank_none(task_detail))
            print('|'.join(line))


def main(args=None):
    lhdr = logging.StreamHandler()
    fmt = LogFormatter()
    lhdr.setFormatter(fmt)
    logger = logging.getLogger('fsl_sub')
    plugin_logger = logging.getLogger('fsl_sub.plugins')
    logger.addHandler(lhdr)
    plugin_logger.addHandler(lhdr)
    try:
        config = read_config()
        cp_info = coproc_info()
    except BadConfiguration as e:
        logger.error("Error in fsl_sub configuration - " + str(e))
        sys.exit(CONFIG_ERROR)

    PLUGINS = load_plugins()

    grid_module = 'fsl_sub_plugin_' + config['method']
    if grid_module not in PLUGINS:
        raise BadConfiguration(
            "{} not a supported method".format(config['method']))

    try:
        plugin_version = PLUGINS[grid_module].plugin_version
    except AttributeError as e:
        raise BadConfiguration(
            "Failed to load plugin " + grid_module
            + " ({0})".format(str(e))
        )

    cmd_parser = build_parser(
        config, cp_info, plugin_name=grid_module,
        plugin_version=plugin_version())
    options = vars(cmd_parser.parse_args(args=args))
    if options['show_config']:
        print(str(config))
        sys.exit(0)
    if not cp_info['available']:
        options['coprocessor'] = None
        options['coprocessor_class'] = None
        options['coprocessor_class_strict'] = False
        options['coprocessor_toolkits'] = None
        options['coprocessor_multi'] = 1
    else:
        if not cp_info['classes']:
            options['coprocessor_class'] = None
            options['coprocessor_class_strict'] = False
        if not cp_info['toolkits']:
            options['coprocessor_toolkits'] = None

    if options['verbose']:
        logger.setLevel(logging.INFO)
        plugin_logger.setLevel(logging.INFO)
    if options['debug']:
        logger.setLevel(logging.DEBUG)
        plugin_logger.setLevel(logging.DEBUG)
    if options['array_task'] and options['args']:
        cmd_parser.error(
            "Individual and array tasks are mutually exclusive."
        )
    if options['fileisimage']:
        logger.debug("Check file is image requested")
        try:
            if file_is_image(options['fileisimage']):
                logger.info("File is an image")
                sys.exit(0)
        except NoFsl as e:
            logger.warn(
                "No FSL found - " + str(e)
                + " assuming is image")
            sys.exit(0)
        except CommandError as e:
            cmd_parser.error(str(e))

    if options['parallelenv']:
        try:
            pe_name, threads = process_pe_def(
                options['parallelenv'], config['queues'])
        except ArgumentError as e:
            cmd_parser.error(str(e))
    else:
        pe_name, threads = (None, 1, )

    if options['array_task'] is not None:
        if options['array_native']:
            cmd_parser.error(
                "Array task files mutually exclusive with"
                " array native mode.")
        array_task = True
        command = options['array_task']
    elif options['array_native'] is None:
        array_task = False
        if (
                options['array_hold'] is not None
                or options['array_limit'] is not None):
            cmd_parser.error(
                "Array controls not applicable to non-array tasks")
        command = options['args']
    else:
        array_task = True
        command = options['args']
    if not command:
        cmd_parser.error("No command or array task file provided")

    for hold_spec in ['jobhold', 'array_hold']:
        if options[hold_spec] and not options['native_holds']:
            options[hold_spec] = options[hold_spec].split(',')

    if 'mailoptions' not in options:
        options['mailoptions'] = None
    if 'mailto' not in options:
        options['mailto'] = None

    if uses_projects():
        project = get_project_env(options['project'])
    else:
        project = None

    try:
        job_id = submit(
            command,
            architecture=options['arch'],
            array_hold=options['array_hold'],
            array_limit=options['array_limit'],
            array_specifier=options['array_native'],
            array_task=array_task,
            coprocessor=options['coprocessor'],
            coprocessor_toolkit=options['coprocessor_toolkit'],
            coprocessor_class=options['coprocessor_class'],
            coprocessor_class_strict=options['coprocessor_class_strict'],
            coprocessor_multi=options['coprocessor_multi'],
            name=options['name'],
            parallel_env=pe_name,
            queue=options['queue'],
            threads=threads,
            jobhold=options['jobhold'],
            jobram=options['jobram'],
            jobtime=options['jobtime'],
            logdir=options['logdir'],
            mail_on=options['mailoptions'],
            mailto=options['mailto'],
            priority=options['priority'],
            ramsplit=not options['noramsplit'],
            resources=options['resource'],
            usescript=options['usescript'],
            validate_command=not options['novalidation'],
            requeueable=not options['not_requeueable'],
            native_holds=options['native_holds'],
            as_tuple=False,
            project=project,
            export_vars=options['export'],
            keep_jobscript=options['keep_jobscript']
        )
    except BadSubmission as e:
        cmd_parser.exit(
            message="Error submitting job - " + str(e) + '\n',
            status=SUBMISSION_ERROR)
    except GridOutputError as e:
        cmd_parser.exit(
            message="Error submitting job - output from submission "
            "not understood. " + str(e) + '\n',
            status=RUNNER_ERROR)
    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        cmd_parser.error("Unexpected error: " + str(e) + '\n')
    print(job_id)


def update_parser(parser_class=argparse.ArgumentParser):
    '''Parse the command line, returns a dict keyed on option'''
    logger = logging.getLogger(__name__)

    logger.debug("updater: parsing arguments")

    parser = parser_class(
        prog="fsl_sub_update",
        usage="Check for fsl_sub updates",
        description="Check online for fsl_sub updates"
    )
    parser.add_argument(
        '--check', '-c', help="Check for updates", action="store_true"
    )
    parser.add_argument(
        '--yes', '-y', help="Update without confirmation", action="store_true"
    )
    parser.add_argument(
        '--test_local', '-t', help=argparse.SUPPRESS
    )
    return parser


def update(args=None):
    lhdr = logging.StreamHandler()
    fmt = LogFormatter()
    lhdr.setFormatter(fmt)
    logger = logging.getLogger('fsl_sub')
    logger.addHandler(lhdr)
    options = update_parser().parse_args(args=args)

    fsldir = find_fsldir()

    # Enumerate plugins
    plugins = available_plugin_packages()
    packages = ['fsl_sub', ] + plugins
    # Check for updates
    try:
        updates = conda_check_update(
            fsldir=fsldir, packages=packages)
        print("Available updates:")
        for u, v in updates.items():
            print("{0} ({1} -> {2})".format(
                u, v['old_version'], v['version']
            ))
    except UpdateError as e:
        sys.exit(
            "Unable to check for updates! ({0})".format(
                str(e)))

    if not options.check:
        if not options.yes:
            answer = user_input('Install pending updates? ')
            if answer.strip().lower() not in ['y', 'yes', ]:
                sys.exit('Aborted')
        try:
            updated = conda_update(
                fsldir=fsldir, packages=packages
            )
            print("{0} updated.".format(", ".join(updated)))
        except UpdateError as e:
            sys.exit(
                "Unable to update! ({0})".format(
                    str(e)
                )
            )


def instplugin_parser(parser_class=argparse.ArgumentParser):
    '''Parse the command line, returns a dict keyed on option'''
    logger = logging.getLogger(__name__)

    logger.debug("plugin installer: parsing arguments")

    parser = parser_class(
        prog="fsl_sub_install_plugin",
        usage="Download and install fsl_sub plugins",
        description="Simplify the installation of cluster backends for fsl_sub"
    )
    parser.add_argument(
        '--list', '-l', help="List available plugins", action="store_true"
    )
    parser.add_argument(
        '--install', '-i', help="Install requested plugin", default=None
    )
    parser.add_argument(
        '--test_local', '-t', help=argparse.SUPPRESS
    )

    return parser


def install_plugin(args=None):
    lhdr = logging.StreamHandler()
    fmt = LogFormatter()
    lhdr.setFormatter(fmt)
    logger = logging.getLogger('fsl_sub')
    logger.addHandler(lhdr)
    inst_parser = instplugin_parser()
    options = inst_parser.parse_args(args=args)

    fsldir = find_fsldir()

    try:
        fsl_sub_plugins = conda_find_packages(
            'fsl_sub_plugin_*', fsldir=fsldir)
    except PackageError:
        sys.exit("Unable to search for plugins - was this installed with FSL?")
    if options.list or options.install is None:
        print('Available plugins:')
        for index, plugin in enumerate(fsl_sub_plugins):
            if not options.list:
                print("{0}: {1}".format(index + 1, plugin))
            else:
                print(plugin)
        if options.list:
            sys.exit(0)
        else:
            try:
                plugin_index = int(user_input("Which backend? "))
                conda_pkg = fsl_sub_plugins[plugin_index - 1]
            except (ValueError, IndexError, ):
                sys.exit("Invalid plugin number")

    # Install
    if options.install:
        if options.install in fsl_sub_plugins:
            conda_pkg = options.install
        else:
            sys.exit("Unrecognised plugin")
    try:
        conda_install(conda_pkg)
    except InstallError as e:
        sys.exit(
            "Unable to install requested plugin! ({0})".format(
                str(e)))
    print("Plugin {0} installed". format(conda_pkg))
    print(
        """You can generate an example config file with:
fsl_sub_config {plugin}

The configuration file can be copied to {fsldir_etc_fslconf} calling "
it fsl_sub.yml, or put in your home folder calling it .fsl_sub.yml. "
A copy in your home folder will override the file in "
{fsldir_etc_fslconf}. Finally, the environment variable FSLSUB_CONF "
can be set to point at the configuration file, this will override all"
other files.""".format(
            plugin=conda_pkg.replace('fsl_sub_plugin_', ''),
            fsldir_etc_fslconf=os.path.join(fsldir, 'etc', 'fslconf'))
    )
