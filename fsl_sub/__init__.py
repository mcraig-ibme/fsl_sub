#!/usr/bin/env fslpython

# fsl_sub python module
# Copyright (c) 2018, University of Oxford (Duncan Mortimer)

import datetime
import errno
import getpass
import logging
import os
import socket
import shlex
import warnings
from math import ceil
from fsl_sub.exceptions import (
    BadConfiguration,
    BadSubmission,
    CommandError,
    LoadModuleError,
)
from fsl_sub.coprocessors import (
    coproc_load_module,
    max_coprocessors,
)
from fsl_sub.config import (
    read_config,
    method_config,
    coprocessor_config,
    uses_projects,
)
import fsl_sub.consts
from fsl_sub.projects import (
    get_project_env,
    project_exists,
)
from fsl_sub.utils import (
    load_plugins,
    check_command,
    check_command_file,
    control_threads,
    human_to_ram,
)
from fsl_sub.version import VERSION


def fsl_sub_warnings_formatter(
        message, category, filename, lineno, file=None, line=None):
    return str(message) + '\n'


warnings.formatwarning = fsl_sub_warnings_formatter
warnings.simplefilter('always', UserWarning)


def version():
    return VERSION


def report(
    job_id,
    subjob_id=None
):
    '''Request a job status. Returns a dictionary:
        id
        name
        script (if available)
        arguments (if available)
        submission_time
        tasks (dict keyed on sub-task ID):
            status:
                fsl_sub.consts.QUEUED
                fsl_sub.consts.RUNNING
                fsl_sub.consts.FINISHED
                fsl_sub.consts.FAILEDNQUEUED
                fsl_sub.consts.SUSPENDED
                fsl_sub.consts.HELD
            start_time
            end_time
            sub_time
            utime
            stime
            exit_status
            error_message
            maxmemory (in Mbytes)
        parents (if available)
        children (if available)
        job_directory (if available)
    '''

    PLUGINS = load_plugins()

    config = read_config()

    if config['method'] == 'shell':
        ntime = datetime.datetime.now()
        return {
            'id': 123456,
            'name': 'nojob',
            'script': None,
            'arguments': None,
            'submission_time': ntime,
            'tasks': {
                '1': {
                    'status': fsl_sub.consts.FINISHED,
                    'start_time': ntime,
                    'end_time': ntime,
                    'sub_time': ntime,
                    'utime': 0,
                    'stime': 0,
                    'exit_status': 0,
                    'error_message': None,
                    'maxmemory': 0
                }
            },
            'parents': None,
            'children': None,
            'job_directory': None,
            'fake': True
        }
    grid_module = 'fsl_sub_plugin_' + config['method']
    if grid_module not in PLUGINS:
        raise BadConfiguration(
            "{} not a supported method".format(config['method']))

    try:
        job_status = PLUGINS[grid_module].job_status
    except AttributeError as e:
        raise BadConfiguration(
            "Failed to load plugin " + grid_module
            + " ({0})".format(str(e))
        )

    return job_status(job_id, subjob_id)


def submit(
    command,
    name=None,
    threads=1,
    queue=None,
    jobhold=None,
    array_task=False,
    array_hold=None,
    array_limit=None,
    array_specifier=None,
    parallel_env=None,
    jobram=None,
    jobtime=None,
    resources=None,
    ramsplit=True,
    priority=None,
    validate_command=True,
    mail_on=None,
    mailto="{username}@{hostname}".format(
        username=getpass.getuser(),
        hostname=socket.gethostname()),
    logdir=None,
    coprocessor=None,
    coprocessor_toolkit=None,
    coprocessor_class=None,
    coprocessor_class_strict=False,
    coprocessor_multi="1",
    usescript=False,
    architecture=None,
    requeueable=True,
    native_holds=False,
    as_tuple=False,
    project=None,
    export_vars=[],
    keep_jobscript=False
):
    '''Submit job(s) to a queue, returns the job id as an int (pass as_tuple=True
    to return a single value tuple).
    Single tasks require a command in the form of a list [command, arg1,
    arg2, ...] or simple string "command arg1 arg2".
    Array tasks (array_task=True) require a file name of the array task table
    file unless array_specifier(=n[-m[:s]]) is specified in which case command
    is as per a single task.

    Requires:

    command - string or list containing command to run
                or the file name of the array task file.
                If array_specifier is given then this must be
                a string/list containing the command to run.

    Optional:
    job_name - Symbolic name for task (defaults to first component of command)
    array_task - is the command is an array task (defaults to False)
    jobhold - id(s) of jobs to hold for (string or list)
    array_hold - complex hold string
    array_limit - limit concurrently scheduled array
            tasks to specified number
    array_specifier - n[-m[:s]] n subtasks or starts at n, ends at m with
            a step of s.
    as_tuple - if true then return job ID as a single value tuple
    parallel_env - parallel environment name
    jobram - RAM required by job (total of all threads)
    jobtime - time (in minutes for task)
    requeueable - job may be requeued on node failure
    resources - list of resource request strings
    ramsplit - break tasks into multiple slots to meet RAM constraints
    priority - job priority (0-1023)
    mail_on - mail user on 'a'bort or reschedule, 'b'egin, 'e'nd,
            's'uspended, 'n'o mail
    mailto - email address to receive job info
    native_holds - whether to process the jobhold or array_hold input
    logdir - directory to put log files in
    coprocessor - name of coprocessor required
    coprocessor_toolkit - coprocessor toolkit version
    coprocessor_class - class of coprocessor required
    coprocessor_class_strict - whether to choose only this class
            or all more capable
    coprocessor_multi - how many coprocessors you need (or
            complex description) (string)
    queue - Explicit queue to submit to - use jobram/jobtime in preference to
            this
    usescript - queue config is defined in script
    project - Cluster project to submit job to, defaults to None
    export_vars - list of environment variables to preserve for job
            ignored if job is copying complete environment
    keep_jobscript - whether to generate and keep a script defining the parameters
            used to run your task
    '''
    logger = logging.getLogger(__name__)
    logger.debug("Submit called with:")
    logger.debug(
        " ".join(
            [
                str(a) for a in [
                    command, name, threads, queue, jobhold, array_task,
                    array_hold, array_limit, array_specifier, parallel_env,
                    jobram, jobtime, resources, ramsplit, priority,
                    validate_command, mail_on, mailto, logdir,
                    coprocessor, coprocessor_toolkit, coprocessor_class,
                    coprocessor_class_strict, coprocessor_multi,
                    usescript, architecture, requeueable, native_holds,
                    as_tuple, project,
                ]
            ]
        )
    )
    PLUGINS = load_plugins()

    config = read_config()

    grid_module = 'fsl_sub_plugin_' + config['method']
    if grid_module not in PLUGINS:
        raise BadConfiguration(
            "{} not a supported method".format(config['method']))

    try:
        queue_submit = PLUGINS[grid_module].submit
        qtest = PLUGINS[grid_module].qtest
        queue_exists = PLUGINS[grid_module].queue_exists
        BadSubmission = PLUGINS[grid_module].BadSubmission
        already_queued = PLUGINS[grid_module].already_queued
    except AttributeError as e:
        raise BadConfiguration(
            "Failed to load plugin " + grid_module
            + " ({0})".format(str(e))
        )
    if config['method'] != 'shell':
        if already_queued():
            config['method'] = 'shell'
            warnings.warn(
                'Warning: job on queue attempted to submit more jobs -'
                'running jobs using shell plugin instead.'
            )

    config['qtest'] = qtest()
    if config['qtest'] is None:
        config['method'] = 'shell'
        warnings.warn(
            'Warning: fsl_sub configured for {0} but {0}'
            ' software not found.'.format(config['method'])
        )

    mconfig = method_config(config['method'])
    parallel_env_requested = parallel_env

    if logdir is not None and logdir != "/dev/null":
        try:
            os.makedirs(logdir)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise BadSubmission(
                    "Unable to create {0} ({1})".format(
                        logdir, str(e)
                    ))
            else:
                if not os.path.isdir(logdir):
                    raise BadSubmission(
                        "Log destination is a file "
                        "(should be a folder)")

    if jobram is None:
        try:
            mem_requested = human_to_ram(
                os.environ['FSLSUB_MEMORY_REQUIRED'],
                config['ram_units'],
                as_int=True)
        except KeyError:
            pass
        except ValueError:
            logger.warn("FSLSUB_MEMORY_REQUIRED variable doesn't make sense")
        else:
            jobram = mem_requested

    if mconfig['mail_support'] is True:
        if mail_on is None:
            try:
                mail_on = mconfig['mail_mode']
            except KeyError:
                warnings.warn(
                    "Mail not configured but enabled in configuration for "
                    + config['method'])
        else:
            # Mail modes is a dictionary
            if mail_on not in mconfig['mail_modes']:
                raise BadSubmission(
                    "Unrecognised mail mode " + mail_on)

    # For simple numbers pass these in as list, if they are strings
    # then leave them alone
    if jobhold is not None:
        if not isinstance(jobhold, (str, int, list, tuple)):
            raise BadSubmission(
                "jobhold must be a string, int, list or tuple")
        if not native_holds:
            if isinstance(jobhold, str):
                jobhold = jobhold.split(',')
    if array_hold is not None:
        if not isinstance(array_hold, (str, int, list, tuple)):
            raise BadSubmission(
                "array_hold must be a string, int, list or tuple")
        if not native_holds:
            array_hold = array_hold.split(',')

    validate_type = 'command'
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
        if (
                array_hold is not None
                or array_limit is not None
                or array_specifier is not None):
            raise BadSubmission(
                "Array controls not applicable to non-array tasks")
        job_args = command
    elif array_specifier is None:
        job_type = 'array file'
        validate_type = 'array'
        job_args = [command, ]
        if name is None:
            name = os.path.basename(command)
    else:
        job_type = 'array aware command'
        validate_type = 'command'
        if isinstance(command, str):
            # command is a basic string
            command = shlex.split(command)
        else:
            raise BadSubmission("Command should be a list or string")
        if validate_command:
            try:
                check_command(command[0])
            except CommandError as e:
                raise BadSubmission(
                    "Problem with task command: " + str(e)
                )
        job_args = command

    if validate_command:
        if validate_type == 'array':
            try:
                check_command_file(command)
            except CommandError as e:
                raise BadSubmission(
                    "Array task definition file fault: " + str(e)
                )
        elif validate_type == 'command':
            try:
                check_command(command[0])
            except CommandError as e:
                raise BadSubmission(
                    "Command not usable: " + str(e)
                )
        else:
            raise BadConfiguration(
                "Unknown validation type: " + validate_type)
    logger.info(
        "METHOD={0} : TYPE={1} : args={2}".format(
            config['method'],
            job_type,
            " ".join(job_args)
        ))
    if name is None:
        if isinstance(command, list):
            c_name = command[0]
        else:
            c_name = command.shlex.split()[0]
        if '/' in c_name:
            c_name = os.path.basename(c_name)
        task_name = c_name
    else:
        task_name = name

    if mconfig['queues'] is False:
        queue = None
        split_on_ram = None
    else:
        split_on_ram = mconfig['map_ram'] and ramsplit

        if (split_on_ram
                and parallel_env is None
                and 'large_job_split_pe' in mconfig):
            parallel_env = mconfig['large_job_split_pe']

        if queue is None:
            queue_details = getq_and_slots(
                job_time=jobtime,
                job_ram=jobram,
                job_threads=threads,
                queues=config['queues'],
                coprocessor=coprocessor,
                ll_env=parallel_env
            )
            logger.debug("Automatic queue selection:")
            logger.debug(queue_details)
            (queue, slots_required) = queue_details
        else:
            if not queue_exists(queue):
                raise BadSubmission("Unrecognised queue " + queue)
            logger.debug("Specific queue: " + queue)
            if queue in config['queues']:
                slots_required = calc_slots(
                    jobram,
                    config['queues'][queue]['slot_size'],
                    threads)
            else:
                slots_required = 1

        threads = max(slots_required, threads)
        # If a subsequent task calls fsl_sub too then we need to avoid
        # further parallelisation. Store allowed number of threads in
        # FSLSUB_PARALLEL...
        # Clear previous FSLSUB_PARALLEL values...
        if 'FSLSUB_PARALLEL' not in export_vars:
            export_vars.append('FSLSUB_PARALLEL')

        control_threads(config['thread_control'], threads)

        if threads == 1 and parallel_env_requested is None:
            parallel_env = None
        if threads > 1 and parallel_env is None:
            raise BadSubmission(
                "Job requires {} slots but no parallel envrionment "
                "available or requested".format(threads))
        if threads > 1 and mconfig['thread_ram_divide'] and not split_on_ram:
            split_on_ram = True

    if coprocessor:
        if mconfig['queues']:
            if coprocessor_multi != '1':
                try:
                    if int(coprocessor_multi) > max_coprocessors(coprocessor):
                        raise BadSubmission(
                            "Unable to provide {} coprocessors for job".format(
                                coprocessor_multi
                            ))

                except ValueError:
                    # Complex coprocessor_multi passed - do not validate
                    pass
        if coprocessor_toolkit:
            logger.debug("Attempting to load coprocessor toolkit")
            logger.debug(":".join((coprocessor, coprocessor_toolkit)))
            try:
                coproc_load_module(
                    coprocessor_config(coprocessor),
                    coprocessor_toolkit)
            except LoadModuleError:
                raise BadSubmission(
                    "Unable to load requested coprocessor toolkit"
                )
    if uses_projects():
        q_project = get_project_env(project)
        if q_project is not None and not project_exists(q_project):
            raise BadSubmission(
                "Project not recognised"
            )
    else:
        q_project = None

    logger.debug("Calling queue_submit fsl_sub_plugin_{0} with: ".format(config['method']))
    logger.debug(
        ", ".join(
            [str(a) for a in [
                command, task_name, queue, jobhold, array_task,
                array_hold, array_limit, array_specifier, parallel_env,
                jobram, jobtime, resources, ramsplit, priority,
                mail_on, mailto, logdir, coprocessor, coprocessor_toolkit,
                coprocessor_class, coprocessor_class_strict, coprocessor_multi,
                usescript, architecture, requeueable]]))

    # If not already set, set FSLSUB_PARALLEL so any sub-fsl_subs know how many
    # threads the parent has been allocated
    if 'FSLSUB_PARALLEL' not in os.environ.keys():
        os.environ['FSLSUB_PARALLEL'] = str(threads)
    job_id = queue_submit(
        command,
        job_name=task_name,
        threads=threads,
        queue=queue,
        jobhold=jobhold,
        array_task=array_task,
        array_hold=array_hold,
        array_limit=array_limit,
        array_specifier=array_specifier,
        parallel_env=parallel_env,
        jobram=jobram,
        jobtime=jobtime,
        resources=resources,
        ramsplit=split_on_ram,
        priority=priority,
        mail_on=mail_on,
        mailto=mailto,
        logdir=logdir,
        coprocessor=coprocessor,
        coprocessor_toolkit=coprocessor_toolkit,
        coprocessor_class=coprocessor_class,
        coprocessor_class_strict=coprocessor_class_strict,
        coprocessor_multi=coprocessor_multi,
        usescript=usescript,
        architecture=architecture,
        requeueable=requeueable,
        project=q_project,
        export_vars=export_vars,
        keep_jobscript=keep_jobscript
    )

    if as_tuple:
        return (job_id,)
    else:
        return job_id


def calc_slots(job_ram, slot_size, job_threads):
    '''Calculate how many slots would be necessary to
    provide this job request'''
    logger = logging.getLogger(__name__)
    logger.debug(
        "Calc slots based on JR:SS:JT - {0}:{1}:{2}".format(
            job_ram, slot_size, job_threads
        ))
    if job_ram == 0 or job_ram is None:
        return max(1, job_threads)
    else:
        return max(int(ceil(job_ram / slot_size)), job_threads)


def getq_and_slots(
        queues, job_time=0, job_ram=0,
        job_threads=1, coprocessor=None,
        ll_env=None):
    '''Calculate which queue to run the job on. job_time is in minutes, job_ram in units given in configuration.
    Still needs job splitting across slots'''
    logger = logging.getLogger(__name__)
    if job_ram is None:
        job_ram = 0

    queue_list = list(queues.keys())

    if not queue_list:
        raise BadSubmission("No matching queues found")

    # Filter on coprocessor availability
    if coprocessor is not None:
        queue_list = [
            q for q in queue_list if 'copros' in queues[q]
            and coprocessor in queues[q]['copros']]
    else:
        queue_list = [
            q for q in queue_list if 'copros' not in queues[q]
        ]
    if not queue_list:
        raise BadSubmission("No matching queues found")

    # Filter on parallel environment availability
    if ll_env is not None:
        queue_list = [
            q for q in queue_list if 'parallel_envs' in queues[q]
            and ll_env in queues[q]['parallel_envs']
        ]
    if not queue_list:
        raise BadSubmission("No matching queues found")

    # If no job time was specified then find the default queues
    # (if defined)
    if job_time is None or job_time == 0:
        d_queues = [
            q for q in queue_list if 'default' in queues[q]
        ]
        if d_queues:
            queue_list = d_queues

        job_time = 0

    slots = {}
    for index, q in enumerate(queue_list):
        slots[q] = calc_slots(
            job_ram, queues[q]['slot_size'], job_threads)
        # If group/priority not specified then create pseudo-groups, one for each queue
        if 'group' not in queues[q].keys():
            queues[q]['group'] = index
        if 'priority' not in queues[q].keys():
            queues[q]['priority'] = 1

    queue_list.sort(key=lambda x: queues[x]['priority'], reverse=True)
    queue_list.sort(key=lambda x: (queues[x]['group'], slots[x]))

    ql = [
        q for q in queue_list if queues[q]['time'] >= job_time
        and queues[q]['max_size'] >= job_ram
        and queues[q]['max_slots'] >= job_threads]
    if not ql:
        raise BadSubmission("No matching queues found")

    logger.info(
        "Estimated RAM was {0} GBm, runtime was {1} minutes.\n".format(
            job_ram, job_time
        ))
    if coprocessor:
        logger.info("Co-processor {} was requested".format(coprocessor))
    if len(ql):
        logger.info(
            "Appropriate queue is {}".format(ql[0]))
    try:
        q_tuple = (ql[0], slots[ql[0]])
    except IndexError:
        raise BadSubmission("No matching queues found")
    return q_tuple
