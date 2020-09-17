# fsl_sub plugin for:
#  * Slurm
import datetime
import logging
import os
import subprocess as sp
import sys
import tempfile
from collections import defaultdict
from shutil import which

from fsl_sub.version import (VERSION, )
from fsl_sub.exceptions import (
    BadSubmission,
    BadConfiguration,
    MissingConfiguration,
    GridOutputError,
    UnknownJobId,
)
from fsl_sub.config import (
    method_config,
    coprocessor_config,
    read_config,
)
import fsl_sub.consts
from fsl_sub.coprocessors import (
    coproc_get_module
)
from fsl_sub.shell_modules import loaded_modules
from fsl_sub.utils import (
    split_ram_by_slots,
    human_to_ram,
    parse_array_specifier,
    bash_cmd,
    fix_permissions,
    flatten_list,
    write_wrapper,
    job_script,
    update_envvar_list,
)
from .version import PLUGIN_VERSION


METHOD_NAME = "myplugin"


def plugin_version():
    return PLUGIN_VERSION


def qtest():
    '''Command that confirms method is available'''
    return qconf_cmd()


def queue_exists(qname, qtest=None):
    '''Does qname exist'''
    # Return True if queue exists

    return False


def already_queued():
    '''Is this a running job?'''
    # Change these environment variables that indicate you are running in a queue already.
    return ('CLUSTER_JOB_ID' in os.environ.keys() or 'CLUSTER_JOBID' in os.environ.keys())


def project_list():
    '''This returns a list of recognised projects (or accounts) that a job
    can be allocated to (e.g. for billing or fair share allocation)'''
    # Add code to get the configured projects from the cluster software


def build_queue_defs():
    '''Not currently implemented'''
    return ''


def _get_logger():
    return logging.getLogger('fsl_sub.' + __name__)


def submit(
        command,
        job_name,
        queue,
        threads=1,
        array_task=False,
        jobhold=None,
        array_hold=None,
        array_limit=None,
        array_specifier=None,
        parallel_env=None,
        jobram=None,
        jobtime=None,
        resources=None,
        ramsplit=False,
        priority=None,
        mail_on=None,
        mailto=None,
        logdir=None,
        coprocessor=None,
        coprocessor_toolkit=None,
        coprocessor_class=None,
        coprocessor_class_strict=False,
        coprocessor_multi=1,
        usescript=False,
        architecture=None,
        requeueable=True,
        project=None,
        export_vars=[],
        keep_jobscript=None):
    '''Submits the job to a SLURM cluster
    Requires:

    command - list containing command to run
                or the file name of the array task file.
                If array_specifier is given then this must be
                a list containing the command to run.
    job_name - Symbolic name for task
    queue - Queue to submit to

    Optional:
    array_task - is the command is an array task (defaults to False)
    jobhold - id(s) of jobs to hold for (string or list)
    array_hold - complex hold string
    array_limit - limit concurrently scheduled array
            tasks to specified number
    array_specifier - n[-m[:s]] n subtasks or starts at n, ends at m with
            a step of s
    parallelenv - parallel environment name
    jobram - RAM required by job (total of all threads)
    jobtime - time (in minutes for task)
    requeueable - may a job be requeued if a node fails
    resources - list of resource request strings
    ramsplit - break tasks into multiple slots to meet RAM constraints
    priority - job priority (0-1023)
    mail_on - mail user on 'a'bort or reschedule, 'b'egin, 'e'nd,
            's'uspended, 'n'o mail
    mailto - email address to receive job info
    logdir - directory to put log files in
    coprocessor - name of coprocessor required
    coprocessor_toolkit - coprocessor toolkit version
    coprocessor_class - class of coprocessor required
    coprocessor_class_strict - whether to choose only this class
            or all more capable
    coprocessor_multi - how many coprocessors you need (or
            complex description) (string)
    usescript - queue config is defined in script
    project - which account to associate this job with
    export_vars - list of environment variables to preserve for job
            ignored if job is copying complete environment
    keep_jobscript - whether to generate (if not configured already) and keep
            a wrapper script for the job
    '''

    logger = _get_logger()
    my_export_vars = list(export_vars)
    if command is None:
        raise BadSubmission(
            "Must provide command line or array task file name")
    if not isinstance(command, list):
        raise BadSubmission(
            "Internal error: command argument must be a list"
        )

    # Set this to the name of the plugin, e.g. a in fsl_sub_plugin_a
    mconf = defaultdict(lambda: False, method_config(METHOD_NAME))
    qsub = qsub_cmd()
    command_args = []

    modules = []
    mconfig = method_config(METHOD_NAME)
    if logdir is None:
        logdir = os.getcwd()
    if isinstance(resources, str):
        resources = [resources, ]

    # This maps FSLSUB task variables to Cluster software variables
    array_map = {
        'FSLSUB_JOB_ID_VAR': 'CLUSTER_JOB_ID',
        'FSLSUB_ARRAYTASKID_VAR': 'CLUSTER_ARRAY_TASK_ID',
        'FSLSUB_ARRAYSTARTID_VAR': 'CLUSTER_ARRAY_TASK_MIN',
        'FSLSUB_ARRAYENDID_VAR': 'CLUSTER_ARRAY_TASK_MAX',
        'FSLSUB_ARRAYSTEPSIZE_VAR': 'CLUSTER_ARRAY_TASK_STEP',
        'FSLSUB_ARRAYCOUNT_VAR': 'CLUSTER_ARRAY_TASK_COUNT',
    }

    gres = []
    if usescript:
        if len(command) > 1:
            raise BadSubmission(
                "Command should be a grid submission script (no arguments)")
        use_jobscript = False
        keep_jobscript = False
    else:
        use_jobscript = mconf.get('use_jobscript', True)
        if keep_jobscript is None:
            keep_jobscript = mconf.get('keep_jobscript', False)
        if keep_jobscript:
            use_jobscript = True
        # Check Parallel Environment is available
        if parallel_env:
            command_args.extend(
                [#PARALLEL ENVIRONMENT ARGUMENT, str(threads),
                ])

        for var, value in array_map.items():
            if not value:
                value = '""'
            update_envvar_list(my_export_vars, '='.join((var, value)))
        if mconf.get('copy_environment', False):
            # Add queue's argument for cloning current environment

        if my_export_vars:
            command_args.append(# Queue argument for exporting variables to job
            )

        if coprocessor is not None:
            # Setup the coprocessor
            cpconf = coprocessor_config(coprocessor)

            if cpconf['classes']:
                available_classes = cpconf['class_types']
                if coprocessor_class is None:
                    coprocessor_class = cpconf['default_class']

                try:
                    copro_class = available_classes[
                        coprocessor_class][
                            'resource']
                except KeyError:
                    raise BadSubmission("Unrecognised coprocessor class")

                # Add arguments that will select/configure the coprocessor

        if logdir == '/dev/null':
            # Add arguments to point stdout and stderr to /dev/null
        else:
            # Add arguments to define stdout and stderr log paths

        # Processing of jobhold necessary for array_holds
        if jobhold:
            # Add argument for configuring a job jold
        if array_task is not None:
            if mconf['array_limits'] and array_limit:
                # Add argument to limit array task concurrent processes

        if jobram:
            if ramsplit:
                # Modify jobram if necessary...
                # jobram = split_ram_by_slots(jobram, threads)
            ram_units = read_config()['ram_units']

            # RAM is specified in megabytes
            try:
                mem_in_mb = human_to_ram(
                    jobram,
                    units=ram_units,
                    output="M")
            except ValueError:
                raise BadConfiguration("ram_units not one of P, T, G, M, K")
            if mconf['notify_ram_usage']:
                # Add argument to notify cluster of the RAM requirements of the job
        if mconf['mail_support']:
            if mailto:
                if not mail_on:
                    mail_on = mconf['mail_mode']
                if mail_on not in mconf['mail_modes']:
                    raise BadSubmission("Unrecognised mail mode")
                # Add argument to enable mail

        command_args.append(
            # Argument to specify job name
        )

        command_args.append(# Argument to specify queue/partition
        )

        command_args.append(# Any other arguments needed...
        )

        if requeueable:
            command_args.append(# Argument that specifies requeueable
            )

        if project is not None:
            command_args.append(# Argument to specify project
            )

        if array_task:
            # Submit array task file
            if array_specifier:
                (
                    array_start,
                    array_end,
                    array_stride
                ) = parse_array_specifier(array_specifier)
                if not array_start:
                    raise BadSubmission("array_specifier doesn't make sense")
                command_args.append(# Argument to describe an array task
                )
            else:
                with open(command[0], 'r') as cmd_f:
                    array_slots = len(cmd_f.readlines())
                command_args.append(# Argument to describe an array task
                )

    logger.info(method_name + ": " + " ".join(
        [str(a) for a in command_args if a != qsub]))

    bash = bash_cmd()

    if array_task and not array_specifier:
        logger.info("executing array task")
    else:
        if usescript:
            logger.info("executing cluster script")
        else:
            if array_specifier:
                logger.info("excuting array task {0}-{1}:{2}".format(
                    array_start,
                    array_end,
                    array_stride
                ))
            else:
                logger.info("executing single task")

    logger.info(" ".join([str(a) for a in command_args]))
    logger.debug(type(command_args))
    logger.debug(command_args)

    extra_lines = []
    if array_task and not array_specifier:
        extra_lines = [
            '',
            'the_command=$(sed -n -e "${{VARIABLE INDICATING_ARRAY_TASK_ID}}p" {0})'.format(command), # Change this line
            '',
        ]
        command = ['exec', bash, '-c', '"$the_command"', ]
        command_args = command_args if use_jobscript else []
        use_jobscript = True

    if mconfig.get('preserve_modules', True):
        modules = loaded_modules()
        if coprocessor_toolkit:
            cp_module = coproc_get_module(coprocessor, coprocessor_toolkit)
            if cp_module is not None:
                modules.append(cp_module)
    js_lines = job_script(
        command, command_args, modules=modules, extra_lines=extra_lines)
    logger.debug('\n'.join(js_lines))
    if keep_jobscript:
        wrapper_name = write_wrapper(js_lines)
        logger.debug(wrapper_name)
        command_args = [wrapper_name]
        logger.debug("Calling fix_permissions " + str(0o755))
        fix_permissions(wrapper_name, 0o755)
    else:
        if not usescript:
            command_args = []
        else:
            command_args = flatten_list(command_args)
            command_args.extend(command)

    command_args.insert(0, qsub)

    if keep_jobscript:
        result = sp.run(
            command_args, universal_newlines=True,
            stdout=sp.PIPE, stderr=sp.PIPE)
    else:
        result = sp.run(
            command_args,
            input='\n'.join(js_lines),
            universal_newlines=True,
            stdout=sp.PIPE, stderr=sp.PIPE
        )
    if result.returncode != 0:
        raise BadSubmission(result.stderr)
    job_words = result.stdout.split(';')
    try:
        job_id = int(job_words[0].split('.')[0])
    except ValueError:
        raise GridOutputError("Grid output was " + result.stdout)

    if keep_jobscript:
        new_name = os.path.join(
            os.getcwd(),
            '_'.join(('wrapper', str(job_id))) + '.sh'
        )
        try:
            logger.debug("Renaming wrapper to " + new_name)
            os.rename(
                wrapper_name,
                new_name
            )
        except OSError:
            logger.warn("Unable to preserve wrapper script")
    return job_id


def _default_config_file():
    return os.path.join(
        os.path.realpath(os.path.dirname(__file__)),
        'fsl_sub_' + method + '.yml')


def example_conf():
    '''Returns a string containing the example configuration for this
    cluster plugin.'''

    try:
        with open(_default_config_file()) as e_conf_f:
            e_conf = e_conf_f.read()
    except FileNotFoundError as e:
        raise MissingConfiguration("Unable to find example configuration file: " + str(e))
    return e_conf


def job_status(job_id, sub_job_id=None):
    '''Return details for the job with given ID.

    details holds a dict with following info:
        id
        name
        script (if available)
        arguments (if available)
        # sub_state: fsl_sub.consts.NORMAL|RESTARTED|SUSPENDED
        submission_time
        tasks (dict keyed on sub-task ID):
            status:
                fsl_sub.consts.QUEUED
                fsl_sub.consts.RUNNING
                fsl_sub.consts.FINISHED
                fsl_sub.consts.FAILEDNQUEUED
                fsl_sub.consts.HELD
            start_time
            end_time
            sub_time
            exit_status
            error_message
            maxmemory (in Mbytes)
        parents (if available)
        children (if available)
        job_directory (if available)

        '''

    # Look for running jobs
    if isinstance(job_id, str):
        if '.' in job_id:
            if sub_job_id is None:
                job_id, sub_job_id = job_id.split('.')
                sub_job_id = int(sub_job_id)
            else:
                job_id, _ = job_id.split('.')
        job_id = int(job_id)
    if isinstance(sub_job_id, str):
        sub_job_id = int(sub_job_id)

    try:
        job_details = _running_job(job_id, sub_job_id)
        if job_details:
            return job_details
        else:
            job_details = _finished_job(job_id, sub_job_id)

    except UnknownJobId as e:
        raise
    except Exception as e:
        raise GridOutputError from e

    return job_details


def _running_job(job_id, sub_job_id=None):
    # Get information on a running job
    return job_info


def _finished_job(job_id, sub_job_id=None):
    # Get information on a finished job
    return job_info

