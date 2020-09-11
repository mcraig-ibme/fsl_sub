# fsl_sub python module
# Copyright (c) 2018, University of Oxford (Duncan Mortimer)

# fsl_sub plugin for running directly on this computer
import logging
import os
import shlex
import subprocess as sp

from fsl_sub.config import method_config
from fsl_sub.exceptions import (BadSubmission, MissingConfiguration, )
from fsl_sub.utils import parse_array_specifier
from collections import defaultdict
from itertools import zip_longest


def plugin_version():
    return '2.0.0'


def qtest():
    '''Command that confirms method is available'''
    return True


def queue_exists(qname, qtest=qtest()):
    '''Command that confirms a queue is available'''
    return True


def already_queued():
    '''Is this a running in a submitted job?'''
    return False


def _disable_parallel(job):
    mconf = defaultdict(lambda: False, method_config('shell'))

    matches = mconf['parallel_disable_matches']

    if matches:
        for m in matches:
            if '/' not in m:
                job = os.path.basename(job)
            if m.startswith('*') and job != m.strip('*') and job.endswith(m.strip('*')):
                return True
            if m.endswith('*') and job != m.strip('*') and job.startswith(m.strip('*')):
                return True
            if job == m:
                return True
    return False


def submit(
        command,
        job_name,
        queue=None,
        array_task=False,
        array_limit=None,
        array_specifier=None,
        logdir=None,
        **kwargs):
    '''Submits the job'''
    logger = logging.getLogger('fsl_sub.plugins')
    mconf = defaultdict(lambda: False, method_config('shell'))
    pid = os.getpid()
    if logdir is None:
        logdir = os.getcwd()
    logfile_base = os.path.join(logdir, job_name)
    stdout = "{0}.{1}{2}".format(logfile_base, 'o', pid)
    stderr = "{0}.{1}{2}".format(logfile_base, 'e', pid)

    if not array_task or (array_task and array_specifier):
        if isinstance(command, str):
            command = shlex.split(command)

    child_env = dict(os.environ)
    child_env['FSLSUB_JOBID_VAR'] = 'JOB_ID'
    child_env['FSLSUB_ARRAYTASKID_VAR'] = 'SHELL_TASK_ID'
    child_env['FSLSUB_ARRAYSTARTID_VAR'] = 'SHELL_TASK_FIRST'
    child_env['FSLSUB_ARRAYENDID_VAR'] = 'SHELL_TASK_LAST'
    child_env['FSLSUB_ARRAYSTEPSIZE_VAR'] = 'SHELL_TASK_STEPSIZE'
    child_env['FSLSUB_ARRAYCOUNT_VAR'] = 'SHELL_ARRAYCOUNT'
    jobs = []
    array_args = {}

    if array_task:
        logger.debug("Array task requested")
        if not mconf['run_parallel']:
            array_args['parallel_limit'] = 1
        if array_limit is not None:
            logger.debug("Limiting number of parallel tasks to " + array_limit)
            array_args['parallel_limit'] = array_limit
        if array_specifier:
            logger.debug("Attempting to parse array specifier " + array_specifier)
            (
                array_start,
                array_end,
                array_stride
            ) = parse_array_specifier(array_specifier)
            if not array_start:
                raise BadSubmission("array_specifier doesn't make sense")
            if array_end is None:
                # array_start is the number of jobs
                njobs = array_start
            else:
                njobs = array_end - array_start

                if array_stride is not None:
                    njobs = (njobs // array_stride) + 1
                    array_args['array_stride'] = array_stride
                array_args['array_start'] = array_start
                array_args['array_end'] = array_end
            jobs += njobs * [command]
            if _disable_parallel(command[0]):
                array_args['parallel_limit'] = 1
            else:
                if array_limit is not None:
                    array_args['parallel_limit'] = array_limit
        else:
            try:
                with open(command, 'r') as ll_tasks:
                    command_lines = ll_tasks.readlines()
                for cline in command_lines:
                    jobs.append(shlex.split(cline))
            except Exception as e:
                raise BadSubmission(
                    "Unable to read array task file "
                    + ' '.join(command)) from e
            if any([_disable_parallel(m[0]) for m in jobs]):
                array_args['parallel_limit'] = 1
            else:
                if array_limit is not None:
                    array_args['parallel_limit'] = array_limit

        _run_parallel(jobs, pid, child_env, stdout, stderr, **array_args)
    else:
        _run_job(command, pid, child_env, stdout, stderr)
    return pid


def _grouper(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx"
    args = [iter(iterable)] * n
    return zip_longest(*args, fillvalue=fillvalue)


def _run_job(job, job_id, child_env, stdout_file, stderr_file):
    logger = logging.getLogger('fsl_sub.plugins')
    with open(stdout_file, mode='w') as stdout:
        with open(stderr_file, mode='w') as stderr:
            child_env['JOB_ID'] = str(job_id)
            logger.info(
                "executing: " + str(' '.join(job)))

            output = sp.run(
                job,
                stdout=stdout,
                stderr=stderr,
                universal_newlines=True,
                env=child_env)

    if output.returncode != 0:
        with open(stderr_file, mode='r') as stderr:
            raise BadSubmission(
                stderr.read())


def _end_job_number(njobs, start, stride):
    return (njobs - 1) * stride + start


def _run_parallel(
        jobs, parent_id, parent_env, stdout_file, stderr_file,
        parallel_limit=None, array_start=1, array_end=None, array_stride=1):
    '''Run jobs in parallel - pass parallel_limit=1 to run array tasks linearly'''
    logger = logging.getLogger('fsl_sub.plugins')
    if array_end is None:
        array_end = _end_job_number(len(jobs), array_start, array_stride)
    logger.info("Running jobs in parallel")
    available_cores = _get_cores()

    if parallel_limit is not None and available_cores > parallel_limit:
        available_cores = parallel_limit

    logger.debug("Have {0} cores available for parallelising over".format(available_cores))

    errors = []
    for group, job_group in enumerate(_grouper(jobs, available_cores)):

        children = []
        for group_job, job in enumerate(job_group):
            if job is not None:
                if type(job) is str:
                    job = shlex.split(job)
                sub_id = group * available_cores + group_job + 1
                child_env = dict(parent_env)

                if stdout_file != '/dev/null':
                    child_stdout = '.'.join((stdout_file, str(sub_id)))
                else:
                    child_stdout = stdout_file

                if stderr_file != '/dev/null':
                    child_stderr = '.'.join((stderr_file, str(sub_id)))
                else:
                    child_stderr = stderr_file

                with open(child_stdout, mode='w') as stdout:
                    with open(child_stderr, mode='w') as stderr:
                        child_env['JOB_ID'] = str(parent_id)
                        child_env['SHELL_TASK_ID'] = str(sub_id)
                        child_env['SHELL_TASK_FIRST'] = str(array_start)
                        child_env['SHELL_TASK_LAST'] = str(array_end)
                        child_env['SHELL_TASK_STEPSIZE'] = str(array_stride)
                        child_env['SHELL_ARRAYCOUNT'] = ''
                        logger.info(
                            "executing: (" + str(sub_id) + ")" + str(job))

                        children.append(
                            (
                                sp.Popen(
                                    job,
                                    stdout=stdout,
                                    stderr=stderr,
                                    universal_newlines=True,
                                    env=child_env),
                                child_stdout,
                                child_stderr,
                            )
                        )

        # Wait for children
        errors.extend(_wait_for_children(children))

    if errors:
        raise BadSubmission('\n'.join(errors))


def _wait_for_children(children):
    errors = []
    for child in children:
        child[0].wait()
        if child[0].returncode != 0:
            try:
                with open(child[2], mode='r') as stderr_f:
                    errors.append(stderr_f.read())
            except FileNotFoundError:
                errors.append("Command {0} generated no output!".format(' '.join(child[0].args)))
    return errors


def _cores():
    '''Obtain maximum number of cores available to us (observing any core masking, where OS supports)'''
    try:
        available_cores = len(os.sched_getaffinity(0))
    except AttributeError:
        # macOS doesn't support above so fall back to count of all CPUs
        available_cores = os.cpu_count()
    return available_cores


def _get_cores():
    '''Obtain maximum number of cores available to us (observing any core masking, where OS supports)'''
    available_cores = _cores()
    try:
        fslsub_parallel = int(os.environ['FSLSUB_PARALLEL'])
        if fslsub_parallel > 0 and available_cores > fslsub_parallel:
            available_cores = fslsub_parallel
    except (KeyError, ValueError, ):
        pass

    return available_cores


def _default_config_file():
    return os.path.join(
        os.path.realpath(os.path.dirname(__file__)),
        'fsl_sub_shell.yml')


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

    return None
