# fsl_sub python module
# Copyright (c) 2018, University of Oxford (Duncan Mortimer)

# fsl_sub plugin for running directly on this computer
import logging
import os
import shlex
import subprocess as sp

from fsl_sub.exceptions import BadSubmission
from fsl_sub.utils import parse_array_specifier


def qtest():
    '''Command that confirms method is available'''
    return True


def queue_exists(qname, qtest=qtest()):
    '''Command that confirms a queue is available'''
    return True


def submit(
        command,
        job_name,
        queue=None,
        threads=1,
        array_task=False,
        array_specifier=None,
        logdir=None,
        **kwargs):
    '''Submits the job'''
    logger = logging.getLogger('fsl_sub.plugins')

    pid = os.getpid()
    if logdir is None:
        logdir = os.getcwd()
    logfile_base = os.path.join(logdir, job_name)
    stdout = "{0}.{1}{2}".format(logfile_base, 'o', pid)
    stderr = "{0}.{1}{2}".format(logfile_base, 'e', pid)
    os.environ['FSLSUB_ARRAYTASKID_VAR'] = 'NONE_TASK_ID'
    os.environ['FSLSUB_ARRAYSTARTID_VAR'] = 'NONE_TASK_FIRST'
    os.environ['FSLSUB_ARRAYENDID_VAR'] = 'NONE_TASK_LAST'
    os.environ['FSLSUB_ARRAYSTEPSIZE_VAR'] = 'NONE_TASK_STEPSIZE'
    os.environ['FSLSUB_ARRAYCOUNT_VAR'] = ''
    command_lines = []
    array_start, array_end, array_stride = (1, 1, 1)
    if array_task and not array_specifier:
        try:
            with open(command, 'r') as ll_tasks:
                commands = ll_tasks.readlines()
        except Exception as e:
            raise BadSubmission(
                "Unable to read array task file " +
                command) from e
        command_lines.extend(commands)
        logger.info(
            "Running commands in: " + command)
        for line in range(0, len(commands)):
            logger.info(
                "{line}: {command}".format(
                    line=line,
                    command=command_lines[line]
                )
            )
    else:
        command_lines.append(command)
        if array_specifier:
            (
                array_start,
                array_end,
                array_stride
                ) = parse_array_specifier(array_specifier)
            if not array_start:
                raise BadSubmission("array_specifier doesn't make sense")
            if not array_end:
                array_end = array_start - 1
                array_start = 1
            if not array_stride:
                array_stride = 1

    # If array task then run each line in turn unless array_specifier
    # in which case run the same command x times setting env. vars
    # appropriately.

    for (line, cmd) in enumerate(command_lines):
        if array_task and not array_specifier:
            os.environ['NONE_TASK_ID'] = str(line + 1)
            os.environ['NONE_TASK_FIRST'] = "1"
            os.environ['NONE_TASK_LAST'] = str(len(commands))
            os.environ['NONE_TASK_STEPSIZE'] = "1"
            os.environ['FSLSUB_ARRAYCOUNT_VAR'] = ''
            stdout_f = "{0}.{1}".format(
                stdout, line + 1)
            stderr_f = "{0}.{1}".format(
                stderr, line + 1)
        else:
            (stdout_f, stderr_f) = (stdout, stderr)
        if isinstance(cmd, str):
            cmd = shlex.split(cmd)
        try:
            for repeat in range(
                    array_start, array_end + 1, array_stride):
                if array_specifier:
                    stdout_f = stdout + ".{0}".format(repeat)
                    stderr_f = stderr + ".{0}".format(repeat)
                with open(stdout_f, 'w') as stdout_file:
                    with open(stderr_f, 'w') as stderr_file:
                        if array_specifier:
                            os.environ['NONE_TASK_ID'] = str(repeat)
                            os.environ['NONE_TASK_FIRST'] = str(array_start)
                            os.environ['NONE_TASK_LAST'] = str(array_end)
                            os.environ['NONE_TASK_STEPSIZE'] = str(
                                array_stride)
                            os.environ['FSLSUB_ARRAYCOUNT_VAR'] = ''
                        logger.info(
                            "executing: (" + str(repeat) + ")" + str(cmd))

                        result = sp.run(
                                    cmd,
                                    stdout=stdout_file,
                                    stderr=stderr_file,
                                    universal_newlines=True)
                        if result.returncode != 0:
                            raise BadSubmission(
                                stderr_file.seek(0).readlines())
        except BadSubmission:
            raise
        except Exception as e:
            raise BadSubmission from e

    return pid


def example_conf():
    '''Returns a string containing the example configuration for this
    cluster plugin.'''

    here = os.path.realpath(__file__)
    with open(os.path.join(here, 'fsl_sub_None.yml')) as e_conf_f:
        e_conf = e_conf_f.readlines()

    return '\n'.join(e_conf)