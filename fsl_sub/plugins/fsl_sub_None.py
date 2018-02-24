# fsl_sub plugin for running directly on this computer
import logging
import os
import shlex
import subprocess as sp

from fsl_sub.exceptions import BadSubmission


def qtest():
    '''Command that confirms method is available'''
    return True


def queue_exists(qname, qtest=qtest()):
    '''Command that confirms a queue is available'''
    return True


def submit(
        command,
        job_name,
        queue,
        threads=1,
        array_task=False,
        logdir=os.getcwd(),
        **kwargs):
    '''Submits the job'''
    logger = logging.getLogger('__name__')
    pid = os.getpid()
    logfile_base = os.path.join(logdir, job_name)
    stdout = "{0}.{1}{2}".format(logfile_base, 'o', pid)
    stderr = "{0}.{1}{2}".format(logfile_base, 'e', pid)
    command_lines = []
    if array_task is True:
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
        logger.info("executing: " + " ".join(command))

    for (line, cmd) in enumerate(command_lines):
        if isinstance(cmd, str):
            cmd = shlex.split(cmd)
        if len(command_lines) > 1:
            stdout_f = "{0}.{1}".format(
                stdout, line + 1)
            stderr_f = "{0}.{1}".format(
                stderr, line + 1)
            logger.info("executing: " + str(line + 1))
        else:
            (stdout_f, stderr_f) = (stdout, stderr)
        try:
            with open(stdout_f, 'w') as stdout_file:
                with open(stderr_f, 'w') as stderr_file:
                    result = sp.run(
                                cmd,
                                stdout=stdout_file,
                                stderr=stderr_file,
                                shell=True)
                    if result.returncode != 0:
                        raise BadSubmission(
                            stderr_file.seek(0).readlines())
        except BadSubmission:
            raise
        except Exception as e:
            raise BadSubmission from e

    return pid
