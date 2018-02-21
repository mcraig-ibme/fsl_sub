# fsl_sub plugin for running directly on this computer
import logging
import os
import subprocess as sp

from ..exceptions import BadSubmission


def qtest():
    '''Command that confirms method is available'''
    return True


def queue_exists(qname, qtest=qtest()):
    '''Command that confirms a queue is available'''
    return True


def submit(
        command,
        job_name,
        threads,
        queue,
        array_task=False,
        array_slots=1,
        logdir=os.getcwd(),
        **kwargs):
    '''Submits the job'''
    logger = logging.getLogger('__name__')

    pid = os.getpid()
    logfile_base = os.path.join(logdir, job_name)
    stdout = "{0}.{1}{2}".format('o', logfile_base, pid)
    stderr = "{0}.{1}{2}".format('e', logfile_base, pid)
    command_lines = []
    if array_task is True:
        try:
            with open(command, 'r') as ll_tasks:
                command_lines.extend(ll_tasks.readlines())
        except Exception as e:
            raise BadSubmission(
                "Unable to read array task file " +
                command) from e
        logger.info(
            "Running commands in: " + command)
        for line in range(0, array_slots):
            logger.info(
                "{line}: {command}".format(
                    line=line,
                    command=command_lines[line]
                )
            )
    else:
        command_lines.append(command)
        logger.info("executing: " + " ".join(command_lines))

    for (line, cmd) in enumerate(command_lines):
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
                                command,
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
