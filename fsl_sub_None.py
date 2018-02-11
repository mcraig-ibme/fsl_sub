# fsl_sub plugin for running directly on this computer
import logging
import os
import subprocess as sp


class BadSubmission(Exception):
    pass


def qtest():
    '''Command that confirms method is available'''
    return True


def queue_exists(qname, qtest=qtest()):
    '''Command that confirms a queue is available'''
    return True


def submit(
        method_config, options,
        copro_config=None, qsub=None):
    '''Submits the job'''
    logger = logging.getLogger('__name__')

    pid = os.getpid()
    logfile_base = os.path.join(options['logdir'], options['jobname'])
    logfiles = "{path}.{type}{pid}"
    stdout = logfiles.format(
        {'type': 'o',
         'path': logfile_base,
         'pid': pid, }
    )
    stderr = logfiles.format(
        {'type': 'e',
         'path': logfile_base,
         'pid': pid, }
    )
    commands = []
    if options['args']:
        commands.append(options['args'])
    elif options['paralleltask']:
        try:
            with open(options['paralleltask'].name, 'r') as ll_tasks:
                commands.extend(ll_tasks.read().splitlines())
        except Exception as e:
            raise BadSubmission from e
    else:
        raise BadSubmission("We shouldn't get here")

    if options['args']:
        logger.warning("executing: " + " ".join(options['args']))
    elif options['paralleltask']:
        logger.warning(
            "Running commands in: " + options['paralleltask'].name)

    for (line, command) in enumerate(commands):
        if len(commands) > 1:
            stdout_f = "{0}.{1}".format(
                stdout, line)
            stderr_f = "{0}.{1}".format(
                stderr, line)
            logger.warning("executing: " + str(line))
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
