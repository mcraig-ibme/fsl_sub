# fsl_sub plugin for:
#  * Sun Grid Engine
#  * Son of Grid Engine
#  * Open Grid Scheduler
#  * Univa Grid Engine
import logging
import os
import subprocess as sp
import tempfile
import xml.etree.ElementTree as ET
from shutil import which

from ..exceptions import (
    BadSubmission,
)
from ..config import (
    method_config,
    copro_conf,
)
from ..utils import (
    split_ram_by_slots,
)


class Config(object):
    '''Configuration for this runner'''
    def __init__(config_file, config_name='sge'):
        '''Read configuration from config file'''


def qtest():
    '''Command that confirms method is available'''
    return which('qconf')


def qconf():
    '''Command that queries queue configuration'''
    qconf = which('qconf')
    if qconf is None:
        raise BadSubmission("Cannot find Grid Engine software")
    return qconf


def qstat():
    '''Command that queries queue state'''
    qstat = which('qstat')
    if qstat is None:
        raise BadSubmission("Cannot find Grid Engine software")
    return qstat


def qsub():
    '''Command that submits a job'''
    qsub = which('qsub')
    if qsub is None:
        raise BadSubmission("Cannot find Grid Engine software")
    return qsub


def queue_exists(qname, qtest=qtest()):
    try:
        sp.run(
            [qtest, '-sq', qname],
            stderr=sp.PIPE,
            check=True, universal_newlines=True)
    except sp.CalledProcessError:
        return False
    return True


def check_pe(pe_name, queue, qconf=qconf(), qstat=qstat()):
    # Check for configured PE of pe_name
    cmd = sp.run([qconf, '-sp', pe_name])
    if cmd.returncode != 0:
        raise BadSubmission(pe_name + " is not a valid PE")

    # Check for availability of PE
    cmd = sp.run(
        [qstat, '-g', 'c', '-pe', pe_name, '-xml'],
        stdout=sp.PIPE, stderr=sp.PIPE)
    if (cmd.returncode != 0 or
            "error: no such parallel environment" in cmd.stderr):
        raise BadSubmission(
            "No instances of {} configured".format(pe_name))

    # Check that PE is available on requested queue
    queue_defs = ET.fromstring(cmd.stdout)
    if queue not in [b.text for b in queue_defs.iter('name')]:
        raise BadSubmission(
            "PE {} is not configured on {}".format(pe_name, queue)
        )


def submit(
        command,
        job_name,
        threads,
        queue,
        array_task=False,
        array_slots=1,
        jobhold=None,
        parallel_hold=None,
        parallel_limit=None,
        parallel_stride=1,
        parallel_env=None,
        jobram=None,
        jobtime=None,
        resources=None,
        ramsplit=False,
        priority=None,
        mail_on=None,
        mailto=None,
        logdir=os.getcwd(),
        coprocessor=None,
        coprocessor_toolkit=None,
        coprocessor_class=None,
        coprocessor_class_strict=False,
        coprocessor_multi=None,
        usescript=False,
        architecture=None
        ):
    '''Submits the job to a Grid Engine cluster
    Requires:

    command - string or list containing command to run
                or the file name of the array task file
    job_name - Symbolic name for task
    queue - Queue to submit to

    Optional:
    array_task - is the command is an array task (defaults to False)
    array_slots - total number of slots in the array task
    jobhold - id(s) of jobs to hold for (string or list)
    parallel_hold - complex hold string
    parallel_limit - limit concurrently scheduled parallel
            tasks to specified number
    parallel_stride - each subtask should be X slots on,
            defaults to 1
    parallelenv - parallel environment name
    jobram - RAM required by job (total of all threads)
    jobtime - time (in minutes for task)
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

    '''

    logger = logging.getLogger("__name__")

    if command is None:
        raise BadSubmission(
            "Must provide command line or parallel task file name")

    mconf = method_config('sge')

    command_args = [qsub, ]
    if not usescript:
        # Check Parallel Environment is available
        if parallel_env:
            check_pe(qtest(), parallel_env)

            command_args.extend(
                ['-pe', parallel_env, threads, '-w', 'e'])

        if mconf['copy_environment']:
            command_args.append('-V')

        if mconf['affinity_type']:
            if mconf['affinity_control'] == 'threads':
                affinity_spec = ':'.join(
                    (mconf['affinity_type'], threads))
            elif mconf['affinity_control'] == 'slots':
                affinity_spec = ':'.join(
                    (mconf['affinity_type'], 'slots'))
            else:
                raise BadSubmission(
                    ("Unrecognised affinity_control setting " +
                     mconf['affinity_control']))
            command_args.extend(['-binding', affinity_spec])

        if (mconf['job_priorities'] and
                priority is not None):
            command_args.extend(['-p', priority, ])

        if resources:
            command_args.extend(
                ['-l', ','.join(resources), ])

        if logdir:
            command_args.extend(
                ['-o', logdir, '-e', logdir]
            )

        if jobhold:
            command_args.extend(
                ['-hold_jid', jobhold, ]
            )

        if mconf['parallel_hold'] and parallel_hold:
            command_args.extend(
                ['-hold_jid_ad', parallel_hold, ]
            )

        if mconf['parallel_limit'] and parallel_limit:
            command_args.extend(
                ['-tc', parallel_limit, ]
            )

        if jobram:
            if ramsplit:
                jobram = split_ram_by_slots(jobram, threads)

            command_args.extend(
                ['-l', ','.join(
                    ['{0}={1}{2}'.format(
                        a, jobram, mconf['ram_units']) for
                        a in mconf['ram_resources']])]
            )

        if mconf['mail_support']:
            if mailto:
                command_args.extend(['-M', mailto, ])
                if mail_on:
                    command_args.extend(['-m', mail_on, ])

        command_args.extend(['-N', job_name, ])
        command_args.extend(
            ['-cwd', '-q', queue, ])

        if coprocessor is not None:
            # Setup the coprocessor
            cpconf = copro_conf(coprocessor)
            if cpconf['classes']:
                available_classes = cpconf['class_types']
                if (coprocessor_class_strict or
                        not cpconf['include_more_capable']):
                    try:
                        copro_class = available_classes[
                                            coprocessor_class][
                                                'resource']
                    except KeyError:
                        raise BadSubmission("Unrecognised coprocessor class")
                else:
                    copro_capability = available_classes[
                                            coprocessor_class][
                                                'capability'
                                            ]
                    copro_class = ','.join(
                        [a['resource'] for a in
                         cpconf['class_types'] if
                            a['capability'] > copro_capability])

                command_args.extend(
                    ['-l',
                     '='.join(
                          cpconf['class_resource'], copro_class)]
                         )
            command_args.extend(
                ['-l',
                 '='.join(
                      cpconf['resource'], coprocessor_multi)]
                    )

        if array_task:
            # Submit parallel task file
            command_args.extend(
                ['-t', "1-{0}:{1}".format(
                    array_slots * parallel_stride,
                    parallel_stride)])
        else:
            # Submit single script/binary
            command_args.extend(
                ['-shell', 'n',
                 '-b', 'y', 'r', 'y', ])
            command_args.extend(command)

    logger.info("sge_args: " + " ".join(
        [a for a in command_args if a != qsub]))

    if array_task:
        logger.info("control file: " + command)
        scriptcontents = '''
#!/bin/sh

#$ -S /bin/sh

the_command=$(sed -n -e "${{SGE_TASK_ID}}p" {0})

exec /bin/sh -c "$the_command"
'''.format(command)
        logger.debug(scriptcontents)
        with tempfile.NamedTemporaryFile(delete=False) as script:
            script.write(scriptcontents)
        logger.debug(script.name)
        command_args.append(script.name)
        logger.info(
            "executing parallel task: " +
            " ".join(command_args))
    else:
        logger.info("executing: " + " ".join(command_args))

    result = sp.run(command_args, stdout=sp.PIPE, stderr=sp.PIPE)
    os.remove(script.name)
    if result.returncode != 0:
        raise BadSubmission(result.stderr)

    (_, _, job_id) = result.stdout.split(' ')
    job_id = job_id.split('.')[0]

    return job_id
