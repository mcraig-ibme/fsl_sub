# fsl_sub plugin for:
#  * Sun Grid Engine
#  * Son of Grid Engine
#  * Open Grid Scheduler
#  * Univa Grid Engine
import logging
import os
import shlex
import subprocess as sp
import tempfile
import xml.etree.ElementTree as ET
from collections import defaultdict
from shutil import which

from fsl_sub.exceptions import (
    BadSubmission,
    BadConfiguration,
    GridOutputError,
)
from fsl_sub.config import (
    method_config,
    coprocessor_config,
    read_config,
)
from fsl_sub.utils import (
    split_ram_by_slots,
)


def qtest():
    '''Command that confirms method is available'''
    return qconf_cmd()


def qconf_cmd():
    '''Command that queries queue configuration'''
    qconf = which('qconf')
    if qconf is None:
        raise BadSubmission("Cannot find Grid Engine software")
    return qconf


def qstat_cmd():
    '''Command that queries queue state'''
    qstat = which('qstat')
    if qstat is None:
        raise BadSubmission("Cannot find Grid Engine software")
    return qstat


def qsub_cmd():
    '''Command that submits a job'''
    qsub = which('qsub')
    if qsub is None:
        raise BadSubmission("Cannot find Grid Engine software")
    return qsub


def queue_exists(qname, qtest=None):
    '''Does qname exist'''
    if qtest is None:
        qtest = qconf_cmd()
    try:
        sp.run(
            [qtest, '-sq', qname],
            stderr=sp.PIPE,
            check=True, universal_newlines=True)
    except FileNotFoundError:
        raise BadSubmission(
            "Grid Engine software may not be correctly installed")
    except sp.CalledProcessError:
        return False
    return True


def check_pe(pe_name, queue, qconf=None, qstat=None):
    if qconf is None:
        qconf = qconf_cmd()
    if qstat is None:
        qstat = qstat_cmd()
    # Check for configured PE of pe_name
    cmd = sp.run([qconf, '-sp', pe_name])
    if cmd.returncode != 0:
        raise BadSubmission(pe_name + " is not a valid PE")

    # Check for availability of PE
    cmd = sp.run(
        [qstat, '-g', 'c', '-pe', pe_name, '-xml'],
        stdout=sp.PIPE, stderr=sp.PIPE)
    if (cmd.returncode != 0 or (
            cmd.stderr is not None and
            "error: no such parallel environment" in cmd.stderr)):
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
        queue,
        threads=1,
        array_task=False,
        jobhold=None,
        array_hold=None,
        array_limit=None,
        array_stride=1,
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
        requeueable=True
        ):
    '''Submits the job to a Grid Engine cluster
    Requires:

    command - string or list containing command to run
                or the file name of the array task file
    job_name - Symbolic name for task
    queue - Queue to submit to

    Optional:
    array_task - is the command is an array task (defaults to False)
    jobhold - id(s) of jobs to hold for (string or list)
    array_hold - complex hold string
    array_limit - limit concurrently scheduled array
            tasks to specified number
    array_stride - each subtask should be X slots on,
            defaults to 1
    parallelenv - parallel environment name
    jobram - RAM required by job (total of all threads)
    jobtime - time (in minutes for task)
    requeueable - job may be requeued on node failure
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
    '''

    logger = logging.getLogger('fsl_sub.plugins')

    if command is None:
        raise BadSubmission(
            "Must provide command line or array task file name")

    mconf = defaultdict(lambda: False, method_config('SGE'))
    qsub = qsub_cmd()
    command_args = [qsub, ]

    if isinstance(resources, str):
        resources = [resources, ]

    if usescript:
        command_args.append(command)
    else:
        # Check Parallel Environment is available
        if parallel_env:
            check_pe(qtest(), parallel_env)

            command_args.extend(
                ['-pe', parallel_env, threads, '-w', 'e'])
        if mconf['copy_environment']:
            command_args.append('-V')

        binding = mconf['affinity_type']

        if coprocessor is not None:
            # Setup the coprocessor
            cpconf = coprocessor_config(coprocessor)
            if cpconf['no_binding']:
                binding = None
            if cpconf['classes']:
                available_classes = cpconf['class_types']
                if coprocessor_class is None:
                    coprocessor_class = cpconf['default_class']
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
                    base_list = [
                        a for a in cpconf['class_types'].keys() if
                        cpconf['class_types'][a]['capability'] >=
                        copro_capability]
                    copro_class = '|'.join(
                        [
                            cpconf['class_types'][a]['resource'] for a in
                            sorted(
                                    base_list,
                                    key=lambda x:
                                    cpconf['class_types'][x]['capability'])
                        ]
                    )

                command_args.extend(
                    ['-l',
                     '='.join(
                          (cpconf['class_resource'], copro_class))]
                         )
            command_args.extend(
                ['-l',
                 '='.join(
                      (cpconf['resource'], str(coprocessor_multi)))]
                    )

        if binding is not None:
            if mconf['affinity_control'] == 'threads':
                affinity_spec = ':'.join(
                    (mconf['affinity_type'], str(threads)))
            elif mconf['affinity_control'] == 'slots':
                affinity_spec = ':'.join(
                    (mconf['affinity_type'], 'slots'))
            else:
                raise BadConfiguration(
                    ("Unrecognised affinity_control setting " +
                     mconf['affinity_control']))
            command_args.extend(['-binding', affinity_spec])

        if (mconf['job_priorities'] and
                priority is not None):
            if 'min_priority' in mconf:
                priority = max(mconf['min_priority'], priority)
                priority = min(mconf['max_priority'], priority)
            command_args.extend(['-p', priority, ])

        if resources:
            command_args.extend(
                ['-l', ','.join(resources), ])

        if logdir is not None:
            command_args.extend(
                ['-o', logdir, '-e', logdir]
            )

        if jobhold:
            command_args.extend(
                ['-hold_jid', jobhold, ]
            )

        if array_task is not None:
            if mconf['array_holds'] and array_hold:
                command_args.extend(
                    ['-hold_jid_ad', array_hold, ]
                )
            elif array_hold:
                command_args.extend(
                    ['-hold_jid', array_hold, ]
                )
            if mconf['array_limits'] and array_limit:
                command_args.extend(
                    ['-tc', array_limit, ]
                )

        if jobram:
            ram_units = read_config()['ram_units']
            if ramsplit:
                jobram = split_ram_by_slots(jobram, threads)

            command_args.extend(
                ['-l', ','.join(
                    ['{0}={1}{2}'.format(
                        a, jobram, ram_units) for
                        a in mconf['ram_resources']])]
            )

        if mconf['mail_support']:
            if mailto:
                command_args.extend(['-M', mailto, ])
                if not mail_on:
                    mail_on = mconf['mail_mode']
                if mail_on not in mconf['mail_modes']:
                    raise BadSubmission("Unrecognised mail mode")
                command_args.extend(
                    [
                        '-m',
                        ','.join(mconf['mail_modes'][mail_on])
                        ])

        command_args.extend(['-N', job_name, ])
        command_args.extend(
            ['-cwd', '-q', queue, ])

        if requeueable:
            command_args.extend(
                ['-r', 'y', ]
            )
        if array_task:
            # Submit array task file
            with open(command, 'r') as cmd_f:
                array_slots = len(cmd_f.readlines())
            command_args.extend(
                ['-t', "1-{0}:{1}".format(
                    array_slots * array_stride,
                    array_stride)])
        else:
            # Submit single script/binary
            command_args.extend(
                ['-shell', 'n',
                 '-b', 'y', ])
            if isinstance(command, str):
                command = shlex.split(command)
            command_args.extend(command)

    logger.info("sge_args: " + " ".join(
        [str(a) for a in command_args if a != qsub]))

    if array_task:
        logger.info("executing array task")
        scriptcontents = '''#!/bin/bash

#$ -S /bin/bash

the_command=$(sed -n -e "${{SGE_TASK_ID}}p" {0})

exec /bin/bash -c "$the_command"
'''.format(command)
        logger.debug(scriptcontents)
        with tempfile.NamedTemporaryFile(delete=False) as script:
            script.write(scriptcontents)
        logger.debug(script.name)
        command_args.append(script.name)
    else:
        if usescript:
            logger.info("executing cluster script")
        else:
            logger.info("executing single task")
            logger.info(" ".join([str(a) for a in command_args]))
    logger.debug(type(command_args))
    logger.debug(command_args)
    result = sp.run(command_args, stdout=sp.PIPE, stderr=sp.PIPE)
    if array_task:
        os.remove(script.name)
    if result.returncode != 0:
        raise BadSubmission(result.stderr)
    job_words = result.stdout.split(' ')
    try:
        job_id = int(job_words[2].split('.')[0])
    except ValueError:
        raise GridOutputError("Grid output was " + result.stdout)
    return job_id
