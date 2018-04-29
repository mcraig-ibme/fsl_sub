# fsl_sub plugin for:
#  * Slurm
import logging
import os
import shlex
import subprocess as sp
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
    human_to_ram,
)


def qtest():
    '''Command that confirms method is available'''
    return qconf_cmd()


def qconf_cmd():
    '''Command that queries queue configuration'''
    qconf = which('scontrol')
    if qconf is None:
        raise BadSubmission("Cannot find Slurm software")
    return qconf


def qstat_cmd():
    '''Command that queries queue state'''
    qstat = which('squeue')
    if qstat is None:
        raise BadSubmission("Cannot find Slurm software")
    return qstat


def qsub_cmd():
    '''Command that submits a job'''
    qsub = which('sbatch')
    if qsub is None:
        raise BadSubmission("Cannot find Slurm software")
    return qsub


def queue_exists(qname, qtest=None):
    '''Does qname exist'''
    if qtest is None:
        qtest = which('showq')
    try:
        sp.run(
            [qtest, '-p', qname],
            stderr=sp.PIPE,
            check=True, universal_newlines=True)
    except sp.CalledProcessError:
        return False
    return True


def slurm_option(opt):
    return "#SBATCH " + opt


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
        mailon=None,
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
    '''Submits the job to a SLURM cluster
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
    requeueable - may a job be requeued if a node fails
    resources - list of resource request strings
    ramsplit - break tasks into multiple slots to meet RAM constraints
    priority - job priority (0-1023)
    mailon - mail user on 'a'bort or reschedule, 'b'egin, 'e'nd,
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

    logger = logging.getLogger("__name__")

    if command is None:
        raise BadSubmission(
            "Must provide command line or array task file name")

    mconf = defaultdict(lambda: False, method_config('SLURM'))
    qsub = qsub_cmd()
    if logdir is None:
        logdir = os.getcwd()
    if isinstance(resources, str):
        resources = [resources, ]

    gres = []
    if usescript:
        command_args = [command]
    else:
        # Check Parallel Environment is available
        if parallel_env:
            command_args.extend(
                ['--cpus-per-task', threads, ])
        if mconf['copy_environment']:
            command_args.append('='.join(
                ('--export', 'ALL', )
            ))
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

                try:
                    copro_class = available_classes[
                                        coprocessor_class][
                                            'resource']
                except KeyError:
                    raise BadSubmission("Unrecognised coprocessor class")
                if (not coprocessor_class_strict and
                        cpconf['include_more_capable'] and
                        cpconf['slurm_constraints']):
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
                    command_args.append(
                        '='.join(
                            ('--constraint', '"{}"'.format(copro_class)))
                            )
                    gres.append(
                        ":".join(
                            (cpconf['resource'], str(coprocessor_multi))))
            else:
                gres.append(
                    ":".join(
                        (
                            cpconf['resource'],
                            copro_class,
                            str(coprocessor_multi))
                    )
                )

        if binding == 'linear':
            command_args.append('='.join(
                ('--cpu-bind', mconf['affinity_control'], )))

        # Job priorities can only be set by admins

        if resources:
            gres.append(','.join(resources))

        command_args.append('='.join(
            ('--gres', ",".join(gres))
        ))

        if logdir == '/dev/null':
            command_args.extend(
                ['-o', logdir, '-e', logdir]
            )
        else:
            logs = []
            for l in ['o', 'e']:
                if array_task:
                    logtemplate = '{0}.{1}%A.%a'
                else:
                    logtemplate = '{0}.{1}%j'
                logs[l] = os.path.join(
                    logdir,
                    logtemplate.format(
                            job_name.replace(' ', '_'),
                            l)
                        )
            command_args.extend(
                ['-o', logs['out'], '-e', logs['err']]
            )

        if array_task is not None and array_hold:
            # SLURM doesn't support array holds, convert to a basic hold
            jobhold = array_hold

        if jobhold:
            parents = jobhold.split(',')
            dependencies = ':'.join(parents)
            command_args.append(
                "=".join(
                    ('--dependancy', 'afterok:' + dependencies)
                )
            )

        if array_task is not None:
            # ntasks%array_limit
            if mconf['array_limits'] and array_limit:
                array_limit_modifier = "%{}".format(array_limit)
            else:
                array_limit_modifier = ""

        if jobram:
            # Minimum memory required per allocated CPU.
            # Default units are megabytes unless the SchedulerParameters
            #  configuration parameter includes the "default_gbytes"
            # option for gigabytes. Default value is DefMemPerCPU and
            # the maximum value is MaxMemPerCPU (see exception below).
            # If configured, both parameters can be seen using the
            # scontrol show config command. Note that if the job's
            # --mem-per-cpu value exceeds the configured MaxMemPerCPU,
            # then the user's limit will be treated as a memory limit
            # per task; --mem-per-cpu will be reduced to a value no
            # larger than MaxMemPerCPU; --cpus-per-task will be set
            # and the value of --cpus-per-task multiplied by the new
            # --mem-per-cpu value will equal the original --mem-per-cpu
            # value specified by the user. This parameter would
            # generally be used if individual processors are allocated
            # to jobs (SelectType=select/cons_res). If resources are
            # allocated by the core, socket or whole nodes; the number
            # of CPUs allocated to a job may be higher than the task
            # count and the value of --mem-per-cpu should be adjusted
            # accordingly. Also see --mem. --mem and --mem-per-cpu are
            # mutually exclusive.

            if ramsplit:
                jobram = split_ram_by_slots(jobram, threads)
                # mem-per-cpu if dividing RAM up, otherwise mem
            ram_units = read_config['ram_units']

            # RAM is specified in megabytes
            try:
                mem_in_mb = human_to_ram(
                            jobram,
                            units=ram_units,
                            output="M")
            except ValueError:
                raise BadConfiguration("ram_units not one of P, T, G, M, K")
            command_args.append(
                '='.join((
                    '--mem-per-cpu',
                    mem_in_mb
                ))
            )

        if mconf['mail_support']:
            if mailto:
                command_args.extend(['-M', mailto, ])
                if not mailon:
                    mailon = mconf['mail_mode']
                if mailon not in mconf['mail_modes']:
                    raise BadSubmission("Unrecognised mail mode")
                command_args.append(
                    '='.join((
                        '--mail-type',
                        ','.join(mconf['mail_mode'][mailon]),
                    ))
                )
        command_args.append(
            '='.join((
                '--job-name', job_name, ))
        )

        command_args.extend(
            ['-p', queue, ])

        command_args.append('--parsable')

        if requeueable:
            command_args.append('--requeue')

        if array_task:
            # Submit array task file
            with open(command, 'r') as cmd_f:
                array_slots = len(cmd_f.readlines())
            command_args.append(
                "=".join((
                    '--array', "1-{0}:{1}{2}".format(
                        array_slots * array_stride,
                        array_stride,
                        array_limit_modifier))))
        else:
            # Submit single script/binary
            if isinstance(command, str):
                command = shlex.split(command)

    logger.info("slurm_args: " + " ".join(
        [str(a) for a in command_args if a != qsub]))

    if usescript:
        command_args.insert(0, qsub)
        logger.info(
            "executing cluster script")
        result = sp.run(
            command_args,
            stdout=sp.PIPE, stderr=sp.PIPE
        )
    else:
        scriptcontents = '''#!/bin/bash
    {0}
    '''.format('\n'.join([slurm_option(x) for x in command_args]))
        logger.info("Passing command script to STDIN")
        if array_task:
            logger.info("executing array task")
            scriptcontents += '''
the_command=$(sed -n -e "${{SLURM_ARRAY_TASK_ID}}p" {0})

exec /bin/bash -c "$the_command"
'''.format(command)
            logger.debug(scriptcontents)
            result = sp.run(
                qsub,
                input=scriptcontents,
                universal_newlines=True,
                stdout=sp.PIPE, stderr=sp.PIPE
            )
        else:
            logger.info("executing single task")
            scriptcontents += '''
{}
'''.format(' '.join([shlex.quote(x) for x in command]))
            logger.debug(scriptcontents)
            result = sp.run(
                command_args,
                stdout=sp.PIPE,
                stderr=sp.PIPE)
    if result.returncode != 0:
        raise BadSubmission(result.stderr)
    job_words = result.stdout.split(';')
    try:
        job_id = int(job_words[0].split('.')[0])
    except ValueError:
        raise GridOutputError("Grid output was " + result.stdout)
    return job_id
