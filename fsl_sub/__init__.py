#!/usr/bin/env fslpython
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
)
from fsl_sub.utils import (
    load_plugins,
    affirmative,
    check_command,
    check_command_file,
    control_threads,
)

VERSION = '2.0'


def fsl_sub_warnings_formatter(
        message, category, filename, lineno, file=None, line=None):
    return str(message)


warnings.formatwarning = fsl_sub_warnings_formatter
warnings.simplefilter('always', UserWarning)


def submit(
    command,
    name=None,
    threads=1,
    queue=None,
    jobhold=None,
    array_task=False,
    array_hold=None,
    array_limit=None,
    array_stride=1,
    parallel_env=None,
    jobram=None,
    jobtime=None,
    resources=None,
    ramsplit=True,
    priority=None,
    validate_command=True,
    mail_on=None,
    mailto="{username}@{hostname}.".format(
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
    requeueable=True
):
    '''Submit job(s) to a queue'''
    logger = logging.getLogger(__name__)
    PLUGINS = load_plugins()

    config = read_config()

    try:
        already_run = os.environ['FSLSUBALREADYRUN']
    except KeyError:
        already_run = 'false'
    os.environ['FSLSUBALREADYRUN'] = 'true'

    if config['method'] != 'None':
        if affirmative(already_run):
            config['method'] == 'None'
            warnings.warn(
                'Warning: job on queue attempted to submit parallel jobs -'
                'running jobs serially instead.'
            )
    grid_module = 'fsl_sub_' + config['method']
    if grid_module not in PLUGINS:
        raise BadConfiguration(
            "{} not a supported method".format(config['method']))

    try:
        queue_submit = PLUGINS[grid_module].submit
        qtest = PLUGINS[grid_module].qtest
        queue_exists = PLUGINS[grid_module].queue_exists
        BadSubmission = PLUGINS[grid_module].BadSubmission
    except AttributeError as e:
        raise BadConfiguration(
            "Failed to load plugin " + grid_module
        )

    config['qtest'] = qtest()
    if config['qtest'] is None:
        config['method'] == 'None'
        warnings.warn(
            'Warning: fsl_sub configured for {} but {}'
            ' software not found.'.format(config['method'])
        )

    mconfig = method_config(config['method'])
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

    if mconfig['mail_support'] is True:
        if mail_on is None:
            try:
                mail_on = mconfig['mail_mode']
            except KeyError:
                warnings.warn(
                    "Mail not configured but enabled in configuration for " +
                    config['method'])
        else:
            # Mail modes is a dictionary
            if mail_on not in mconfig['mail_modes']:
                raise BadSubmission(
                        "Unrecognised mail mode " + mail_on)

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
        if validate_command:
            check_command(command[0])
    else:
        job_type = 'array'
        if (
                array_hold is not None or
                array_limit is not None):
            raise BadSubmission(
                "Array controls not applicable to non-array tasks")
        if validate_command:
            try:
                check_command_file(command)
            except CommandError as e:
                raise BadSubmission(
                    "Array task definition file fault: " + str(e)
                )
        if name is None:
            name = os.path.basename(command)
    logger.info(
        "METHOD={0} : TYPE={1} : args={2}".format(
            config['method'],
            job_type,
            " ".join(command)
        ))
    if name is None:
        if isinstance(command, list):
            c_name = command[0]
        else:
            c_name = command.shlex.split()[0]
        if '/' in c_name:
            c_name = os.path.basename(c_name)
        task_name = c_name

    split_on_ram = mconfig['map_ram'] and ramsplit

    if (split_on_ram and
            parallel_env is None and
            'large_job_split_pe' in mconfig):
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
        if queue_details is None:
            raise BadSubmission("Unable to find a queue with these parameters")
        else:
            (queue, slots_required) = queue_details

    if not queue_exists(queue):
        raise BadSubmission("Unrecognised queue " + queue)

    threads = max(slots_required, threads)

    control_threads(config['thread_control'], threads)

    if threads == 1 and parallel_env is not None:
        parallel_env = None
    if threads > 1 and parallel_env is None:
        raise BadSubmission(
                "Job requires {} slots but no parallel envrionment "
                "available or requested".format(threads))
    if threads > 1 and config['thread_ram_divide'] and not split_on_ram:
        split_on_ram = True

    if coprocessor:
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
        if coprocessor_toolkit != -1:
            try:
                coproc_load_module(
                    coprocessor_config(coprocessor),
                    coprocessor_toolkit)
            except LoadModuleError:
                raise BadSubmission(
                    "Unable to load requested coprocessor toolkit"
                )
    job_id = queue_submit(
        command,
        job_name=task_name,
        threads=threads,
        queue=queue,
        jobhold=jobhold,
        array_task=array_task,
        array_hold=array_hold,
        array_limit=array_limit,
        array_stride=array_stride,
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
        requeueable=requeueable)

    return job_id


def getq_and_slots(
        queues, job_time=0, job_ram=0,
        job_threads=1, coprocessor=None,
        ll_env=None):
    '''Calculate which queue to run the job on
    Still needs job splitting across slots'''
    logger = logging.getLogger(__name__)

    if job_ram is None:
        job_ram = 0
    
    queue_list = list(queues.keys())
    
    if not queue_list:
        return None
    
    # Filter on coprocessor availability
    if coprocessor is not None:
        queue_list = [
            q for q in queue_list if 'copros' in queues[q] and
            coprocessor in queues[q]['copros']]
    else:
        queue_list = [
            q for q in queue_list if 'copros' not in queues[q]
        ]
    if not queue_list:
        return None

    # Filter on parallel environment availability
    if ll_env is not None:
        queue_list = [
            q for q in queue_list if 'parallel_envs' in queues[q] and
            ll_env in queues[q]['parallel_envs']
        ]
    if not queue_list:
        return None

    # If no job time was specified then find the default queues
    # (if defined)
    if job_time is None or job_time == 0:
        d_queues = [
            q for q in queue_list if 'default' in queues[q]
        ]
        if d_queues:
            queue_list = d_queues

        job_time = 0

    # For each queue calculate how many slots would be necessary...
    def calc_slots(job_ram, slot_size, job_threads):
        # No ram specified
        logger.debug(
            "Calc slots based on JR:SS:JT - {0}:{1}:{2}".format(
                job_ram, slot_size, job_threads
            ))
        if job_ram == 0 or job_ram is None:
            return max(1, job_threads)
        else:
            return max(int(ceil(job_ram / slot_size)), job_threads)

    slots = {}
    for q in queue_list:
        slots[q] = calc_slots(
            job_ram, queues[q]['slot_size'], job_threads)

    queue_list.sort(key=lambda x: queues[x]['priority'], reverse=True)
    queue_list.sort(key=lambda x: (queues[x]['group'], slots[x]))

    ql = [q for q in queue_list if queues[q]['time'] >= job_time and
          queues[q]['max_size'] >= job_ram and
          queues[q]['max_slots'] >= job_threads]
    if not ql:
        return None

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
