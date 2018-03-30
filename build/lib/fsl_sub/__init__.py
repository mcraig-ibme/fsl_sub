#!/usr/bin/env fslpython
import copy
import getpass
import logging
import os
import shlex
import socket
import warnings
from operator import itemgetter
from math import ceil

from .exceptions import (
    LoadModuleError,
    BadConfiguration,
)

from fsl_sub.config import (
    read_config,
    method_config,
    coprocessor_config,
)

from fsl_sub.coprocessors import (
    coproc_load_module,
)

from fsl_sub.utils import (
    load_plugins,
    affirmative,
    check_command,
    check_command_file,
    control_threads
)
VERSION = '2.0'
PLUGINS = load_plugins()


def fsl_sub_warnings_formatter(
        message, category, filename, lineno, file=None, line=None):
    return str(message)


warnings.formatwarning = fsl_sub_warnings_formatter
warnings.simplefilter('always', UserWarning)


def parallel_envs(queues):
    '''Return the list of configured parallel environments
    in the supplied queue definition dict'''
    ll_envs = []
    for q in queues.values():
        try:
            ll_envs.extend(q['parallel_envs'])
        except KeyError:
            pass
    return list(set(ll_envs))


def submit(
    command,
    name=None,
    threads=1,
    queue=None,
    jobhold=None,
    array_task=False,
    parallel_hold=None,
    parallel_limit=None,
    parallel_stride=1,
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
    logdir=os.getcwd(),
    coprocessor=None,
    coprocessor_toolkit=None,
    coprocessor_class=None,
    coprocessor_class_strict=False,
    coprocessor_multi="1",
    usescript=False,
    architecture=None,
):
    '''Submit job(s) to a queue'''
    logger = logging.getLogger('__name__')
    global PLUGINS

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
        qfind = PLUGINS[grid_module].qfind
        queue_exists = PLUGINS[grid_module].queue_exists
        BadSubmission = PLUGINS[grid_module].BadSubmission
    except AttributeError as e:
        raise BadConfiguration(
            "Failed to load plugin " + grid_module
        )

    config['qtest'] = qfind()
    if config['qtest'] is None:
        config['method'] == 'None'
        warnings.warn(
            'Warning: fsl_sub configured for {} but {}'
            ' software not found.'.format(config['method'])
        )

    if method_config['mail_support'] is True:
        if mail_on is None:
            try:
                mail_on = method_config['mail_mode']
            except KeyError:
                warnings.warn(
                    "Mail not configured but enabled in configuration for " +
                    config['method'])
        else:
            for m_opt in method_config['mail_modes'].split(','):
                if m_opt not in method_config['mail_modes']:
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
        if validate_command:
            task_numbers = check_command_file(command)
        if name is None:
            name = os.path.basename(command)
    logger.info(
        "METHOD={0} : TYPE={1} : args={2}".format(
            config['method'],
            job_type,
            " ".join(command)
        ))
    task_name = os.path.basename(command)

    m_config = method_config(config['method'])

    split_on_ram = m_config['map_ram'] and ramsplit

    if (split_on_ram and
            parallel_env is None and
            'large_job_split_pe' in m_config):
        parallel_env = m_config['large_job_split_pe']

    if queue is None:
        (queue, slots_required) = getq_and_slots(
                job_time=jobtime,
                job_ram=jobram,
                job_threads=threads,
                queues=config['queues'],
                coprocessor=coprocessor,
                ll_env=parallel_env
                )

    if queue is None:
        raise BadSubmission("Unable to find a queue with these parameters")

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

    if coprocessor:
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
        array_slots=task_numbers,
        parallel_hold=parallel_hold,
        parallel_limit=parallel_limit,
        parallel_stride=parallel_stride,
        parallel_env=parallel_env,
        jobram=jobram,
        jobtime=jobtime,
        resources=resources,
        ramsplit=split_on_ram,
        prority=priority,
        mail_on=mail_on,
        mailto=mailto,
        logdir=logdir,
        coprocessor=coprocessor,
        coprocessor_toolkit=coprocessor_toolkit,
        coprocessor_class=coprocessor_class,
        coprocessor_class_strict=coprocessor_class_strict,
        coprocessor_multi=coprocessor_multi,
        usescript=usescript,
        architecture=architecture)

    return job_id


def split_ram_by_slots(jram, jslots):
    return int(ceil(jram / jslots))


def getq_and_slots(
        queues, job_time=None, job_ram=None,
        job_threads=1, coprocessor=None,
        ll_env=None):
    '''Calculate which queue to run the job on
    Still needs job splitting across slots'''
    logger = logging.getLogger('__name__')

    if job_time is None:
        queue_list = [
            q for q in queues if 'default' in q and q['default']]
    else:
        queue_list = copy.deepcopy(queues)
    # Filter on coprocessor availability
    if coprocessor:
        queue_list = [
            q for q in queue_list if 'copros' in q and
            coprocessor in q['copros']]
    # Filter on parallel environment availability
    if ll_env:
        queue_list = [
            q for q in queue_list if 'parallel_envs' in q and
            ll_env in q['parallel_envs']
        ]

    # For each queue calculate how many slots would be necessary...
    def calc_slots(job_ram, slot_size, job_threads):
        # No ram specified
        if job_ram is None:
            return max(1, job_threads)
        else:
            return max(int(ceil(job_ram / slot_size)), job_threads)

    for queue in queue_list:
        queue_list[queue]['slots_required'] = calc_slots(
            job_ram, queue['slot_size'], job_threads)

    sql = sorted(
        queue_list,
        key=itemgetter('group', 'priority', 'slots_required'))

    ql = [
        q['name'] for q in sql if q['time'] >= job_time and
        q['memory'] >= job_ram and
        q['max_slots'] <= job_threads]

    logger.info(
        "Estimated RAM was {0} GBm, runtime was {1} minutes.\n".format(
            job_ram, job_time
        ))
    if coprocessor:
        logger.info("Co-processor {} was requested".format(coprocessor))
    logger.info(
        "Appropriate queue is {}".format(ql[0]))
    return (ql[0], ql[0]['slots_required'])
