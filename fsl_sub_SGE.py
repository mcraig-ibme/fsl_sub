# fsl_sub plugin for:
#  * Sun Grid Engine
#  * Son of Grid Engine
#  * Open Grid Scheduler
#  * Univa Grid Engine
import logging
import subprocess as sp
import tempfile
import xml.etree.ElementTree as ET
from shutil import which


class BadSubmission(Exception):
    pass


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
        method_config, options, 
        copro_config=None, qsub=qsub()):
    '''Submits the job to the cluster'''

    logger = logging.getLogger("__name__")

    command_args = [qsub, ]
    if not options['usescript']:
        # Check Parallel Environment is available
        if options['pe']:
            threads = options['pe']['slots']
            check_pe(options['qtest'], options['pe']['name'])

            command_args.extend(
                ['-pr', options['pe']['name'], threads, '-w', 'e'])
        else:
            threads = 1

        if method_config['copy_environment']:
            command_args.append('-V')

        if method_config['affinity_type']:
            if method_config['affinity_control'] == 'threads':
                affinity_spec = ':'.join(
                    (method_config['affinity_type'], threads))
            elif method_config['affinity_control'] == 'slots':
                affinity_spec = ':'.join(
                    (method_config['affinity_type'], 'slots'))
            else:
                raise BadSubmission(
                    ("Unrecognised affinity_control setting " +
                     method_config['affinity_control']))
            command_args.extend(['-binding', affinity_spec])

        if (method_config['job_priorities'] and
                options['priority'] is not None):
            command_args.extend(['-p', options['priority']])

        if options['resource']:
            command_args.extend(
                ['-l', ','.join(options['resource'])])

        if options['logdir']:
            command_args.extend(
                ['-o', options['logdir'], '-e', options['logdir']]
            )

        if options['jobhold']:
            command_args.extend(
                ['-hold_jid', options['jobhold']]
            )

        if method_config['parallel_hold'] and options['parallel_hold']:
            command_args.extend(
                ['-hold_jid_ad', options['parallel_hold'], ]
            )

        if method_config['parallel_limit'] and options['parallel_limit']:
            command_args.extend(
                ['-tc', options['parallel_limit'], ]
            )

        if options['jobram']:
            command_args.extend(
                ['-l', ','.join(
                    ['{0}={1}{2}'.format(
                        a, options['jobram'], method_config['ram_units']) for
                        a in method_config['ram_resources']])]
            )

        if method_config['mail_support']:
            if options['mailoptions']:
                command_args.extend(['-m', options['mailoptions']])
            if options['mailto']:
                command_args.extend(['-M', options['mailto']])

        command_args.extend(['-N', options['jobname'], ])
        command_args.extend(
            ['-cwd', '-q', options['queue']['name']])

        if copro_config:
            # Setup the coprocessor
            if copro_config['classes']:
                available_classes = copro_config['class_types']
                if (options['coprocessor_class_strict'] or
                        not copro_config['include_more_capable']):
                    try:
                        copro_class = available_classes[
                                            options['coprocessor_class']][
                                                'resource']
                    except KeyError:
                        raise BadSubmission("Unrecognised coprocessor class")
                else:
                    copro_capability = available_classes[
                                            options['coprocessor_class']][
                                                'capability'
                                            ]
                    copro_class = ','.join(
                        [a['resource'] for a in
                            copro_config['class_types'] if
                            a['capability'] > copro_capability])

                command_args.extend(
                    ['-l',
                     '='.join(
                         (copro_config['class_resource'], copro_class))]
                         )
            command_args.extend(
                ['-l',
                 '='.join(
                     (copro_config['resource'], options['coprocessor_multi']))]
                    )

        if options['args']:
            # Submit single script/binary
            command_args.extend(
                ['-shell', 'n',
                 '-b', 'y', 'r', 'y', ])
            command_args.extend(options['args'])

        elif options['paralleltask']:
            # Submit parallel task file
            command_args.extend(
                ['-t', "1-{0}:{1}".format(
                    options['task_numbers'] * options['parallel_stride'],
                    options['parallel_stride'])])
        else:
            raise BadSubmission("We shouldn't get here!")

    logger.info("sge_args: " + " ".join(
        [a for a in command_args if a != qsub]))

    if options['args']:
        logger.info("executing: " + " ".join(
            [
                " ".join(command_args),
                " ".join(options['args'])]
        ))
    elif options['paralleltask']:
        logger.info("control file: " + options['paralleltask'].name)
        script = tempfile.NamedTemporaryFile(delete=False)
        scriptcontents = '''
#!/bin/sh

#$ -S /bin/sh

the_command=$(sed -n -e "${{SGE_TASK_ID}}p" {0})

exec /bin/sh -c "$the_command"
'''.format(options['paralleltask'].name)
        logger.debug(scriptcontents)
        script.close()
        logger.debug(script.name)
        command_args.append(script.name)

    result = sp.run(command_args, stdout=sp.PIPE, stderr=sp.PIPE)
    if result.returncode != 0:
        raise BadSubmission(result.stderr)

    (_, _, job_id) = result.stdout.split(' ')
    job_id = job_id.split('.')[0]

    return job_id
