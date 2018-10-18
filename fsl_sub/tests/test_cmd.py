#!/usr/bin/python
import argparse
import getpass
import io
import os
import socket
import unittest
import yaml
import fsl_sub.cmd
from unittest.mock import patch

YAML_CONF = '''---
method: sge
ram_units: G
modulecmd: /usr/bin/modulecmd
method_opts:
    sge:
        queues: True
        large_job_split_pe: shmem
        copy_environment: True
        affinity_type: linear
        affinity_control: threads
        mail_support: True
        mail_modes:
            b:
                - b
            e:
                - e
            a:
                - a
            f:
                - a
                - e
                - b
            n:
                - n
        mail_mode: a
        map_ram: True
        ram_resources:
            - m_mem_free
            - h_vmem
        job_priorities: True
        min_priority: -1023
        max_priority: 0
        array_holds: True
        array_limit: True
        architecture: False
        job_resources: True
coproc_opts:
  cuda:
    resource: gpu
    classes: True
    class_resource: gputype
    class_types:
      K:
        resource: k80
        doc: Kepler. ECC, double- or single-precision workloads
        capability: 2
      P:
        resource: p100
        doc: >
          Pascal. ECC, double-, single- and half-precision
          workloads
        capability: 3
    default_class: K
    include_more_capable: True
    uses_modules: True
    module_parent: cuda
queues:
  gpu.q:
    time: 18000
    max_size: 250
    slot_size: 64
    max_slots: 20
    copros:
      cuda:
        max_quantity: 4
        classes:
          - K
          - P
          - V
    map_ram: true
    parallel_envs:
      - shmem
    priority: 1
    group: 0
    default: true
  a.qa,a.qb,a.qc:
    time: 1440
    max_size: 160
    slot_size: 4
    max_slots: 16
    map_ram: true
    parallel_envs:
      - shmem
    priority: 3
    group: 1
    default: true
  a.qa,a.qc:
    time: 1440
    max_size: 240
    slot_size: 16
    max_slots: 16
    map_ram: true
    parallel_envs:
      - shmem
    priority: 2
    group: 1
    default: true
  a.qc:
    time: 1440
    max_size: 368
    slot_size: 16
    max_slots: 24
    map_ram: true
    parallel_envs:
      - shmem
    priority: 1
    group: 1
    default: true
  b.qa,b.qb,b.qc:
    time: 10080
    max_size: 160
    slot_size: 4
    max_slots: 16
    map_ram: true
    parallel_envs:
      - shmem
    priority: 3
    group: 2
  b.qa,b.qc:
    time: 10080
    max_size: 240
    slot_size: 16
    max_slots: 16
    map_ram: true
    parallel_envs:
      - shmem
    priority: 2
    group: 2
  b.qc:
    time: 10080
    max_size: 368
    slot_size: 16
    max_slots: 24
    map_ram: true
    parallel_envs:
      - shmem
    priority: 1
    group: 2
  t.q:
    time: 10080
    max_size: 368
    slot_size: 16
    max_slots: 24
    map_ram: true
    parallel_envs:
      - specialpe
    priority: 1
    group: 2

default_queues:
  - a.qa,a,qb,a.qc
  - a.qa,a.qc
  - a.qc

'''
USER_EMAIL = "{username}@{hostname}".format(
                    username=getpass.getuser(),
                    hostname=socket.gethostname()
                )


@patch(
    'fsl_sub.shell_modules.read_config',
    autospec=True,
    return_value=yaml.load(YAML_CONF))
@patch(
    'fsl_sub.cmd.read_config',
    autospec=True,
    return_value=yaml.load(YAML_CONF))
@patch(
    'fsl_sub.config.read_config',
    autospec=True,
    return_value=yaml.load(YAML_CONF))
@patch(
    'fsl_sub.cmd.submit',
    autospec=True,
    return_value=123)
@patch(
    'fsl_sub.cmd.get_modules', autospec=True,
    return_value=['7.5', '8.0', ])
@patch(
    'fsl_sub.coprocessors.get_modules',
    autospec=True, return_value=['7.5', '8.0', ])
class TestMain(unittest.TestCase):
    def test_noramsplit(self, *args):
        fsl_sub.cmd.main(['--noramsplit', '1', '2', ])

        args[2].assert_called_with(
            ['1', '2', ],
            architecture=None,
            array_hold=None,
            array_limit=None,
            array_specifier=None,
            array_task=False,
            coprocessor=None,
            coprocessor_toolkit=None,
            coprocessor_class=None,
            coprocessor_class_strict=False,
            coprocessor_multi=1,
            name=None,
            parallel_env=None,
            queue=None,
            threads=1,
            jobhold=None,
            jobram=None,
            jobtime=None,
            logdir=None,
            mail_on=None,
            mailto=USER_EMAIL,
            priority=None,
            ramsplit=False,
            requeueable=True,
            resources=None,
            usescript=False,
            validate_command=True,
            native_holds=False,
            as_tuple=False
        )

    def test_parallelenv(self, *args):
        fsl_sub.cmd.main(['--parallelenv', 'shmem,2', '1', '2', ])

        args[2].assert_called_with(
            ['1', '2', ],
            architecture=None,
            array_hold=None,
            array_limit=None,
            array_specifier=None,
            array_task=False,
            coprocessor=None,
            coprocessor_toolkit=None,
            coprocessor_class=None,
            coprocessor_class_strict=False,
            coprocessor_multi=1,
            name=None,
            parallel_env='shmem',
            queue=None,
            threads=2,
            jobhold=None,
            jobram=None,
            jobtime=None,
            logdir=None,
            mail_on=None,
            mailto=USER_EMAIL,
            priority=None,
            ramsplit=True,
            requeueable=True,
            resources=None,
            usescript=False,
            validate_command=True,
            native_holds=False,
            as_tuple=False
        )
        args[2].reset_mock()
        fsl_sub.cmd.main(['-s', 'shmem,2', '1', '2', ])

        args[2].assert_called_with(
            ['1', '2', ],
            architecture=None,
            array_hold=None,
            array_limit=None,
            array_specifier=None,
            array_task=False,
            coprocessor=None,
            coprocessor_toolkit=None,
            coprocessor_class=None,
            coprocessor_class_strict=False,
            coprocessor_multi=1,
            name=None,
            parallel_env='shmem',
            queue=None,
            threads=2,
            jobhold=None,
            jobram=None,
            jobtime=None,
            logdir=None,
            mail_on=None,
            mailto=USER_EMAIL,
            priority=None,
            ramsplit=True,
            requeueable=True,
            resources=None,
            usescript=False,
            validate_command=True,
            native_holds=False,
            as_tuple=False
        )

    def test_mailoptions(self, *args):
        fsl_sub.cmd.main(['--mailoptions', 'n', '1', '2', ])

        args[2].assert_called_with(
            ['1', '2', ],
            architecture=None,
            array_hold=None,
            array_limit=None,
            array_specifier=None,
            array_task=False,
            coprocessor=None,
            coprocessor_toolkit=None,
            coprocessor_class=None,
            coprocessor_class_strict=False,
            coprocessor_multi=1,
            name=None,
            parallel_env=None,
            queue=None,
            threads=1,
            jobhold=None,
            jobram=None,
            jobtime=None,
            logdir=None,
            mail_on='n',
            mailto=USER_EMAIL,
            priority=None,
            ramsplit=True,
            requeueable=True,
            resources=None,
            usescript=False,
            validate_command=True,
            native_holds=False,
            as_tuple=False
        )

    def test_mailto(self, *args):
        fsl_sub.cmd.main(['--mailto', 'user@test.com', '1', '2', ])

        args[2].assert_called_with(
            ['1', '2', ],
            architecture=None,
            array_hold=None,
            array_limit=None,
            array_specifier=None,
            array_task=False,
            coprocessor=None,
            coprocessor_toolkit=None,
            coprocessor_class=None,
            coprocessor_class_strict=False,
            coprocessor_multi=1,
            name=None,
            parallel_env=None,
            queue=None,
            threads=1,
            jobhold=None,
            jobram=None,
            jobtime=None,
            logdir=None,
            mail_on=None,
            mailto='user@test.com',
            priority=None,
            ramsplit=True,
            requeueable=True,
            resources=None,
            usescript=False,
            validate_command=True,
            native_holds=False,
            as_tuple=False
        )
        args[2].reset_mock()
        fsl_sub.cmd.main(['-M', 'user@test.com', '1', '2', ])

        args[2].assert_called_with(
            ['1', '2', ],
            architecture=None,
            array_hold=None,
            array_limit=None,
            array_specifier=None,
            array_task=False,
            coprocessor=None,
            coprocessor_toolkit=None,
            coprocessor_class=None,
            coprocessor_class_strict=False,
            coprocessor_multi=1,
            name=None,
            parallel_env=None,
            queue=None,
            threads=1,
            jobhold=None,
            jobram=None,
            jobtime=None,
            logdir=None,
            mail_on=None,
            mailto='user@test.com',
            priority=None,
            ramsplit=True,
            requeueable=True,
            resources=None,
            usescript=False,
            validate_command=True,
            native_holds=False,
            as_tuple=False
        )

    def test_array_task(self, *args):
        fsl_sub.cmd.main(['--array_task', 'taskfile', ])

        args[2].assert_called_with(
            'taskfile',
            architecture=None,
            array_hold=None,
            array_limit=None,
            array_specifier=None,
            array_task=True,
            coprocessor=None,
            coprocessor_toolkit=None,
            coprocessor_class=None,
            coprocessor_class_strict=False,
            coprocessor_multi=1,
            name=None,
            parallel_env=None,
            queue=None,
            threads=1,
            jobhold=None,
            jobram=None,
            jobtime=None,
            logdir=None,
            mail_on=None,
            mailto=USER_EMAIL,
            priority=None,
            ramsplit=True,
            requeueable=True,
            resources=None,
            usescript=False,
            validate_command=True,
            native_holds=False,
            as_tuple=False
        )
        args[2].reset_mock()
        fsl_sub.cmd.main(['-t', 'taskfile', ])

        args[2].assert_called_with(
            'taskfile',
            architecture=None,
            array_hold=None,
            array_limit=None,
            array_specifier=None,
            array_task=True,
            coprocessor=None,
            coprocessor_toolkit=None,
            coprocessor_class=None,
            coprocessor_class_strict=False,
            coprocessor_multi=1,
            name=None,
            parallel_env=None,
            queue=None,
            threads=1,
            jobhold=None,
            jobram=None,
            jobtime=None,
            logdir=None,
            mail_on=None,
            mailto=USER_EMAIL,
            priority=None,
            ramsplit=True,
            requeueable=True,
            resources=None,
            usescript=False,
            validate_command=True,
            native_holds=False,
            as_tuple=False
        )

    def test_array_limit(self, *args):
        fsl_sub.cmd.main(
            ['--array_task', 'commandfile', '--array_limit', '2', ])

        args[2].assert_called_with(
            'commandfile',
            architecture=None,
            array_hold=None,
            array_limit=2,
            array_specifier=None,
            array_task=True,
            coprocessor=None,
            coprocessor_toolkit=None,
            coprocessor_class=None,
            coprocessor_class_strict=False,
            coprocessor_multi=1,
            name=None,
            parallel_env=None,
            queue=None,
            threads=1,
            jobhold=None,
            jobram=None,
            jobtime=None,
            logdir=None,
            mail_on=None,
            mailto=USER_EMAIL,
            priority=None,
            ramsplit=True,
            requeueable=True,
            resources=None,
            usescript=False,
            validate_command=True,
            native_holds=False,
            as_tuple=False
        )
        args[2].reset_mock()
        fsl_sub.cmd.main(['-x', '2', '--array_task', 'commandfile', ])

        args[2].assert_called_with(
            'commandfile',
            architecture=None,
            array_hold=None,
            array_limit=2,
            array_specifier=None,
            array_task=True,
            coprocessor=None,
            coprocessor_toolkit=None,
            coprocessor_class=None,
            coprocessor_class_strict=False,
            coprocessor_multi=1,
            name=None,
            parallel_env=None,
            queue=None,
            threads=1,
            jobhold=None,
            jobram=None,
            jobtime=None,
            logdir=None,
            mail_on=None,
            mailto=USER_EMAIL,
            priority=None,
            ramsplit=True,
            requeueable=True,
            resources=None,
            usescript=False,
            validate_command=True,
            native_holds=False,
            as_tuple=False
        )

    def test_array_hold(self, *args):
        fsl_sub.cmd.main(
            ['--array_task', 'commandfile', '--array_hold', '20002', ])

        args[2].assert_called_with(
            'commandfile',
            architecture=None,
            array_hold=['20002'],
            array_limit=None,
            array_specifier=None,
            array_task=True,
            coprocessor=None,
            coprocessor_toolkit=None,
            coprocessor_class=None,
            coprocessor_class_strict=False,
            coprocessor_multi=1,
            name=None,
            parallel_env=None,
            queue=None,
            threads=1,
            jobhold=None,
            jobram=None,
            jobtime=None,
            logdir=None,
            mail_on=None,
            mailto=USER_EMAIL,
            priority=None,
            ramsplit=True,
            requeueable=True,
            resources=None,
            usescript=False,
            validate_command=True,
            native_holds=False,
            as_tuple=False
        )

    def test_array_hold_native(self, *args):
        fsl_sub.cmd.main(
            [
                '--array_task', 'commandfile',
                '--array_hold', '20002:aa', '--native_holds', ])

        args[2].assert_called_with(
            'commandfile',
            architecture=None,
            array_hold='20002:aa',
            array_limit=None,
            array_specifier=None,
            array_task=True,
            coprocessor=None,
            coprocessor_toolkit=None,
            coprocessor_class=None,
            coprocessor_class_strict=False,
            coprocessor_multi=1,
            name=None,
            parallel_env=None,
            queue=None,
            threads=1,
            jobhold=None,
            jobram=None,
            jobtime=None,
            logdir=None,
            mail_on=None,
            mailto=USER_EMAIL,
            priority=None,
            ramsplit=True,
            requeueable=True,
            resources=None,
            usescript=False,
            validate_command=True,
            native_holds=True,
            as_tuple=False
        )

    def test_job_hold(self, *args):
        fsl_sub.cmd.main(
            ['--jobhold', '20002', 'commandfile'])

        args[2].assert_called_with(
            ['commandfile'],
            architecture=None,
            array_hold=None,
            array_limit=None,
            array_specifier=None,
            array_task=False,
            coprocessor=None,
            coprocessor_toolkit=None,
            coprocessor_class=None,
            coprocessor_class_strict=False,
            coprocessor_multi=1,
            name=None,
            parallel_env=None,
            queue=None,
            threads=1,
            jobhold=['20002'],
            jobram=None,
            jobtime=None,
            logdir=None,
            mail_on=None,
            mailto=USER_EMAIL,
            priority=None,
            ramsplit=True,
            requeueable=True,
            resources=None,
            usescript=False,
            validate_command=True,
            native_holds=False,
            as_tuple=False
        )

    def test_job_hold_native(self, *args):
        fsl_sub.cmd.main(
            ['--jobhold', '20002:aa', '--native_holds', 'commandfile', ])

        args[2].assert_called_with(
            ['commandfile'],
            architecture=None,
            array_hold=None,
            array_limit=None,
            array_specifier=None,
            array_task=False,
            coprocessor=None,
            coprocessor_toolkit=None,
            coprocessor_class=None,
            coprocessor_class_strict=False,
            coprocessor_multi=1,
            name=None,
            parallel_env=None,
            queue=None,
            threads=1,
            jobhold='20002:aa',
            jobram=None,
            jobtime=None,
            logdir=None,
            mail_on=None,
            mailto=USER_EMAIL,
            priority=None,
            ramsplit=True,
            requeueable=True,
            resources=None,
            usescript=False,
            validate_command=True,
            native_holds=True,
            as_tuple=False
        )

    def test_array_native(self, *args):
        fsl_sub.cmd.main(
            ['--array_native', '1-4:2', 'command', ])

        args[2].assert_called_with(
            ['command'],
            architecture=None,
            array_hold=None,
            array_limit=None,
            array_specifier='1-4:2',
            array_task=True,
            coprocessor=None,
            coprocessor_toolkit=None,
            coprocessor_class=None,
            coprocessor_class_strict=False,
            coprocessor_multi=1,
            name=None,
            parallel_env=None,
            queue=None,
            threads=1,
            jobhold=None,
            jobram=None,
            jobtime=None,
            logdir=None,
            mail_on=None,
            mailto=USER_EMAIL,
            priority=None,
            ramsplit=True,
            requeueable=True,
            resources=None,
            usescript=False,
            validate_command=True,
            native_holds=False,
            as_tuple=False
        )

    def test_coprocessor(self, *args):
        fsl_sub.cmd.main(['--coprocessor', 'cuda', '1', '2', ])

        args[2].assert_called_with(
            ['1', '2', ],
            architecture=None,
            array_hold=None,
            array_limit=None,
            array_specifier=None,
            array_task=False,
            coprocessor='cuda',
            coprocessor_toolkit=None,
            coprocessor_class=None,
            coprocessor_class_strict=False,
            coprocessor_multi=1,
            name=None,
            parallel_env=None,
            queue=None,
            threads=1,
            jobhold=None,
            jobram=None,
            jobtime=None,
            logdir=None,
            mail_on=None,
            mailto=USER_EMAIL,
            priority=None,
            ramsplit=True,
            requeueable=True,
            resources=None,
            usescript=False,
            validate_command=True,
            native_holds=False,
            as_tuple=False
        )

    def test_coprocessor_toolkit(self, *args):
        fsl_sub.cmd.main([
            '--coprocessor', 'cuda',
            '--coprocessor_toolkit', '7.5',
            '1', '2', ])

        args[2].assert_called_with(
            ['1', '2', ],
            architecture=None,
            array_hold=None,
            array_limit=None,
            array_specifier=None,
            array_task=False,
            coprocessor='cuda',
            coprocessor_toolkit='7.5',
            coprocessor_class=None,
            coprocessor_class_strict=False,
            coprocessor_multi=1,
            name=None,
            parallel_env=None,
            queue=None,
            threads=1,
            jobhold=None,
            jobram=None,
            jobtime=None,
            logdir=None,
            mail_on=None,
            mailto=USER_EMAIL,
            priority=None,
            ramsplit=True,
            requeueable=True,
            resources=None,
            usescript=False,
            validate_command=True,
            native_holds=False,
            as_tuple=False
        )

    def test_coprocessor_class(self, *args):
        fsl_sub.cmd.main([
            '--coprocessor', 'cuda',
            '--coprocessor_class', 'K',
            '1', '2', ])

        args[2].assert_called_with(
            ['1', '2', ],
            architecture=None,
            array_hold=None,
            array_limit=None,
            array_specifier=None,
            array_task=False,
            coprocessor='cuda',
            coprocessor_toolkit=None,
            coprocessor_class='K',
            coprocessor_class_strict=False,
            coprocessor_multi=1,
            name=None,
            parallel_env=None,
            queue=None,
            threads=1,
            jobhold=None,
            jobram=None,
            jobtime=None,
            logdir=None,
            mail_on=None,
            mailto=USER_EMAIL,
            priority=None,
            ramsplit=True,
            requeueable=True,
            resources=None,
            usescript=False,
            validate_command=True,
            native_holds=False,
            as_tuple=False
        )

    def test_coprocessor_class_strict(self, *args):
        fsl_sub.cmd.main([
            '--coprocessor', 'cuda',
            '--coprocessor_class', 'K',
            '--coprocessor_class_strict',
            '1', '2', ])

        args[2].assert_called_with(
            ['1', '2', ],
            architecture=None,
            array_hold=None,
            array_limit=None,
            array_specifier=None,
            array_task=False,
            coprocessor='cuda',
            coprocessor_toolkit=None,
            coprocessor_class='K',
            coprocessor_class_strict=True,
            coprocessor_multi=1,
            name=None,
            parallel_env=None,
            queue=None,
            threads=1,
            jobhold=None,
            jobram=None,
            jobtime=None,
            logdir=None,
            mail_on=None,
            mailto=USER_EMAIL,
            priority=None,
            ramsplit=True,
            requeueable=True,
            resources=None,
            usescript=False,
            validate_command=True,
            native_holds=False,
            as_tuple=False
        )

    def test_coprocessor_multi(self, *args):
        fsl_sub.cmd.main([
            '--coprocessor', 'cuda',
            '--coprocessor_multi', '2',
            '1', '2', ])

        args[2].assert_called_with(
            ['1', '2', ],
            architecture=None,
            array_hold=None,
            array_limit=None,
            array_specifier=None,
            array_task=False,
            coprocessor='cuda',
            coprocessor_toolkit=None,
            coprocessor_class=None,
            coprocessor_class_strict=False,
            coprocessor_multi='2',
            name=None,
            parallel_env=None,
            queue=None,
            threads=1,
            jobhold=None,
            jobram=None,
            jobtime=None,
            logdir=None,
            mail_on=None,
            mailto=USER_EMAIL,
            priority=None,
            ramsplit=True,
            requeueable=True,
            resources=None,
            usescript=False,
            validate_command=True,
            native_holds=False,
            as_tuple=False
        )


class ErrorRaisingArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        raise ValueError(message)  # reraise an error


class TestExampleConf(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.parser = fsl_sub.cmd.example_config_parser(
            parser_class=ErrorRaisingArgumentParser)
        none_config = os.path.join(
            os.path.dirname(__file__), '..',
            'plugins', 'fsl_sub_none.yml')
        with open(none_config, 'r') as yfile:
            cls.exp_conf = yfile.read()

    def test_example_config_parser_blank(self):
        self.assertRaises(
            ValueError,
            self.parser.parse_args,
            ['', ]
        )

    def test_example_config_parser_unknown_plugin(self):
        self.assertRaises(
            ValueError,
            self.parser.parse_args,
            ['NoCluster', ]
        )

    def test_example_config_parser_known_plugin(self):
        self.assertEqual(
            self.parser.parse_args(['None']).plugin,
            'None'
        )

    @unittest.mock.patch('fsl_sub.cmd.sys.stdout', new_callable=io.StringIO)
    def test_example_config(self, mock_stdout):
        fsl_sub.cmd.example_config(['None', ])
        self.assertEqual(
            mock_stdout.getvalue(),
            self.exp_conf + '\n'
        )