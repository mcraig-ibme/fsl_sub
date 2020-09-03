#!/usr/bin/python
import argparse
import getpass
import io
import os
import socket
import sys
import unittest
import yaml
import fsl_sub.cmd
from unittest.mock import patch

YAML_CONF = '''---
method: sge
ram_units: G
modulecmd: /usr/bin/modulecmd
thread_control:
  - OMP_NUM_THREADS
  - MKL_NUM_THREADS
  - MKL_DOMAIN_NUM_THREADS
  - OPENBLAS_NUM_THREADS
  - GOTO_NUM_THREADS
preserve_modules: True
export_vars: []
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
        projects: False
        script_conf: True
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
    no_binding: True
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
YAML_CONF_PROJECTS = '''---
method: sge
ram_units: G
modulecmd: /usr/bin/modulecmd
thread_control:
  - OMP_NUM_THREADS
  - MKL_NUM_THREADS
  - MKL_DOMAIN_NUM_THREADS
  - OPENBLAS_NUM_THREADS
  - GOTO_NUM_THREADS
preserve_modules: True
export_vars: []
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
        projects: True
        script_conf: True
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
    no_binding: True
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


class FakePlugin(object):
    def submit(self):
        pass

    def qtest(self):
        pass

    def queue_exists(self):
        pass

    def plugin_version(self):
        return '1.2.0'

    def already_queued(self):
        return False


class TestMisc(unittest.TestCase):
    def test_titlize_key(self):
        self.assertEqual(
            'A Word',
            fsl_sub.utils.titlize_key(
                'a_word'
            )
        )

    def test_blank_none(self):
        self.assertEqual(
            fsl_sub.utils.blank_none(1),
            '1'
        )
        self.assertEqual(
            fsl_sub.utils.blank_none(None),
            ''
        )
        self.assertEqual(
            fsl_sub.utils.blank_none('A'),
            'A'
        )
        self.assertEqual(
            fsl_sub.utils.blank_none(['a', 'b']),
            "['a', 'b']"
        )


@patch(
    'fsl_sub.cmd.load_plugins',
    autospec=True,
    return_value={'fsl_sub_plugin_sge': FakePlugin()}
)
@patch(
    'fsl_sub.shell_modules.read_config',
    autospec=True,
    return_value=yaml.safe_load(YAML_CONF))
@patch(
    'fsl_sub.cmd.read_config',
    autospec=True,
    return_value=yaml.safe_load(YAML_CONF))
@patch(
    'fsl_sub.config.read_config',
    autospec=True,
    return_value=yaml.safe_load(YAML_CONF))
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
        with io.StringIO() as text_trap:
            sys.stdout = text_trap

            fsl_sub.cmd.main(['--noramsplit', '1', '2', ])

            sys.stdout = sys.__stdout__

            self.assertEqual(
                text_trap.getvalue(),
                '123\n'
            )

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
            as_tuple=False,
            project=None
        )

    def test_parallelenv(self, *args):
        with io.StringIO() as text_trap:
            sys.stdout = text_trap

            fsl_sub.cmd.main(['--parallelenv', 'shmem,2', '1', '2', ])

            sys.stdout = sys.__stdout__

            self.assertEqual(
                text_trap.getvalue(),
                '123\n'
            )

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
            as_tuple=False,
            project=None
        )
        args[2].reset_mock()
        with io.StringIO() as text_trap:
            sys.stdout = text_trap

            fsl_sub.cmd.main(['-s', 'shmem,2', '1', '2', ])

            sys.stdout = sys.__stdout__

            self.assertEqual(
                text_trap.getvalue(),
                '123\n'
            )
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
            as_tuple=False,
            project=None
        )

    def test_mailoptions(self, *args):
        with io.StringIO() as text_trap:
            sys.stdout = text_trap

            fsl_sub.cmd.main(['--mailoptions', 'n', '1', '2', ])

            sys.stdout = sys.__stdout__

            self.assertEqual(
                text_trap.getvalue(),
                '123\n'
            )
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
            as_tuple=False,
            project=None
        )

    def test_mailto(self, *args):
        with io.StringIO() as text_trap:
            sys.stdout = text_trap

            fsl_sub.cmd.main(['--mailto', 'user@test.com', '1', '2', ])

            sys.stdout = sys.__stdout__

            self.assertEqual(
                text_trap.getvalue(),
                '123\n'
            )
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
            as_tuple=False,
            project=None
        )
        args[2].reset_mock()
        with io.StringIO() as text_trap:
            sys.stdout = text_trap

            fsl_sub.cmd.main(['-M', 'user@test.com', '1', '2', ])

            sys.stdout = sys.__stdout__

            self.assertEqual(
                text_trap.getvalue(),
                '123\n'
            )

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
            as_tuple=False,
            project=None
        )

    def test_array_task(self, *args):
        with io.StringIO() as text_trap:
            sys.stdout = text_trap

            fsl_sub.cmd.main(['--array_task', 'taskfile', ])

            sys.stdout = sys.__stdout__

            self.assertEqual(
                text_trap.getvalue(),
                '123\n'
            )
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
            as_tuple=False,
            project=None
        )
        args[2].reset_mock()
        with io.StringIO() as text_trap:
            sys.stdout = text_trap

            fsl_sub.cmd.main(['-t', 'taskfile', ])

            sys.stdout = sys.__stdout__

            self.assertEqual(
                text_trap.getvalue(),
                '123\n'
            )
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
            as_tuple=False,
            project=None
        )

    def test_array_limit(self, *args):
        with io.StringIO() as text_trap:
            sys.stdout = text_trap

            fsl_sub.cmd.main(
                ['--array_task', 'commandfile', '--array_limit', '2', ])

            sys.stdout = sys.__stdout__

            self.assertEqual(
                text_trap.getvalue(),
                '123\n'
            )
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
            as_tuple=False,
            project=None
        )
        args[2].reset_mock()
        with io.StringIO() as text_trap:
            sys.stdout = text_trap

            fsl_sub.cmd.main(['-x', '2', '--array_task', 'commandfile', ])

            sys.stdout = sys.__stdout__

            self.assertEqual(
                text_trap.getvalue(),
                '123\n'
            )
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
            as_tuple=False,
            project=None
        )

    def test_array_hold(self, *args):
        with io.StringIO() as text_trap:
            sys.stdout = text_trap

            fsl_sub.cmd.main(
                ['--array_task', 'commandfile', '--array_hold', '20002', ])
            sys.stdout = sys.__stdout__

            self.assertEqual(
                text_trap.getvalue(),
                '123\n'
            )
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
            as_tuple=False,
            project=None
        )

    def test_array_hold_native(self, *args):
        with io.StringIO() as text_trap:
            sys.stdout = text_trap

            fsl_sub.cmd.main(
                [
                    '--array_task', 'commandfile',
                    '--array_hold', '20002:aa', '--native_holds', ])
            sys.stdout = sys.__stdout__

            self.assertEqual(
                text_trap.getvalue(),
                '123\n'
            )
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
            as_tuple=False,
            project=None
        )

    def test_job_hold(self, *args):
        with io.StringIO() as text_trap:
            sys.stdout = text_trap

            fsl_sub.cmd.main(
                ['--jobhold', '20002', 'commandfile'])

            sys.stdout = sys.__stdout__

            self.assertEqual(
                text_trap.getvalue(),
                '123\n'
            )
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
            as_tuple=False,
            project=None
        )

    def test_job_hold_native(self, *args):
        with io.StringIO() as text_trap:
            sys.stdout = text_trap

            fsl_sub.cmd.main(
                ['--jobhold', '20002:aa', '--native_holds', 'commandfile', ])
            sys.stdout = sys.__stdout__

            self.assertEqual(
                text_trap.getvalue(),
                '123\n'
            )

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
            as_tuple=False,
            project=None
        )

    def test_array_native(self, *args):
        with io.StringIO() as text_trap:
            sys.stdout = text_trap

            fsl_sub.cmd.main(
                ['--array_native', '1-4:2', 'command', ])

            sys.stdout = sys.__stdout__

            self.assertEqual(
                text_trap.getvalue(),
                '123\n'
            )

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
            as_tuple=False,
            project=None
        )

    def test_coprocessor(self, *args):
        with io.StringIO() as text_trap:
            sys.stdout = text_trap

            fsl_sub.cmd.main(['--coprocessor', 'cuda', '1', '2', ])

            sys.stdout = sys.__stdout__

            self.assertEqual(
                text_trap.getvalue(),
                '123\n'
            )

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
            as_tuple=False,
            project=None
        )

    def test_coprocessor_toolkit(self, *args):
        with io.StringIO() as text_trap:
            sys.stdout = text_trap

            fsl_sub.cmd.main([
                '--coprocessor', 'cuda',
                '--coprocessor_toolkit', '7.5',
                '1', '2', ])

            sys.stdout = sys.__stdout__

            self.assertEqual(
                text_trap.getvalue(),
                '123\n'
            )

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
            as_tuple=False,
            project=None
        )

    def test_coprocessor_class(self, *args):
        with io.StringIO() as text_trap:
            sys.stdout = text_trap

            fsl_sub.cmd.main([
                '--coprocessor', 'cuda',
                '--coprocessor_class', 'K',
                '1', '2', ])

            sys.stdout = sys.__stdout__

            self.assertEqual(
                text_trap.getvalue(),
                '123\n'
            )

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
            as_tuple=False,
            project=None
        )

    def test_coprocessor_class_strict(self, *args):
        with io.StringIO() as text_trap:
            sys.stdout = text_trap

            fsl_sub.cmd.main([
                '--coprocessor', 'cuda',
                '--coprocessor_class', 'K',
                '--coprocessor_class_strict',
                '1', '2', ])

            sys.stdout = sys.__stdout__

            self.assertEqual(
                text_trap.getvalue(),
                '123\n'
            )

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
            as_tuple=False,
            project=None
        )

    def test_coprocessor_multi(self, *args):
        with io.StringIO() as text_trap:
            sys.stdout = text_trap

            fsl_sub.cmd.main([
                '--coprocessor', 'cuda',
                '--coprocessor_multi', '2',
                '1', '2', ])

            sys.stdout = sys.__stdout__

            self.assertEqual(
                text_trap.getvalue(),
                '123\n'
            )

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
            as_tuple=False,
            project=None
        )

    def test_project(self, *args):
        args[3].return_value = yaml.safe_load(YAML_CONF_PROJECTS)
        args[4].return_value = yaml.safe_load(YAML_CONF_PROJECTS)
        args[5].return_value = yaml.safe_load(YAML_CONF_PROJECTS)
        fsl_sub.config.method_config.cache_clear()
        with patch(
                'fsl_sub.projects.read_config',
                autospec=True,
                return_value=yaml.safe_load(YAML_CONF_PROJECTS)):
            with io.StringIO() as text_trap:
                sys.stdout = text_trap

                fsl_sub.cmd.main(['--project', 'Aproject', '1', '2', ])

                sys.stdout = sys.__stdout__

                self.assertEqual(
                    text_trap.getvalue(),
                    '123\n'
                )
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
                ramsplit=True,
                requeueable=True,
                resources=None,
                usescript=False,
                validate_command=True,
                native_holds=False,
                as_tuple=False,
                project='Aproject'
            )

    def test_project_env(self, *args):
        args[3].return_value = yaml.safe_load(YAML_CONF_PROJECTS)
        args[4].return_value = yaml.safe_load(YAML_CONF_PROJECTS)
        args[5].return_value = yaml.safe_load(YAML_CONF_PROJECTS)
        fsl_sub.config.method_config.cache_clear()
        with patch(
                'fsl_sub.projects.read_config',
                autospec=True,
                return_value=yaml.safe_load(YAML_CONF_PROJECTS)):
            with patch.dict(
                    'fsl_sub.projects.os.environ',
                    {'FSLSUB_PROJECT': 'Bproject', }, clear=True):
                with io.StringIO() as text_trap:
                    sys.stdout = text_trap

                    fsl_sub.cmd.main(['1', '2', ])

                    sys.stdout = sys.__stdout__

                    self.assertEqual(
                        text_trap.getvalue(),
                        '123\n')
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
                    ramsplit=True,
                    requeueable=True,
                    resources=None,
                    usescript=False,
                    validate_command=True,
                    native_holds=False,
                    as_tuple=False,
                    project='Bproject'
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
        exp_conf = '''# These are added to defaults
method_opts:
  None:
    queues: False
    large_job_split_pe: Null
    mail_support: False
    map_ram: False
    job_priorities: False
    array_holds: False
    array_limit: False
    architecture: False
    job_resources: False
    script_conf: False
    projects: False
'''
        fsl_sub.cmd.example_config(['None', ])
        self.assertEqual(
            mock_stdout.getvalue(),
            exp_conf + '\n'
        )


@patch(
    'fsl_sub.cmd.find_fsldir', autospec=True,
    return_value='/usr/local/fsl'
)
@patch(
    'fsl_sub.cmd.conda_check_update', autospec=True
)
@patch(
    'fsl_sub.cmd.available_plugin_packages', autospec=True,
    return_value=['fsl_sub_plugin_sge', ]
)
class TestUpdate(unittest.TestCase):
    def test_update_check(
            self, mock_pp,
            mock_cup, mock_fsldir):

        mock_cup.return_value = {
            'fsl_sub': {'version': '2.0.0', 'old_version': '1.0.0', },
        }
        # Trap stdout
        with io.StringIO() as text_trap:
            sys.stdout = text_trap

            fsl_sub.cmd.update(args=['-c'])
            sys.stdout = sys.__stdout__

            self.assertEqual(
                text_trap.getvalue(),
                '''Available updates:
fsl_sub (1.0.0 -> 2.0.0)
'''
            )
        mock_cup.assert_called_with(
            fsldir='/usr/local/fsl',
            packages=['fsl_sub', 'fsl_sub_plugin_sge', ]
        )

    @patch(
        'fsl_sub.cmd.conda_update', autospec=True)
    def test_update_noquestion(
            self, mock_up, mock_pp,
            mock_cup, mock_fsldir):

        mock_cup.return_value = {
            'fsl_sub': {'version': '2.0.0', 'old_version': '1.0.0', },
        }
        mock_up.return_value = {
            'fsl_sub': {'version': '2.0.0', 'old_version': '1.0.0', },
        }
        # Trap stdout
        with io.StringIO() as text_trap:
            sys.stdout = text_trap

            fsl_sub.cmd.update(args=['-y'])
            sys.stdout = sys.__stdout__

            self.assertEqual(
                text_trap.getvalue(),
                '''Available updates:
fsl_sub (1.0.0 -> 2.0.0)
fsl_sub updated.
'''
            )
        mock_cup.assert_called_with(
            fsldir='/usr/local/fsl',
            packages=['fsl_sub', 'fsl_sub_plugin_sge', ]
        )

    @patch(
        'fsl_sub.cmd.conda_update', autospec=True)
    @patch(
        'fsl_sub.cmd.user_input', autospec=True
    )
    def test_update_ask(
            self, mock_input, mock_up, mock_pp,
            mock_cup, mock_fsldir):

        mock_cup.return_value = {
            'fsl_sub': {'version': '2.0.0', 'old_version': '1.0.0', },
        }
        mock_up.return_value = {
            'fsl_sub': {'version': '2.0.0', 'old_version': '1.0.0', },
        }
        mock_input.return_value = 'y'
        # Trap stdout
        with io.StringIO() as text_trap:
            sys.stdout = text_trap

            fsl_sub.cmd.update(args=[])
            sys.stdout = sys.__stdout__

            self.assertEqual(
                text_trap.getvalue(),
                '''Available updates:
fsl_sub (1.0.0 -> 2.0.0)
fsl_sub updated.
'''
            )
        mock_input.assert_called_once_with('Install pending updates? ')
        mock_input.reset_mock()
        mock_cup.assert_called_with(
            fsldir='/usr/local/fsl',
            packages=['fsl_sub', 'fsl_sub_plugin_sge', ]
        )

        mock_cup.reset_mock()
        mock_input.return_value = 'yes'
        # Trap stdout
        with io.StringIO() as text_trap:
            sys.stdout = text_trap

            fsl_sub.cmd.update(args=[])
            sys.stdout = sys.__stdout__

            self.assertEqual(
                text_trap.getvalue(),
                '''Available updates:
fsl_sub (1.0.0 -> 2.0.0)
fsl_sub updated.
'''
            )
        mock_cup.assert_called_with(
            fsldir='/usr/local/fsl',
            packages=['fsl_sub', 'fsl_sub_plugin_sge', ]
        )
        mock_input.assert_called_once_with('Install pending updates? ')
        mock_input.reset_mock()

        mock_cup.reset_mock()
        mock_input.return_value = 'no'

        # Trap stdout
        with io.StringIO() as text_trap:
            sys.stdout = text_trap
            with self.assertRaises(SystemExit) as cm:
                fsl_sub.cmd.update(args=[])
            sys.stdout = sys.__stdout__

            self.assertEqual(
                text_trap.getvalue(),
                '''Available updates:
fsl_sub (1.0.0 -> 2.0.0)
'''
            )
        mock_cup.assert_called_with(
            fsldir='/usr/local/fsl',
            packages=['fsl_sub', 'fsl_sub_plugin_sge', ]
        )
        self.assertEqual(
            'Aborted',
            str(cm.exception))
        mock_input.assert_called_once_with('Install pending updates? ')
        mock_input.reset_mock()

        mock_cup.reset_mock()
        mock_input.return_value = 'anythin'
        # Trap stdout
        with io.StringIO() as text_trap:
            sys.stdout = text_trap
            with self.assertRaises(SystemExit) as cm:
                fsl_sub.cmd.update(args=[])

            sys.stdout = sys.__stdout__

            self.assertEqual(
                text_trap.getvalue(),
                '''Available updates:
fsl_sub (1.0.0 -> 2.0.0)
'''
            )
        self.assertEqual(
            'Aborted',
            str(cm.exception))
        mock_cup.assert_called_with(
            fsldir='/usr/local/fsl',
            packages=['fsl_sub', 'fsl_sub_plugin_sge', ]
        )

        mock_cup.reset_mock()


@patch(
    'fsl_sub.cmd.find_fsldir', autospec=True,
    return_value='/usr/local/fsl'
)
@patch(
    'fsl_sub.cmd.conda_find_packages', autospec=True
)
@patch(
    'fsl_sub.cmd.available_plugin_packages', autospec=True,
    return_value=['fsl_sub_plugin_sge', ]
)
class TestInstall(unittest.TestCase):
    def test_install_plugin_list(
            self, mock_pp,
            mock_fp, mock_fsldir):

        mock_fp.return_value = ['fsl_sub_plugin_sge', ]
        # Trap stdout
        with io.StringIO() as text_trap:
            sys.stdout = text_trap

            with self.assertRaises(SystemExit):
                fsl_sub.cmd.install_plugin(args=['-l'])
            sys.stdout = sys.__stdout__

            self.assertEqual(
                text_trap.getvalue(),
                '''Available plugins:
fsl_sub_plugin_sge
'''
            )
        mock_fp.assert_called_with(
            'fsl_sub_plugin_*',
            fsldir='/usr/local/fsl',
        )

    @patch(
        'fsl_sub.cmd.conda_install', autospec=True)
    def test_list_and_install(
            self, mock_ci, mock_pp,
            mock_fp, mock_fsldir):

        mock_fp.return_value = ['fsl_sub_plugin_sge', ]
        mock_ci.return_value = {
            'fsl_sub_plugin_sge': {'version': '1.0.0', }}
        # Trap stdout
        with patch('fsl_sub.cmd.user_input', autospec=True) as ui:
            ui.return_value = '1'
            with io.StringIO() as text_trap:
                sys.stdout = text_trap

                fsl_sub.cmd.install_plugin(args=[])
                sys.stdout = sys.__stdout__

                self.assertEqual(
                    text_trap.getvalue(),
                    '''Available plugins:
1: fsl_sub_plugin_sge
Plugin fsl_sub_plugin_sge installed
You can generate an example config file with:
fsl_sub_config sge

The configuration file can be copied to /usr/local/fsl/etc/fslconf calling "
it fsl_sub.yml, or put in your home folder calling it .fsl_sub.yml. "
A copy in your home folder will override the file in "
/usr/local/fsl/etc/fslconf. Finally, the environment variable FSLSUB_CONF "
can be set to point at the configuration file, this will override all"
other files.
'''
                )
            mock_fp.assert_called_with(
                'fsl_sub_plugin_*',
                fsldir='/usr/local/fsl',
            )
            mock_ci.assert_called_once_with(
                'fsl_sub_plugin_sge'
            )
            ui.assert_called_once_with("Which backend? ")

    @patch(
        'fsl_sub.cmd.conda_install', autospec=True)
    def test_list_and_install_badchoice(
            self, mock_ci, mock_pp,
            mock_fp, mock_fsldir):

        mock_fp.return_value = ['fsl_sub_plugin_sge', ]
        mock_ci.return_value = {
            'fsl_sub_plugin_sge': {'version': '1.0.0', }}
        # Trap stdout
        with patch('fsl_sub.cmd.user_input', autospec=True) as ui:
            ui.return_value = '2'
            with io.StringIO() as text_trap:
                sys.stdout = text_trap

                with self.assertRaises(SystemExit) as se:
                    fsl_sub.cmd.install_plugin(args=[])
                    self.assertEqual(
                        str(se.exception),
                        'Invalid plugin number')
                sys.stdout = sys.__stdout__

                self.assertEqual(
                    text_trap.getvalue(),
                    '''Available plugins:
1: fsl_sub_plugin_sge
'''
                )
            mock_fp.assert_called_with(
                'fsl_sub_plugin_*',
                fsldir='/usr/local/fsl',
            )
            ui.assert_called_once_with("Which backend? ")

    @patch(
        'fsl_sub.cmd.conda_install', autospec=True)
    def test_install_direct(
            self, mock_ci, mock_pp,
            mock_fp, mock_fsldir):

        mock_fp.return_value = ['fsl_sub_plugin_sge', ]
        mock_ci.return_value = {
            'fsl_sub_plugin_sge': {'version': '1.0.0', }}
        # Trap stdout
        with io.StringIO() as text_trap:
            sys.stdout = text_trap

            fsl_sub.cmd.install_plugin(
                args=['-i', 'fsl_sub_plugin_sge'])
            sys.stdout = sys.__stdout__

            self.assertEqual(
                text_trap.getvalue(),
                '''Plugin fsl_sub_plugin_sge installed
You can generate an example config file with:
fsl_sub_config sge

The configuration file can be copied to /usr/local/fsl/etc/fslconf calling "
it fsl_sub.yml, or put in your home folder calling it .fsl_sub.yml. "
A copy in your home folder will override the file in "
/usr/local/fsl/etc/fslconf. Finally, the environment variable FSLSUB_CONF "
can be set to point at the configuration file, this will override all"
other files.
'''
            )
        mock_fp.assert_called_with(
            'fsl_sub_plugin_*',
            fsldir='/usr/local/fsl',
        )
        mock_ci.assert_called_once_with(
            'fsl_sub_plugin_sge'
        )

    @patch(
        'fsl_sub.cmd.conda_install', autospec=True)
    def test_install_direct_bad(
            self, mock_ci, mock_pp,
            mock_fp, mock_fsldir):

        mock_fp.return_value = ['fsl_sub_plugin_sge', ]
        mock_ci.return_value = {
            'fsl_sub_plugin_sge': {'version': '1.0.0', }}
        # Trap stdout
        with io.StringIO() as text_trap:
            sys.stdout = text_trap

            with self.assertRaises(SystemExit) as se:
                fsl_sub.cmd.install_plugin(
                    args=['-i', 'fsl_sub_plugin_slurm'])
                self.assertEqual(
                    'Unrecognised plugin',
                    str(se.exception)
                )
            sys.stdout = sys.__stdout__

        mock_fp.assert_called_with(
            'fsl_sub_plugin_*',
            fsldir='/usr/local/fsl',
        )

# Once main tests are written they will need:


# @patch(
#     'fsl_sub.cmd.load_plugins',
#     autospec=True,
#     return_value=FakePlugin()
#     )
