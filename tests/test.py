#!/usr/bin/env python
import getpass
import os
import socket
import subprocess
import unittest
import yaml
import fsl_sub
from fsl_sub.exceptions import CommandError
from unittest.mock import patch

YAML_CONF = '''---
method: SGE
method_opts:
    SGE:
        parallel_envs:
        - shmem
        - specialpe
        same_node_pes:
        - shmem
        - specialpe
        large_job_split_pe: shmem
        copy_environment: True
        affinity_type: linear
        affinity_control: threads
        mail_support: True
        mail_modes:
        - b
        - e
        - a
        - s
        - n
        mail_mode: a
        map_ram: True
        ram_resources:
            - m_mem_free
            - h_vmem
        ram_units: G
        job_priorities: True
        min_priority: -1023
        max_priority: 0
        parallel_holds: True
        parallel_limit: True
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


class TestParallelEnvs(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.config = yaml.load('''
method: SGE
thread_control:
  - OMP_THREADS
  - MKL_NUM_THREADS
method_opts:
  None:
    parallel_envs: []
    same_node_pes: []
    large_job_split_pe: []
    mail_support: False
    map_ram: False
    job_priorities: False
    parallel_holds: False
    parallel_limit: False
    architecture: False
    job_resources: False
  SGE:
    parallel_envs:
      - shmem
    same_node_pes:
      - shmem
    large_job_split_pe: shmem
queues:
  short.q:
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
  long.q:
    time: 10080
    max_size: 160
    slot_size: 4
    max_slots: 16
    map_ram: true
    parallel_envs:
      - shmem
    priority: 3
    group: 2
''')

    def test_parallel_envs(self):
        with self.subTest('Test 1'):
            self.assertListEqual(
                fsl_sub.parallel_envs(self.config['queues']),
                ['shmem', ]
            )
        with self.subTest('Test 2'):
            self.config['queues']['long.q']['parallel_envs'] = ['make', ]
            self.assertCountEqual(
                fsl_sub.parallel_envs(self.config['queues']),
                ['shmem', 'make', ]
            )
        with self.subTest('Test 3'):
            self.config['queues']['long.q']['parallel_envs'] = [
                'make', 'shmem', ]
            self.assertCountEqual(
                fsl_sub.parallel_envs(self.config['queues']),
                ['shmem', 'make', ]
            )

    @patch('fsl_sub.parallel_envs')
    def test_process_pe_def(self, mock_parallel_envs):
        mock_parallel_envs.return_value = ['openmp', ]
        queues = self.config['queues']
        with self.subTest('Success'):
            self.assertTupleEqual(
                ('openmp', 2, ),
                fsl_sub.process_pe_def(
                    'openmp,2',
                    queues
                )
            )
        with self.subTest('Bad input'):
            self.assertRaises(
                fsl_sub.ArgumentError,
                fsl_sub.process_pe_def,
                'openmp.2',
                queues
            )
        with self.subTest("No PE"):
            self.assertRaises(
                fsl_sub.ArgumentError,
                fsl_sub.process_pe_def,
                'mpi,2',
                queues
            )
        with self.subTest("No PE"):
            self.assertRaises(
                fsl_sub.ArgumentError,
                fsl_sub.process_pe_def,
                'mpi,A',
                queues
            )
        with self.subTest("No PE"):
            self.assertRaises(
                fsl_sub.ArgumentError,
                fsl_sub.process_pe_def,
                'mpi,2.2',
                queues
            )


class GetQTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conf_dict = yaml.load(YAML_CONF)

    def test_getq_and_slots(self):
        with self.subTest('All a queues'):
            self.assertTupleEqual(
                ('a.qa,a.qb,a.qc', 1, ),
                fsl_sub.getq_and_slots(
                    self.conf_dict['queues'],
                    job_time=1000)
            )
        with self.subTest('Default queue'):
            self.assertTupleEqual(
                ('a.qa,a.qb,a.qc', 1, ),
                fsl_sub.getq_and_slots(
                    self.conf_dict['queues'])
            )
        with self.subTest("More RAM"):
            self.assertTupleEqual(
                ('a.qa,a.qc', 13, ),
                fsl_sub.getq_and_slots(
                    self.conf_dict['queues'],
                    job_time=1000,
                    job_ram=200)
            )
        with self.subTest("No time"):
            self.assertTupleEqual(
                ('a.qa,a.qc', 13, ),
                fsl_sub.getq_and_slots(
                    self.conf_dict['queues'],
                    job_ram=200)
            )
        with self.subTest("More RAM"):
            self.assertTupleEqual(
                ('a.qc', 19, ),
                fsl_sub.getq_and_slots(
                    self.conf_dict['queues'],
                    job_ram=300)
            )
        with self.subTest('Longer job'):
            self.assertTupleEqual(
                ('b.qa,b.qb,b.qc', 1, ),
                fsl_sub.getq_and_slots(
                    self.conf_dict['queues'],
                    job_time=2000)
            )
        with self.subTest('Too long job'):
            self.assertRaises(
                fsl_sub.BadSubmission,
                fsl_sub.getq_and_slots(
                    self.conf_dict['queues'],
                    job_time=200000)
            )
        with self.subTest("2x RAM"):
            self.assertIsNone(
                fsl_sub.getq_and_slots(
                    self.conf_dict['queues'],
                    job_ram=600)
            )
        with self.subTest('PE'):
            self.assertTupleEqual(
                ('t.q', 1, ),
                fsl_sub.getq_and_slots(
                    self.conf_dict['queues'],
                    ll_env="specialpe")
            )
        with self.subTest('PE missing'):
            self.assertIsNone(
                fsl_sub.getq_and_slots(
                    self.conf_dict['queues'],
                    ll_env="unknownpe")
            )
        with self.subTest('GPU'):
            self.assertTupleEqual(
                ('gpu.q', 1, ),
                fsl_sub.getq_and_slots(
                    self.conf_dict['queues'],
                    coprocessor='cuda')
            )


@patch(
    'fsl_sub.config.read_config',
    autospec=True,
    return_value=yaml.load(YAML_CONF))
@patch(
    'fsl_sub.read_config',
    autospec=True,
    return_value=yaml.load(YAML_CONF))
@patch(
    'fsl_sub.submit',
    autospec=True,
    return_value=123)
@patch(
    'fsl_sub.get_modules', autospec=True,
    return_value=['7.5', '8.0', ])
@patch(
    'fsl_sub.coprocessors.get_modules',
    autospec=True, return_value=['7.5', '8.0', ])
class TestMain(unittest.TestCase):
    def test_noramsplit(self, *args):
        fsl_sub.main(['--noramsplit', '1', '2', ])

        args[2].assert_called_with(
            ['1', '2', ],
            architecture=None,
            array_hold=None,
            array_limit=None,
            array_stride=1,
            array_task=False,
            coprocessor=None,
            coprocessor_toolkit=None,
            coprocessor_class=None,
            coprocessor_class_strict=False,
            coprocessor_multi=1,
            name=None,
            parallel_env=None,
            queue=None,
            threads=None,
            jobhold=None,
            jobram=None,
            jobtime=None,
            logdir=os.getcwd(),
            mail_on=None,
            mailto=USER_EMAIL,
            priority=None,
            ramsplit=False,
            resources=None,
            usescript=False,
            validate_command=True
        )

    def test_parallelenv(self, *args):
        fsl_sub.main(['--parallelenv', 'shmem,2', '1', '2', ])

        args[2].assert_called_with(
            ['1', '2', ],
            architecture=None,
            array_hold=None,
            array_limit=None,
            array_stride=1,
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
            logdir=os.getcwd(),
            mail_on=None,
            mailto=USER_EMAIL,
            priority=None,
            ramsplit=True,
            resources=None,
            usescript=False,
            validate_command=True
        )
        args[2].reset_mock()
        fsl_sub.main(['-s', 'shmem,2', '1', '2', ])

        args[2].assert_called_with(
            ['1', '2', ],
            architecture=None,
            array_hold=None,
            array_limit=None,
            array_stride=1,
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
            logdir=os.getcwd(),
            mail_on=None,
            mailto=USER_EMAIL,
            priority=None,
            ramsplit=True,
            resources=None,
            usescript=False,
            validate_command=True
        )

    def test_mailoptions(self, *args):
        fsl_sub.main(['--mailoptions', 'n', '1', '2', ])

        args[2].assert_called_with(
            ['1', '2', ],
            architecture=None,
            array_hold=None,
            array_limit=None,
            array_stride=1,
            array_task=False,
            coprocessor=None,
            coprocessor_toolkit=None,
            coprocessor_class=None,
            coprocessor_class_strict=False,
            coprocessor_multi=1,
            name=None,
            parallel_env=None,
            queue=None,
            threads=None,
            jobhold=None,
            jobram=None,
            jobtime=None,
            logdir=os.getcwd(),
            mail_on='n',
            mailto=USER_EMAIL,
            priority=None,
            ramsplit=True,
            resources=None,
            usescript=False,
            validate_command=True
        )

    def test_mailto(self, *args):
        fsl_sub.main(['--mailto', 'user@test.com', '1', '2', ])

        args[2].assert_called_with(
            ['1', '2', ],
            architecture=None,
            array_hold=None,
            array_limit=None,
            array_stride=1,
            array_task=False,
            coprocessor=None,
            coprocessor_toolkit=None,
            coprocessor_class=None,
            coprocessor_class_strict=False,
            coprocessor_multi=1,
            name=None,
            parallel_env=None,
            queue=None,
            threads=None,
            jobhold=None,
            jobram=None,
            jobtime=None,
            logdir=os.getcwd(),
            mail_on=None,
            mailto='user@test.com',
            priority=None,
            ramsplit=True,
            resources=None,
            usescript=False,
            validate_command=True
        )
        args[2].reset_mock()
        fsl_sub.main(['-M', 'user@test.com', '1', '2', ])

        args[2].assert_called_with(
            ['1', '2', ],
            architecture=None,
            array_hold=None,
            array_limit=None,
            array_stride=1,
            array_task=False,
            coprocessor=None,
            coprocessor_toolkit=None,
            coprocessor_class=None,
            coprocessor_class_strict=False,
            coprocessor_multi=1,
            name=None,
            parallel_env=None,
            queue=None,
            threads=None,
            jobhold=None,
            jobram=None,
            jobtime=None,
            logdir=os.getcwd(),
            mail_on=None,
            mailto='user@test.com',
            priority=None,
            ramsplit=True,
            resources=None,
            usescript=False,
            validate_command=True
        )

    def test_array_task(self, *args):
        fsl_sub.main(['--array_task', 'taskfile', ])

        args[2].assert_called_with(
            'taskfile',
            architecture=None,
            array_hold=None,
            array_limit=None,
            array_stride=1,
            array_task=True,
            coprocessor=None,
            coprocessor_toolkit=None,
            coprocessor_class=None,
            coprocessor_class_strict=False,
            coprocessor_multi=1,
            name=None,
            parallel_env=None,
            queue=None,
            threads=None,
            jobhold=None,
            jobram=None,
            jobtime=None,
            logdir=os.getcwd(),
            mail_on=None,
            mailto=USER_EMAIL,
            priority=None,
            ramsplit=True,
            resources=None,
            usescript=False,
            validate_command=True
        )
        args[2].reset_mock()
        fsl_sub.main(['-t', 'taskfile', ])

        args[2].assert_called_with(
            'taskfile',
            architecture=None,
            array_hold=None,
            array_limit=None,
            array_stride=1,
            array_task=True,
            coprocessor=None,
            coprocessor_toolkit=None,
            coprocessor_class=None,
            coprocessor_class_strict=False,
            coprocessor_multi=1,
            name=None,
            parallel_env=None,
            queue=None,
            threads=None,
            jobhold=None,
            jobram=None,
            jobtime=None,
            logdir=os.getcwd(),
            mail_on=None,
            mailto=USER_EMAIL,
            priority=None,
            ramsplit=True,
            resources=None,
            usescript=False,
            validate_command=True
        )

    def test_array_limit(self, *args):
        fsl_sub.main(['--array_task', 'commandfile', '--array_limit', '2', ])

        args[2].assert_called_with(
            'commandfile',
            architecture=None,
            array_hold=None,
            array_limit=2,
            array_stride=1,
            array_task=True,
            coprocessor=None,
            coprocessor_toolkit=None,
            coprocessor_class=None,
            coprocessor_class_strict=False,
            coprocessor_multi=1,
            name=None,
            parallel_env=None,
            queue=None,
            threads=None,
            jobhold=None,
            jobram=None,
            jobtime=None,
            logdir=os.getcwd(),
            mail_on=None,
            mailto=USER_EMAIL,
            priority=None,
            ramsplit=True,
            resources=None,
            usescript=False,
            validate_command=True
        )
        args[2].reset_mock()
        fsl_sub.main(['-x', '2', '--array_task', 'commandfile', ])

        args[2].assert_called_with(
            'commandfile',
            architecture=None,
            array_hold=None,
            array_limit=2,
            array_stride=1,
            array_task=True,
            coprocessor=None,
            coprocessor_toolkit=None,
            coprocessor_class=None,
            coprocessor_class_strict=False,
            coprocessor_multi=1,
            name=None,
            parallel_env=None,
            queue=None,
            threads=None,
            jobhold=None,
            jobram=None,
            jobtime=None,
            logdir=os.getcwd(),
            mail_on=None,
            mailto=USER_EMAIL,
            priority=None,
            ramsplit=True,
            resources=None,
            usescript=False,
            validate_command=True
        )

    def test_array_hold(self, *args):
        fsl_sub.main(
            ['--array_task', 'commandfile', '--array_hold', '20002', ])

        args[2].assert_called_with(
            'commandfile',
            architecture=None,
            array_hold='20002',
            array_limit=None,
            array_stride=1,
            array_task=True,
            coprocessor=None,
            coprocessor_toolkit=None,
            coprocessor_class=None,
            coprocessor_class_strict=False,
            coprocessor_multi=1,
            name=None,
            parallel_env=None,
            queue=None,
            threads=None,
            jobhold=None,
            jobram=None,
            jobtime=None,
            logdir=os.getcwd(),
            mail_on=None,
            mailto=USER_EMAIL,
            priority=None,
            ramsplit=True,
            resources=None,
            usescript=False,
            validate_command=True
        )

    def test_array_stride(self, *args):
        fsl_sub.main(
            ['--array_task', 'commandfile', '--array_stride', '2', ])

        args[2].assert_called_with(
            'commandfile',
            architecture=None,
            array_hold=None,
            array_limit=None,
            array_stride=2,
            array_task=True,
            coprocessor=None,
            coprocessor_toolkit=None,
            coprocessor_class=None,
            coprocessor_class_strict=False,
            coprocessor_multi=1,
            name=None,
            parallel_env=None,
            queue=None,
            threads=None,
            jobhold=None,
            jobram=None,
            jobtime=None,
            logdir=os.getcwd(),
            mail_on=None,
            mailto=USER_EMAIL,
            priority=None,
            ramsplit=True,
            resources=None,
            usescript=False,
            validate_command=True
        )

    def test_coprocessor(self, *args):
        fsl_sub.main(['--coprocessor', 'cuda', '1', '2', ])

        args[2].assert_called_with(
            ['1', '2', ],
            architecture=None,
            array_hold=None,
            array_limit=None,
            array_stride=1,
            array_task=False,
            coprocessor='cuda',
            coprocessor_toolkit=None,
            coprocessor_class=None,
            coprocessor_class_strict=False,
            coprocessor_multi=1,
            name=None,
            parallel_env=None,
            queue=None,
            threads=None,
            jobhold=None,
            jobram=None,
            jobtime=None,
            logdir=os.getcwd(),
            mail_on=None,
            mailto=USER_EMAIL,
            priority=None,
            ramsplit=True,
            resources=None,
            usescript=False,
            validate_command=True
        )

    def test_coprocessor_toolkit(self, *args):
        fsl_sub.main([
            '--coprocessor', 'cuda',
            '--coprocessor_toolkit', '7.5',
            '1', '2', ])

        args[2].assert_called_with(
            ['1', '2', ],
            architecture=None,
            array_hold=None,
            array_limit=None,
            array_stride=1,
            array_task=False,
            coprocessor='cuda',
            coprocessor_toolkit='7.5',
            coprocessor_class=None,
            coprocessor_class_strict=False,
            coprocessor_multi=1,
            name=None,
            parallel_env=None,
            queue=None,
            threads=None,
            jobhold=None,
            jobram=None,
            jobtime=None,
            logdir=os.getcwd(),
            mail_on=None,
            mailto=USER_EMAIL,
            priority=None,
            ramsplit=True,
            resources=None,
            usescript=False,
            validate_command=True
        )

    def test_coprocessor_class(self, *args):
        fsl_sub.main([
            '--coprocessor', 'cuda',
            '--coprocessor_class', 'K',
            '1', '2', ])

        args[2].assert_called_with(
            ['1', '2', ],
            architecture=None,
            array_hold=None,
            array_limit=None,
            array_stride=1,
            array_task=False,
            coprocessor='cuda',
            coprocessor_toolkit=None,
            coprocessor_class='K',
            coprocessor_class_strict=False,
            coprocessor_multi=1,
            name=None,
            parallel_env=None,
            queue=None,
            threads=None,
            jobhold=None,
            jobram=None,
            jobtime=None,
            logdir=os.getcwd(),
            mail_on=None,
            mailto=USER_EMAIL,
            priority=None,
            ramsplit=True,
            resources=None,
            usescript=False,
            validate_command=True
        )

    def test_coprocessor_class_strict(self, *args):
        fsl_sub.main([
            '--coprocessor', 'cuda',
            '--coprocessor_class', 'K',
            '--coprocessor_class_strict',
            '1', '2', ])

        args[2].assert_called_with(
            ['1', '2', ],
            architecture=None,
            array_hold=None,
            array_limit=None,
            array_stride=1,
            array_task=False,
            coprocessor='cuda',
            coprocessor_toolkit=None,
            coprocessor_class='K',
            coprocessor_class_strict=True,
            coprocessor_multi=1,
            name=None,
            parallel_env=None,
            queue=None,
            threads=None,
            jobhold=None,
            jobram=None,
            jobtime=None,
            logdir=os.getcwd(),
            mail_on=None,
            mailto=USER_EMAIL,
            priority=None,
            ramsplit=True,
            resources=None,
            usescript=False,
            validate_command=True
        )

    def test_coprocessor_multi(self, *args):
        fsl_sub.main([
            '--coprocessor', 'cuda',
            '--coprocessor_multi', '2',
            '1', '2', ])

        args[2].assert_called_with(
            ['1', '2', ],
            architecture=None,
            array_hold=None,
            array_limit=None,
            array_stride=1,
            array_task=False,
            coprocessor='cuda',
            coprocessor_toolkit=None,
            coprocessor_class=None,
            coprocessor_class_strict=False,
            coprocessor_multi='2',
            name=None,
            parallel_env=None,
            queue=None,
            threads=None,
            jobhold=None,
            jobram=None,
            jobtime=None,
            logdir=os.getcwd(),
            mail_on=None,
            mailto=USER_EMAIL,
            priority=None,
            ramsplit=True,
            resources=None,
            usescript=False,
            validate_command=True
        )


class TestFileIsImage(unittest.TestCase):
    @patch('fsl_sub.os.path.isfile', autospec=True)
    @patch('fsl_sub.system_stdout', autospec=True)
    def test_file_is_image(self, mock_sstdout, mock_isfile):
        mock_isfile.return_value = False
        self.assertFalse(fsl_sub.file_is_image('a'))
        mock_isfile.return_value = True
        mock_sstdout.return_value = '1\n'
        self.assertTrue(fsl_sub.file_is_image('a'))
        mock_sstdout.return_value = '0\n'
        self.assertFalse(fsl_sub.file_is_image('a'))
        mock_sstdout.side_effect = subprocess.CalledProcessError(
            1, 'a', "failed")
        self.assertRaises(
            CommandError,
            fsl_sub.file_is_image,
            'a'
        )


if __name__ == '__main__':
    unittest.main()
