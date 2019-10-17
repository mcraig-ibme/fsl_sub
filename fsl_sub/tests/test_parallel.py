#!/usr/bin/python
import unittest
import yaml
import fsl_sub.parallel
from unittest.mock import patch
from fsl_sub.exceptions import ArgumentError


class TestParallelEnvs(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.config = yaml.safe_load('''
method: sge
thread_control:
  - OMP_THREADS
  - MKL_NUM_THREADS
method_opts:
  None:
    large_job_split_pe: []
    mail_support: False
    map_ram: False
    job_priorities: False
    parallel_holds: False
    parallel_limit: False
    architecture: False
    job_resources: False
  sge:
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
                fsl_sub.parallel.parallel_envs(self.config['queues']),
                ['shmem', ]
            )
        with self.subTest('Test 2'):
            self.config['queues']['long.q']['parallel_envs'] = ['make', ]
            self.assertCountEqual(
                fsl_sub.parallel.parallel_envs(self.config['queues']),
                ['shmem', 'make', ]
            )
        with self.subTest('Test 3'):
            self.config['queues']['long.q']['parallel_envs'] = [
                'make', 'shmem', ]
            self.assertCountEqual(
                fsl_sub.parallel.parallel_envs(self.config['queues']),
                ['shmem', 'make', ]
            )

    @patch('fsl_sub.parallel.parallel_envs')
    def test_process_pe_def(self, mock_parallel_envs):
        mock_parallel_envs.return_value = ['openmp', ]
        queues = self.config['queues']
        with self.subTest('Success'):
            self.assertTupleEqual(
                ('openmp', 2, ),
                fsl_sub.parallel.process_pe_def(
                    'openmp,2',
                    queues
                )
            )
        with self.subTest('Bad input'):
            self.assertRaises(
                ArgumentError,
                fsl_sub.parallel.process_pe_def,
                'openmp.2',
                queues
            )
        with self.subTest("No PE"):
            self.assertRaises(
                ArgumentError,
                fsl_sub.parallel.process_pe_def,
                'mpi,2',
                queues
            )
        with self.subTest("No PE"):
            self.assertRaises(
                ArgumentError,
                fsl_sub.parallel.process_pe_def,
                'mpi,A',
                queues
            )
        with self.subTest("No PE"):
            self.assertRaises(
                ArgumentError,
                fsl_sub.parallel.process_pe_def,
                'mpi,2.2',
                queues
            )


if __name__ == '__main__':
    unittest.main()
