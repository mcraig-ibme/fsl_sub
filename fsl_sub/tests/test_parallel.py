#!/usr/bin/python
import os
import os.path
import unittest
import yaml
import fsl_sub.parallel
from tempfile import TemporaryDirectory
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

    def test_parallel_submit(self):
        with TemporaryDirectory() as tempdir:
            # make a file with commands to run
            # parallel submit this
            # check .o and .e files
            pfile_name = "pjobs"
            cfile = os.path.join(tempdir, pfile_name)
            outputs = ['a', 'b', 'c']
            with open(cfile, 'w') as cf:
                for a in outputs:
                    cf.write("echo " + a + '\n')
            os.chdir(tempdir)
            jid = str(fsl_sub.submit(
                    cfile,
                    name=None,
                    array_task=True))
            with self.subTest("Check output files"):
                for st in range(len(outputs)):
                    stask_id = str(st + 1)
                    of = ".".join((pfile_name, 'o' + jid, stask_id))
                    ef = ".".join((pfile_name, 'e' + jid, stask_id))
                    self.assertEqual(
                        os.path.getsize(
                            os.path.join(tempdir, ef)
                        ),
                        0
                    )
                    self.assertNotEqual(
                        os.path.getsize(
                            os.path.join(tempdir, of)
                        ),
                        0
                    )
            with self.subTest("Check .o content"):
                for st in range(len(outputs)):
                    stask_id = str(st + 1)
                    of = ".".join((pfile_name, 'o' + jid, stask_id))
                    ef = ".".join((pfile_name, 'e' + jid, stask_id))
                    with open(os.path.join(tempdir, of), 'r') as ofile:
                        output = ofile.readline()
                    self.assertEqual(output, outputs[st] + '\n')

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
