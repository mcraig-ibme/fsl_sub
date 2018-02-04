#!/usr/bin/env python
import unittest
import yaml
import fsl_sub
import subprocess
from unittest.mock import patch


class TestCoprocessors(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.config = yaml.load('''
queues:
    cuda.q:
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
    phi.q:
        time: 1440
        max_size: 160
        slot_size: 4
        max_slots: 16
        copros:
            phi:
                max_quantity: 2
        map_ram: true
        parallel_envs:
            - shmem
        priority: 1
        group: 1
    default: true
copro_opts:
    cuda:
        resource: gpu
        classes: True
        class_resource: gputype
        class_types:
        G:
            resource: TitanX
            doc: TitanX. No-ECC, single-precision workloads
            capability: 1
        K:
            resource: k80
            doc: Kepler. ECC, double- or single-precision workloads
            capability: 2
        P:
            resource: v100
            doc: >
            Pascal. ECC, double-, single- and half-precision
            workloads
            capability: 3
        V:
            resource: v100
            doc: >
            Volta. ECC, double-, single-, half-
            and quarter-precision workloads
            capability: 4
        default_class: K
        include_more_capable: True
        uses_modules: True
        module_parent: cuda
    phi:
        resource: phi
        classes: False
        users_modules: True
        module_parent: phi
''')

    def test_list_coprocessors(self):
        self.assertCountEqual(
            fsl_sub.list_coprocessors(self.config),
            ['cuda', 'phi', ])

    def test_max_coprocessors(self):
        with self.subTest("Max CUDA"):
            self.assertEqual(
                fsl_sub.max_coprocessors(
                    self.config,
                    'cuda'),
                4
            )
        with self.subTest("Max Phi"):
            self.assertEqual(
                fsl_sub.max_coprocessors(
                    self.config,
                    'phi'),
                2
            )

    def test_copro_classes(self):
        with self.subTest("CUDA classes"):
            self.assertListEqual(
                fsl_sub.copro_classes(
                    self.config,
                    'cuda'),
                ['G', 'K', 'P', 'V', ]
            )
        with self.subTest("Phi classes"):
            self.assertIsNone(
                fsl_sub.copro_classes(
                    self.config,
                    'phi'))

    @patch('fsl_sub.get_modules', auto_spec=True)
    def test_coprocessor_toolkits(self, mock_get_modules):
        with self.subTest("CUDA toolkits"):
            mock_get_modules.return_value = ['6.5', '7.5', '7.0', ]
            self.assertEqual(
                fsl_sub.coprocessor_toolkits(
                        self.config,
                        'cuda'),
                ['6.5', '7.0', '7.5', ]
                )
            mock_get_modules.assert_called_once_with('cuda')


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
                fsl_sub.parallel_envs(self.conf['queues']),
                ['shmem', ]
            )
        with self.subTest('Test 2'):
            self.conf['queues']['long.q']['parallel_envs'] = ['make', ]
            self.assertListEqual(
                fsl_sub.parallel_envs(self.conf['queues']),
                ['shmem', 'make', ]
            )
        with self.subTest('Test 3'):
            self.conf['queues']['long.q']['parallel_envs'] = ['make', 'shmem', ]
            self.assertListEqual(
                fsl_sub.parallel_envs(self.conf['queues']),
                ['shmem', 'make', ]
            )


class TestModuleSupport(unittest.TestCase):
    @patch('fsl_sub.shutil.which')
    def test_find_module_cmd(self, mock_which):
        mock_which.return_value = '/usr/bin/modulecmd'
        self.assertEqual(
            fsl_sub.find_module_cmd(),
            '/usr/bin/modulecmd')
        mock_which.assert_called_once_with('modulecmd')

    def test_read_module_environment(self):
        lines = '''
os.environ['PATH']='/usr/bin:/usr/sbin:/usr/local/bin'
os.environ['LD_LIBRARY_PATH']='/usr/lib64:/usr/local/lib64'
'''
        self.assertDictEqual(
            fsl_sub.read_module_environment(lines),
            {'PATH': '/usr/bin:/usr/sbin:/usr/local/bin',
             'LD_LIBRARY_PATH': '/usr/lib64:/usr/local/lib64', }
        )

    @patch('fsl_sub.find_module_cmd')
    @patch('fsl_sub.system_stdout')
    @patch('fsl_sub.read_module_environment')
    def test_module_add(
            self,
            mock_read_module_environment,
            mock_system_stdout,
            mock_find_module_cmd):
        mcmd = '/usr/bin/modulecmd'
        mock_system_stdout.return_value = '''
os.environ['PATH']='/usr/bin:/usr/sbin:/usr/local/bin'
os.environ['LD_LIBRARY_PATH']='/usr/lib64:/usr/local/lib64'
'''
        mock_find_module_cmd.return_value = mcmd
        mock_read_module_environment.return_value = 'some text'
        with self.subTest('Test 1'):
            self.assertEqual(
                fsl_sub.module_add('amodule'),
                'some text'
            )
            mock_find_module_cmd.assert_called_once_with()
            mock_read_module_environment.assert_called_once_with(
                '''
    os.environ['PATH']='/usr/bin:/usr/sbin:/usr/local/bin'
    os.environ['LD_LIBRARY_PATH']='/usr/lib64:/usr/local/lib64'
    '''
            )
            mock_system_stdout.assert_called_once_with(
                mcmd, "python", "add", 'amodule')
        with self.subTest('Test 2'):
            mock_system_stdout.side_effect(
                subprocess.CalledProcessError('An Error'))
            self.assertRaises(
                fsl_sub.LoadModuleError,
                fsl_sub.module_add('amodule'))

        with self.subTest('Test 3'):
            mock_find_module_cmd.return_value = ''
            self.assertFalse(
                fsl_sub.module_add('amodule')
            )

    @patch('fsl_sub.module_add')
    @patch.dict('fsl_sub.os.environ')
    def test_load_module(self, mock_environ, mock_module_add):
        mock_environ = {}
        mock_module_add.return_value = {'VAR': 'VAL', 'VAR2': 'VAL2', }
        with self.subTest('Test 1'):
            self.assertTrue(
                fsl_sub.load_module('amodule'))
            self.assertDictEqual(
                mock_environ,
                {'VAR': 'VAL', 'VAR2': 'VAL2', }
            )
        with self.subTest('Test 2'):
            mock_module_add.return_value = {}
            self.assertFalse(
                fsl_sub.load_module('amodule'))

    @patch('fsl_sub.module_add')
    @patch.dict('fsl_sub.os.environ')
    def test_unload_module(self, mock_environ, mock_module_add):
        mock_environ = {'VAR': 'VAL', 'VAR2': 'VAL2', 'EXISTING': 'VALUE', }
        mock_module_add.return_value = {'VAR': 'VAL', 'VAR2': 'VAL2', }
        with self.subTest('Test 1'):
            self.assertTrue(
                fsl_sub.unload_module('amodule'))
            self.assertDictEqual(
                mock_environ,
                {'EXISTING': 'VALUE', }
            )
        with self.subTest('Test 2'):
            mock_module_add.return_value = {}
            self.assertFalse(
                fsl_sub.unload_module('amodule'))

    @patch.dict('fsl_sub.os.environ')
    def test_unload_module(self, mock_environ, mock_module_add):
        mock_environ = {'LOADEDMODULES': 'mod1:mod2:mod3', 'EXISTING': 'VALUE', }
        with self.subTest('Test 1'):
            self.assertListEqual(
                fsl_sub.loaded_modules(),
                ['mod1', 'mod2', 'mod3', ])
        with self.subTest('Test 2'):
            mock_environ = {'EXISTING': 'VALUE', }
            self.assertListEqual(
                fsl_sub.loaded_modules(),
                [])

    @patch.dict('fsl_sub.system_stdout')
    def test_get_modules(self, mock_system_stdout):
        mock_system_stdout = '''
/usr/local/etc/ShellModules:
amodule/5.0
amodule/5.5
/usr/share/Modules/modulefiles:
/etc/modulefiles:
'''
        with self.subTest('Test 1'):
            self.assertListEqual(
                fsl_sub.get_modules('amodule'),
                ['5.0', '5.5', ])
        with self.subTest('Test 2'):
            mock_system_stdout.side_effect = subprocess.CalledProcessError(
                'An Error')
            self.assertRaises(
                fsl_sub.NoModule,
                fsl_sub.get_modules('amodule'))

    @patch.dict('fsl_sub.get_modules')
    def test_latest_module(self, mock_get_modules):
        mock_get_modules.return_value = ['5.5', '5.0', ]
        with self.subTest('Test 1'):
            self.assertEqual(
                fsl_sub.latest_module('amodule'),
                '5.5')
        with self.subTest('Test 2'):
            mock_get_modules.return_value = ['5.0', '5.5', ]
            self.assertEqual(
                fsl_sub.latest_module('amodule'),
                '5.5')
        with self.subTest('Test 3'):
            mock_get_modules.return_value = None
            self.assertFalse(
                fsl_sub.latest_module('amodule')
            )
        with self.subTest('Test 4'):
            mock_get_modules.side_effect = fsl_sub.NoModule('amodule')
            self.assertRaises(
                fsl_sub.NoModule,
                fsl_sub.latest_module('amodule')
            )

    def test_module_string(self):
        with self.subTest('Test 1'):
            self.assertEqual(
                fsl_sub.module_string('amodule', '5.0'),
                'amodule/5.0'
            )
        with self.subTest('Test 2'):
            self.assertEqual(
                fsl_sub.module_string('amodule', None),
                'amodule'
            )


class TestUtils(unittest.TestCase):
    def test_minutes_to_human(self):
        with self.subTest('Test 1'):
            self.assertEqual(
                fsl_sub.minutes_to_human(
                    10,
                    '10m'
                )
            )
        with self.subTest('Test 2'):
            self.assertEqual(
                fsl_sub.minutes_to_human(
                    23 * 60,
                    '23h'
                )
            )
        with self.subTest('Test 3'):
            self.assertEqual(
                fsl_sub.minutes_to_human(
                    48 * 60,
                    '2d'
                )
            )
        with self.subTest('Test 4'):
            self.assertEqual(
                fsl_sub.minutes_to_human(
                    23 * 59,
                    '22.6h'
                )
            )
        with self.subTest('Test 5'):
            self.assertEqual(
                fsl_sub.minutes_to_human(
                    48 * 58,
                    '1.9d'
                )
            )


if __name__ == '__main__':
    unittest.main()
