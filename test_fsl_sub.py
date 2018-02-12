#!/usr/bin/env python
import unittest
import yaml
import fsl_sub
import subprocess
from unittest.mock import (patch, mock_open)


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

    def test_coproc_classes(self):
        with self.subTest("CUDA classes"):
            self.assertListEqual(
                fsl_sub.coproc_classes(
                    self.config,
                    'cuda'),
                ['K', 'P', 'V', ]
            )
        with self.subTest("Phi classes"):
            self.assertIsNone(
                fsl_sub.coproc_classes(
                    self.config,
                    'phi'))

    @patch('fsl_sub.get_modules', auto_spec=True)
    def test_coproc_toolkits(self, mock_get_modules):
        with self.subTest("CUDA toolkits"):
            mock_get_modules.return_value = ['6.5', '7.0', '7.5', ]
            self.assertEqual(
                fsl_sub.coproc_toolkits(
                        self.config,
                        'cuda'),
                ['6.5', '7.0', '7.5', ]
                )
            mock_get_modules.assert_called_once_with('cuda')
####  This isn't really a useful test!


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
            self.assertEqual(
                {'name': 'openmp', 'slots': 2},
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


class TestModuleSupport(unittest.TestCase):
    @patch('fsl_sub.shutil.which', auto_spec=True)
    def test_find_module_cmd(self, mock_which):
        mock_which.return_value = '/usr/bin/modulecmd'
        self.assertEqual(
            fsl_sub.find_module_cmd(),
            '/usr/bin/modulecmd')
        mock_which.assert_called_once_with('modulecmd')

    def test_read_module_environment(self):
        lines = [
            "os.environ['PATH']='/usr/bin:/usr/sbin:/usr/local/bin'",
            "os.environ['LD_LIBRARY_PATH']='/usr/lib64:/usr/local/lib64'",
        ]
        self.assertDictEqual(
            fsl_sub.read_module_environment(lines),
            {'PATH': '/usr/bin:/usr/sbin:/usr/local/bin',
             'LD_LIBRARY_PATH': '/usr/lib64:/usr/local/lib64', }
        )

    @patch('fsl_sub.find_module_cmd', auto_spec=True)
    @patch('fsl_sub.system_stdout', auto_spec=True)
    @patch('fsl_sub.read_module_environment', auto_spec=True)
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
                (mcmd, "python", "add", 'amodule', ), shell=True)
        with self.subTest('Test 2'):
            mock_system_stdout.side_effect = subprocess.CalledProcessError(
                'acmd', 1)
            self.assertRaises(
                fsl_sub.LoadModuleError,
                fsl_sub.module_add,
                'amodule')

        with self.subTest('Test 3'):
            mock_find_module_cmd.return_value = ''
            self.assertFalse(
                fsl_sub.module_add('amodule')
            )

    @patch('fsl_sub.module_add', auto_spec=True)
    @patch.dict('fsl_sub.os.environ', {}, clear=True)
    def test_load_module(self, mock_module_add):
        mock_module_add.return_value = {'VAR': 'VAL', 'VAR2': 'VAL2', }
        with self.subTest('Test 1'):
            self.assertTrue(
                fsl_sub.load_module('amodule'))
            self.assertDictEqual(
                dict(fsl_sub.os.environ),
                {'VAR': 'VAL', 'VAR2': 'VAL2', }
            )
        with self.subTest('Test 2'):
            mock_module_add.return_value = {}
            self.assertFalse(
                fsl_sub.load_module('amodule'))

    @patch('fsl_sub.module_add', auto_spec=True)
    @patch.dict(
        'fsl_sub.os.environ',
        {'VAR': 'VAL', 'VAR2': 'VAL2', 'EXISTING': 'VALUE', },
        clear=True)
    def test_unload_module(self, mock_module_add):
        mock_module_add.return_value = {'VAR': 'VAL', 'VAR2': 'VAL2', }
        with self.subTest('Test 1'):
            self.assertTrue(
                fsl_sub.unload_module('amodule'))
            self.assertDictEqual(
                dict(fsl_sub.os.environ),
                {'EXISTING': 'VALUE', }
            )
        with self.subTest('Test 2'):
            mock_module_add.return_value = {}
            self.assertFalse(
                fsl_sub.unload_module('amodule'))

    @patch.dict(
        'fsl_sub.os.environ',
        {'LOADEDMODULES': 'mod1:mod2:mod3', 'EXISTING': 'VALUE', },
        clear=True)
    def test_loaded_modules(self):
        with self.subTest('Test 1'):
            self.assertListEqual(
                fsl_sub.loaded_modules(),
                ['mod1', 'mod2', 'mod3', ])
        with self.subTest('Test 2'):
            with patch.dict(
                    'fsl_sub.os.environ',
                    {'EXISTING': 'VALUE', },
                    clear=True):
                self.assertListEqual(
                    fsl_sub.loaded_modules(),
                    [])

    @patch('fsl_sub.system_stdout', auto_spec=True)
    def test_get_modules(self, mock_system_stdout):
        mock_system_stdout.return_value = '''
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
                'acmd', 1
            )
            self.assertRaises(
                fsl_sub.NoModule,
                fsl_sub.get_modules, 'amodule')

    @patch('fsl_sub.get_modules', auto_spec=True)
    def test_latest_module(self, mock_get_modules):
        with self.subTest('Test 1'):
            mock_get_modules.return_value = ['5.0', '5.5', ]
            self.assertEqual(
                fsl_sub.latest_module('amodule'),
                '5.5')
        with self.subTest('Test 2'):
            mock_get_modules.return_value = None
            self.assertFalse(
                fsl_sub.latest_module('amodule')
            )
        with self.subTest('Test 3'):
            mock_get_modules.side_effect = fsl_sub.NoModule('amodule')
            self.assertRaises(
                fsl_sub.NoModule,
                fsl_sub.latest_module, 'amodule'
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
                fsl_sub.minutes_to_human(10),
                '10m'
            )
        with self.subTest('Test 2'):
            self.assertEqual(
                fsl_sub.minutes_to_human(23 * 60),
                '23h'
            )
        with self.subTest('Test 3'):
            self.assertEqual(
                fsl_sub.minutes_to_human(48 * 60),
                '2d'
            )
        with self.subTest('Test 4'):
            self.assertEqual(
                fsl_sub.minutes_to_human(23 * 59),
                '22.6h'
            )
        with self.subTest('Test 5'):
            self.assertEqual(
                fsl_sub.minutes_to_human(48 * 58),
                '1.9d'
            )

    @patch.dict(
        'fsl_sub.os.environ',
        {},
        clear=True)
    def test_control_threads(self):
        fsl_sub.control_threads(
                ['THREADS', 'MORETHREADS', ],
                1)
        self.assertDictEqual(
            dict(fsl_sub.os.environ),
            {'THREADS': '1', 'MORETHREADS': '1'}
        )

    @patch('fsl_sub.shutil.which')
    def test_check_command(self, mock_which):
        mock_which.return_value = None
        self.assertRaises(
            fsl_sub.ArgumentError,
            fsl_sub.check_command, 'acommand'
        )

    @patch('fsl_sub.check_command')
    def test_check_command_file(
            self, mock_check_command):
        with patch(
                '__main__.open',
                mock_open(read_data='A')) as m:
            with open('foo') as cmdf:
                self.assertEqual(
                    fsl_sub.check_command_file(cmdf),
                    1
                )
        with patch(
                '__main__.open',
                mock_open(read_data='A')) as m:
            with open('foo') as cmdf:
                cmdf.seek(0)
                mock_check_command.side_effect = fsl_sub.ArgumentError()
                self.assertRaises(
                    fsl_sub.ArgumentError,
                    fsl_sub.check_command_file,
                    cmdf
                )

    def test_split_ram_by_slots(self):
        self.assertEqual(
            fsl_sub.split_ram_by_slots(10, 5),
            2
        )
        self.assertEqual(
            fsl_sub.split_ram_by_slots(10, 3),
            4
        )

    def test_affirmative(self):
        with self.subTest('yes'):
            self.assertTrue(fsl_sub.affirmative('yes'))
        with self.subTest('Yes'):
            self.assertTrue(fsl_sub.affirmative('Yes'))
        with self.subTest('YES'):
            self.assertTrue(fsl_sub.affirmative('YES'))
        with self.subTest('YEs'):
            self.assertTrue(fsl_sub.affirmative('YEs'))
        with self.subTest('y'):
            self.assertTrue(fsl_sub.affirmative('y'))
        with self.subTest('Y'):
            self.assertTrue(fsl_sub.affirmative('Y'))
        with self.subTest('true'):
            self.assertTrue(fsl_sub.affirmative('true'))
        with self.subTest('True'):
            self.assertTrue(fsl_sub.affirmative('True'))
        with self.subTest('TRUE'):
            self.assertTrue(fsl_sub.affirmative('TRUE'))
        with self.subTest('TRue'):
            self.assertTrue(fsl_sub.affirmative('TRue'))
        with self.subTest('TRUe'):
            self.assertTrue(fsl_sub.affirmative('TRUe'))
        with self.subTest('no'):
            self.assertFalse(fsl_sub.affirmative('no'))
        with self.subTest('No'):
            self.assertFalse(fsl_sub.affirmative('No'))
        with self.subTest('NO'):
            self.assertFalse(fsl_sub.affirmative('NO'))
        with self.subTest('n'):
            self.assertFalse(fsl_sub.affirmative('n'))
        with self.subTest('N'):
            self.assertFalse(fsl_sub.affirmative('N'))
        with self.subTest('false'):
            self.assertFalse(fsl_sub.affirmative('false'))
        with self.subTest('False'):
            self.assertFalse(fsl_sub.affirmative('False'))
        with self.subTest('FALSE'):
            self.assertFalse(fsl_sub.affirmative('FALSE'))
        with self.subTest('FAlse'):
            self.assertFalse(fsl_sub.affirmative('FAlse'))
        with self.subTest('FALse'):
            self.assertFalse(fsl_sub.affirmative('FALse'))
        with self.subTest('FALSe'):
            self.assertFalse(fsl_sub.affirmative('FALSe'))

    def test_negative(self):
        with self.subTest('no'):
            self.assertTrue(fsl_sub.negative('no'))
        with self.subTest('No'):
            self.assertTrue(fsl_sub.negative('No'))
        with self.subTest('NO'):
            self.assertTrue(fsl_sub.negative('NO'))
        with self.subTest('n'):
            self.assertTrue(fsl_sub.negative('n'))
        with self.subTest('N'):
            self.assertTrue(fsl_sub.negative('N'))
        with self.subTest('false'):
            self.assertTrue(fsl_sub.negative('false'))
        with self.subTest('False'):
            self.assertTrue(fsl_sub.negative('False'))
        with self.subTest('FALSE'):
            self.assertTrue(fsl_sub.negative('FALSE'))
        with self.subTest('FAlse'):
            self.assertTrue(fsl_sub.negative('FAlse'))
        with self.subTest('FALse'):
            self.assertTrue(fsl_sub.negative('FALse'))
        with self.subTest('FALSe'):
            self.assertTrue(fsl_sub.negative('FALSe'))
        with self.subTest('yes'):
            self.assertFalse(fsl_sub.negative('yes'))
        with self.subTest('Yes'):
            self.assertFalse(fsl_sub.negative('Yes'))
        with self.subTest('YES'):
            self.assertFalse(fsl_sub.negative('YES'))
        with self.subTest('YEs'):
            self.assertFalse(fsl_sub.negative('YEs'))
        with self.subTest('y'):
            self.assertFalse(fsl_sub.negative('y'))
        with self.subTest('Y'):
            self.assertFalse(fsl_sub.negative('Y'))
        with self.subTest('true'):
            self.assertFalse(fsl_sub.negative('true'))
        with self.subTest('True'):
            self.assertFalse(fsl_sub.negative('True'))
        with self.subTest('TRUE'):
            self.assertFalse(fsl_sub.negative('TRUE'))
        with self.subTest('TRue'):
            self.assertFalse(fsl_sub.negative('TRue'))
        with self.subTest('TRUe'):
            self.assertFalse(fsl_sub.negative('TRUe'))


class TestConfig(unittest.TestCase):
    @patch('fsl_sub.os.path.expanduser')
    @patch('fsl_sub.os.path.exists')
    def test_find_config_file(
            self, mock_exists, mock_expanduser):
        with self.subTest('Expand user'):
            mock_exists.side_effect = [True]
            mock_expanduser.return_value = '/home/auser'
            self.assertEqual(
                fsl_sub.find_config_file(),
                '/home/auser/.fsl_sub.yml'
            )
        mock_exists.reset_mock()
        with patch.dict(
                'fsl_sub.os.environ',
                {'FSLDIR': '/usr/local/fsl', },
                clear=True):
            with self.subTest('FSLDIR'):
                mock_exists.side_effect = [False, True]
                self.assertEqual(
                    fsl_sub.find_config_file(),
                    '/usr/local/fsl/etc/fslconf/fsl_sub.yml'
                )
        mock_exists.reset_mock()
        with self.subTest('Missing configuration'):
            mock_exists.side_effect = [False, False]
            self.assertRaises(
                fsl_sub.BadConfiguration,
                fsl_sub.find_config_file
            )
        mock_exists.reset_mock()
        mock_exists.side_effect = [False, False]
        with self.subTest('Environment variable'):
            with patch.dict(
                    'fsl_sub.os.environ',
                    {'FSLSUB_CONF': '/etc/fsl_sub.yml', },
                    clear=True):
                self.assertEqual(
                    fsl_sub.find_config_file(),
                    '/etc/fsl_sub.yml'
                )

    @patch('fsl_sub.find_config_file')
    def test_read_config(self, mock_find_config_file):
        example_yaml = '''
adict:
    alist:
        - 1
        - 2
    astring: hello
'''
        mock_find_config_file.return_value = '/etc/fsl_sub.conf'
        with patch(
                'fsl_sub.open',
                unittest.mock.mock_open(read_data=example_yaml)) as m:
            self.assertDictEqual(
                fsl_sub.read_config(),
                {'adict': {
                    'alist': [1, 2],
                    'astring': 'hello',
                }}
            )
            m.assert_called_once_with('/etc/fsl_sub.conf', 'r')
            bad_yaml = "unbalanced: ]["
        with patch(
                'fsl_sub.open',
                unittest.mock.mock_open(read_data=bad_yaml)) as m:
            self.assertRaises(
                fsl_sub.BadConfiguration,
                fsl_sub.read_config)


class TestPlugins(unittest.TestCase):
    @patch('fsl_sub.pkgutil.iter_modules', auto_spec=True)
    @patch('fsl_sub.importlib.import_module', auto_spec=True)
    def test_load_plugins(
            self, mock_import_module, mock_iter_modules):
        mock_import_module.side_effect = [
            'finder1', 'finder2',
        ]
        mock_iter_modules.return_value = [
            ('finder1', 'fsl_sub_1', True, ),
            ('finder2', 'fsl_sub_2', True, ),
            ('nothing', 'notfsl', True, ),
            ]
        self.assertDictEqual(
            fsl_sub.load_plugins(),
            {'fsl_sub_1': 'finder1',
             'fsl_sub_2': 'finder2', }
        )


if __name__ == '__main__':
    unittest.main()
