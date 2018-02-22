import unittest
import shell_modules
import subprocess
from unittest.mock import patch


class TestModuleSupport(unittest.TestCase):
    @patch('shell_modules.shutil.which', auto_spec=True)
    def test_find_module_cmd(self, mock_which):
        mock_which.return_value = '/usr/bin/modulecmd'
        self.assertEqual(
            shell_modules.find_module_cmd(),
            '/usr/bin/modulecmd')
        mock_which.assert_called_once_with('modulecmd')

    def test_read_module_environment(self):
        lines = [
            "os.environ['PATH']='/usr/bin:/usr/sbin:/usr/local/bin'",
            "os.environ['LD_LIBRARY_PATH']='/usr/lib64:/usr/local/lib64'",
        ]
        self.assertDictEqual(
            shell_modules.read_module_environment(lines),
            {'PATH': '/usr/bin:/usr/sbin:/usr/local/bin',
             'LD_LIBRARY_PATH': '/usr/lib64:/usr/local/lib64', }
        )

    @patch('shell_modules.find_module_cmd', auto_spec=True)
    @patch('shell_modules.system_stdout', auto_spec=True)
    @patch('shell_modules.read_module_environment', auto_spec=True)
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
                shell_modules.module_add('amodule'),
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
                shell_modules.LoadModuleError,
                shell_modules.module_add,
                'amodule')

        with self.subTest('Test 3'):
            mock_find_module_cmd.return_value = ''
            self.assertFalse(
                shell_modules.module_add('amodule')
            )

    @patch('shell_modules.module_add', auto_spec=True)
    @patch.dict('shell_modules.os.environ', {}, clear=True)
    def test_load_module(self, mock_module_add):
        mock_module_add.return_value = {'VAR': 'VAL', 'VAR2': 'VAL2', }
        with self.subTest('Test 1'):
            self.assertTrue(
                shell_modules.load_module('amodule'))
            self.assertDictEqual(
                dict(shell_modules.os.environ),
                {'VAR': 'VAL', 'VAR2': 'VAL2', }
            )
        with self.subTest('Test 2'):
            mock_module_add.return_value = {}
            self.assertFalse(
                shell_modules.load_module('amodule'))

    @patch('shell_modules.module_add', auto_spec=True)
    @patch.dict(
        'shell_modules.os.environ',
        {'VAR': 'VAL', 'VAR2': 'VAL2', 'EXISTING': 'VALUE', },
        clear=True)
    def test_unload_module(self, mock_module_add):
        mock_module_add.return_value = {'VAR': 'VAL', 'VAR2': 'VAL2', }
        with self.subTest('Test 1'):
            self.assertTrue(
                shell_modules.unload_module('amodule'))
            self.assertDictEqual(
                dict(shell_modules.os.environ),
                {'EXISTING': 'VALUE', }
            )
        with self.subTest('Test 2'):
            mock_module_add.return_value = {}
            self.assertFalse(
                shell_modules.unload_module('amodule'))

    @patch.dict(
        'shell_modules.os.environ',
        {'LOADEDMODULES': 'mod1:mod2:mod3', 'EXISTING': 'VALUE', },
        clear=True)
    def test_loaded_modules(self):
        with self.subTest('Test 1'):
            self.assertListEqual(
                shell_modules.loaded_modules(),
                ['mod1', 'mod2', 'mod3', ])
        with self.subTest('Test 2'):
            with patch.dict(
                    'shell_modules.os.environ',
                    {'EXISTING': 'VALUE', },
                    clear=True):
                self.assertListEqual(
                    shell_modules.loaded_modules(),
                    [])

    @patch('shell_modules.system_stdout', auto_spec=True)
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
                shell_modules.get_modules('amodule'),
                ['5.0', '5.5', ])
        with self.subTest('Test 2'):
            mock_system_stdout.side_effect = subprocess.CalledProcessError(
                'acmd', 1
            )
            self.assertRaises(
                shell_modules.NoModule,
                shell_modules.get_modules, 'amodule')

    @patch('shell_modules.get_modules', auto_spec=True)
    def test_latest_module(self, mock_get_modules):
        with self.subTest('Test 1'):
            mock_get_modules.return_value = ['5.0', '5.5', ]
            self.assertEqual(
                shell_modules.latest_module('amodule'),
                '5.5')
        with self.subTest('Test 2'):
            mock_get_modules.return_value = None
            self.assertFalse(
                shell_modules.latest_module('amodule')
            )
        with self.subTest('Test 3'):
            mock_get_modules.side_effect = shell_modules.NoModule('amodule')
            self.assertRaises(
                shell_modules.NoModule,
                shell_modules.latest_module, 'amodule'
            )

    def test_module_string(self):
        with self.subTest('Test 1'):
            self.assertEqual(
                shell_modules.module_string('amodule', '5.0'),
                'amodule/5.0'
            )
        with self.subTest('Test 2'):
            self.assertEqual(
                shell_modules.module_string('amodule', None),
                'amodule'
            )


if __name__ == '__main__':
    unittest.main()
