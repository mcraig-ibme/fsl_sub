#!/usr/bin/env python
import unittest
import fsl_sub.shell_modules
import subprocess
from unittest.mock import patch


class TestModuleSupport(unittest.TestCase):
    @patch('fsl_sub.shell_modules.shutil.which', auto_spec=True)
    def test_find_module_cmd(self, mock_which):
        mock_which.return_value = '/usr/bin/modulecmd'
        self.assertEqual(
            fsl_sub.shell_modules.find_module_cmd(),
            '/usr/bin/modulecmd')
        mock_which.assert_called_once_with('modulecmd')
        mock_which.reset_mock()
        with patch(
                'fsl_sub.shell_modules.read_config',
                return_value={'modulecmd': '/opt/bin/modulecmd', },
                autospec=True):
            mock_which.return_value = '/opt/bin/modulecmd'
            self.assertEqual(
                fsl_sub.shell_modules.find_module_cmd(),
                '/opt/bin/modulecmd')
            mock_which.assert_called_once_with('modulecmd')
        mock_which.reset_mock()
        with patch(
                'fsl_sub.shell_modules.read_config',
                return_value={'modulecmd': '/usr/local/bin/modulecmd', },
                autospec=True):
            mock_which.return_value = None
            self.assertEqual(
                fsl_sub.shell_modules.find_module_cmd(),
                '/usr/local/bin/modulecmd'
            )
        mock_which.reset_mock()
        with patch(
                'fsl_sub.shell_modules.read_config',
                return_value={'modulecmd': None, },
                autospec=True):
            mock_which.return_value = None
            self.assertFalse(
                fsl_sub.shell_modules.find_module_cmd()
            )

    def test_read_module_environment(self):
        lines = [
            "os.environ['PATH']='/usr/bin:/usr/sbin:/usr/local/bin'",
            "os.environ['LD_LIBRARY_PATH']='/usr/lib64:/usr/local/lib64'",
        ]
        self.assertDictEqual(
            fsl_sub.shell_modules.read_module_environment(lines),
            {'PATH': '/usr/bin:/usr/sbin:/usr/local/bin',
             'LD_LIBRARY_PATH': '/usr/lib64:/usr/local/lib64', }
        )

    @patch('fsl_sub.shell_modules.find_module_cmd', auto_spec=True)
    @patch('fsl_sub.shell_modules.system_stdout', auto_spec=True)
    @patch('fsl_sub.shell_modules.read_module_environment', auto_spec=True)
    def test_module_add(
            self,
            mock_read_module_environment,
            mock_system_stdout,
            mock_find_module_cmd):
        mcmd = '/usr/bin/modulecmd'
        mock_system_stdout.return_value = [
            "os.environ['PATH']='/usr/bin:/usr/sbin:/usr/local/bin'",
            "os.environ['LD_LIBRARY_PATH']='/usr/lib64:/usr/local/lib64'"
            ]
        mock_find_module_cmd.return_value = mcmd
        mock_read_module_environment.return_value = 'some text'
        with self.subTest('Test 1'):
            self.assertEqual(
                fsl_sub.shell_modules.module_add('amodule'),
                'some text'
            )
            mock_find_module_cmd.assert_called_once_with()
            mock_read_module_environment.assert_called_once_with([
                "os.environ['PATH']='/usr/bin:/usr/sbin:/usr/local/bin'",
                "os.environ['LD_LIBRARY_PATH']='/usr/lib64:/usr/local/lib64'"
                ]
            )
            mock_system_stdout.assert_called_once_with(
                [mcmd, "python", "add", 'amodule', ])
        with self.subTest('Test 2'):
            mock_system_stdout.side_effect = subprocess.CalledProcessError(
                'acmd', 1)
            self.assertRaises(
                fsl_sub.shell_modules.LoadModuleError,
                fsl_sub.shell_modules.module_add,
                'amodule')

        with self.subTest('Test 3'):
            mock_find_module_cmd.return_value = ''
            self.assertFalse(
                fsl_sub.shell_modules.module_add('amodule')
            )

    @patch('fsl_sub.shell_modules.module_add', auto_spec=True)
    @patch.dict('fsl_sub.shell_modules.os.environ', {}, clear=True)
    def test_load_module(self, mock_module_add):
        mock_module_add.return_value = {'VAR': 'VAL', 'VAR2': 'VAL2', }
        with self.subTest('Test 1'):
            self.assertTrue(
                fsl_sub.shell_modules.load_module('amodule'))
            self.assertDictEqual(
                dict(fsl_sub.shell_modules.os.environ),
                {'VAR': 'VAL', 'VAR2': 'VAL2', }
            )
        with self.subTest('Test 2'):
            mock_module_add.return_value = {}
            self.assertFalse(
                fsl_sub.shell_modules.load_module('amodule'))

    @patch('fsl_sub.shell_modules.module_add', auto_spec=True)
    @patch.dict(
        'fsl_sub.shell_modules.os.environ',
        {'VAR': 'VAL', 'VAR2': 'VAL2', 'EXISTING': 'VALUE', },
        clear=True)
    def test_unload_module(self, mock_module_add):
        mock_module_add.return_value = {'VAR': 'VAL', 'VAR2': 'VAL2', }
        with self.subTest('Test 1'):
            self.assertTrue(
                fsl_sub.shell_modules.unload_module('amodule'))
            self.assertDictEqual(
                dict(fsl_sub.shell_modules.os.environ),
                {'EXISTING': 'VALUE', }
            )
        with self.subTest('Test 2'):
            mock_module_add.return_value = {}
            self.assertFalse(
                fsl_sub.shell_modules.unload_module('amodule'))

    @patch.dict(
        'fsl_sub.shell_modules.os.environ',
        {'LOADEDMODULES': 'mod1:mod2:mod3', 'EXISTING': 'VALUE', },
        clear=True)
    def test_loaded_modules(self):
        with self.subTest('Test 1'):
            self.assertListEqual(
                fsl_sub.shell_modules.loaded_modules(),
                ['mod1', 'mod2', 'mod3', ])
        with self.subTest('Test 2'):
            with patch.dict(
                    'fsl_sub.shell_modules.os.environ',
                    {'EXISTING': 'VALUE', },
                    clear=True):
                self.assertListEqual(
                    fsl_sub.shell_modules.loaded_modules(),
                    [])

    @patch('fsl_sub.shell_modules.system_stderr', auto_spec=True)
    def test_get_modules(self, mock_system_stderr):
        mock_system_stderr.return_value = [
            "/usr/local/etc/ShellModules:",
            "amodule/5.0",
            "amodule/5.5",
            "/usr/share/Modules/modulefiles:",
            "/etc/modulefiles:",
        ]
        with self.subTest('Test 1'):
            self.assertListEqual(
                fsl_sub.shell_modules.get_modules('amodule'),
                ['5.0', '5.5', ])
        with self.subTest('Test 1b'):
            mock_system_stderr.reset_mock()
            mock_system_stderr.return_value = [
                "/usr/local/etc/ShellModules:",
                "bmodule",
            ]
            self.assertListEqual(
                fsl_sub.shell_modules.get_modules('bmodule'),
                ['bmodule', ])
        mock_system_stderr.reset_mock()
        with self.subTest('Test 2'):
            mock_system_stderr.side_effect = subprocess.CalledProcessError(
                'acmd', 1
            )
            self.assertRaises(
                fsl_sub.shell_modules.NoModule,
                fsl_sub.shell_modules.get_modules, 'amodule')
        mock_system_stderr.reset_mock()
        mock_system_stderr.return_value = ''
        with self.subTest('Test 3'):
            self.assertRaises(
                fsl_sub.shell_modules.NoModule,
                fsl_sub.shell_modules.get_modules, 'amodule')

    @patch('fsl_sub.shell_modules.get_modules', auto_spec=True)
    def test_latest_module(self, mock_get_modules):
        with self.subTest('Test 1'):
            mock_get_modules.return_value = ['5.0', '5.5', ]
            self.assertEqual(
                fsl_sub.shell_modules.latest_module('amodule'),
                '5.5')
        with self.subTest('Test 2'):
            mock_get_modules.return_value = None
            self.assertFalse(
                fsl_sub.shell_modules.latest_module('amodule')
            )
        with self.subTest('Test 3'):
            mock_get_modules.side_effect = fsl_sub.shell_modules.NoModule(
                'amodule')
            self.assertRaises(
                fsl_sub.shell_modules.NoModule,
                fsl_sub.shell_modules.latest_module, 'amodule'
            )

    def test_module_string(self):
        with self.subTest('Test 1'):
            self.assertEqual(
                fsl_sub.shell_modules.module_string('amodule', '5.0'),
                'amodule/5.0'
            )
        with self.subTest('Test 2'):
            self.assertEqual(
                fsl_sub.shell_modules.module_string('amodule', None),
                'amodule'
            )


if __name__ == '__main__':
    unittest.main()
