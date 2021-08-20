#!/usr/bin/env python
import unittest
import fsl_sub.shell_modules
import subprocess
from unittest.mock import patch


class TestModuleSupport(unittest.TestCase):
    def setUp(self):
        fsl_sub.shell_modules.get_modules.cache_clear()

    @patch('fsl_sub.shell_modules.shutil.which', autospec=True)
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

    @patch('fsl_sub.shell_modules.find_module_cmd', autospec=True)
    @patch('fsl_sub.shell_modules.system_stdout', autospec=True)
    @patch('fsl_sub.shell_modules.read_module_environment', autospec=True)
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
            mock_read_module_environment.assert_called_once_with(
                [
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

    @patch('fsl_sub.shell_modules.module_add', autospec=True)
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
        with self.subTest("Add to loadedmodules"):
            mock_module_add.return_value = {'LOADEDMODULES': 'bmodule/1.2.3', 'VAR': 'VAL', 'VAR2': 'VAL2', }
            self.assertTrue(
                fsl_sub.shell_modules.load_module('amodule'))
            self.assertDictEqual(
                dict(fsl_sub.shell_modules.os.environ),
                {'LOADEDMODULES': 'bmodule/1.2.3', 'VAR': 'VAL', 'VAR2': 'VAL2', }
            )

    @patch('fsl_sub.shell_modules.module_add', autospec=True)
    def test_load_module2(self, mock_module_add):
        with self.subTest("Add to loadedmodules 2"):
            with patch.dict('fsl_sub.shell_modules.os.environ', {'LOADEDMODULES': 'amodule/2.3.4', }, clear=True):
                mock_module_add.return_value = {'LOADEDMODULES': 'bmodule/1.2.3', 'VAR': 'VAL', 'VAR2': 'VAL2', }
                self.assertTrue(
                    fsl_sub.shell_modules.load_module('bmodule'))
                self.assertDictEqual(
                    dict(fsl_sub.shell_modules.os.environ),
                    {'LOADEDMODULES': 'amodule/2.3.4:bmodule/1.2.3', 'VAR': 'VAL', 'VAR2': 'VAL2', }
                )

    @patch('fsl_sub.shell_modules.module_add', autospec=True)
    def test_unload_module(self, mock_module_add):
        with self.subTest("Unload modules 2"):
            with patch.dict(
                    'fsl_sub.shell_modules.os.environ', {
                        'LOADEDMODULES': 'amodule/2.3.4:bmodule/1.2.3', 'VAR': 'VAL', 'VAR2': 'VAL2', },
                    clear=True):
                mock_module_add.return_value = {'LOADEDMODULES': 'bmodule/1.2.3', 'VAR': 'VAL', 'VAR2': 'VAL2', }
                self.assertTrue(
                    fsl_sub.shell_modules.unload_module('bmodule'))
                self.assertDictEqual(
                    dict(fsl_sub.shell_modules.os.environ),
                    {'LOADEDMODULES': 'amodule/2.3.4', }
                )
        with self.subTest("Unload modules 3"):
            with patch.dict(
                    'fsl_sub.shell_modules.os.environ', {
                        'LOADEDMODULES': 'amodule/2.3.4:bmodule/1.2.3', 'VAR': 'VAL', 'VAR2': 'VAL2', },
                    clear=True):
                mock_module_add.return_value = {'LOADEDMODULES': 'amodule/2.3.4', 'VAR': 'VAL', 'VAR2': 'VAL2', }
                self.assertTrue(
                    fsl_sub.shell_modules.unload_module('amodule'))
                self.assertDictEqual(
                    dict(fsl_sub.shell_modules.os.environ),
                    {'LOADEDMODULES': 'bmodule/1.2.3', }
                )
        with self.subTest("Unload modules 4"):
            with patch.dict(
                    'fsl_sub.shell_modules.os.environ', {
                        'LOADEDMODULES': 'bmodule/1.2.3:amodule/2.3.4:', 'VAR': 'VAL', 'VAR2': 'VAL2', },
                    clear=True):
                mock_module_add.return_value = {'LOADEDMODULES': 'amodule/2.3.4', 'VAR': 'VAL', 'VAR2': 'VAL2', }
                self.assertTrue(
                    fsl_sub.shell_modules.unload_module('amodule'))
                self.assertDictEqual(
                    dict(fsl_sub.shell_modules.os.environ),
                    {'LOADEDMODULES': 'bmodule/1.2.3', }
                )

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

    @patch('fsl_sub.shell_modules.system_stderr', autospec=True)
    def test_get_modules(self, mock_system_stderr):
        mock_system_stderr.return_value = [
            "/usr/local/etc/ShellModules:",
            "amodule/5.0",
            "amodule/5.5",
            "/usr/share/Modules/modulefiles:",
            "/etc/modulefiles:",
        ]
        fsl_sub.shell_modules.get_modules.cache_clear()
        with self.subTest('Test 1'):
            self.assertListEqual(
                fsl_sub.shell_modules.get_modules('amodule'),
                ['5.0', '5.5', ])
        fsl_sub.shell_modules.get_modules.cache_clear()
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
        fsl_sub.shell_modules.get_modules.cache_clear()
        with self.subTest('Module parent with /'):
            mock_system_stderr.reset_mock()
            mock_system_stderr.return_value = [
                "/usr/local/etc/ShellModules:",
                "bmodule/submodule/version",
            ]
            self.assertListEqual(
                fsl_sub.shell_modules.get_modules('bmodule/submodule'),
                ['version', ]
            )
        mock_system_stderr.reset_mock()
        mock_system_stderr.return_value = ''
        fsl_sub.shell_modules.get_modules.cache_clear()
        with self.subTest('Test long lines'):
            mock_system_stderr.return_value = [
                '''------------------------------------------'''
                + '''--------------------------------------------- '''
                + '''/apps/system/easybuild/modules/all'''
                + ''' ---------------------------------------------'''
                + '''-------------------------------------------''',
                '''   Amodule/1.2.3                              '''
                + '''                         Amodule/2.14         '''
                + '''                          Amodule/7.3.0''',
                '''   Amodule/2.1.5                              '''
                + '''                         Amodule/2.13.03      '''
                + '''                          Amodule/2.13.1''']
            self.assertListEqual(
                fsl_sub.shell_modules.get_modules('Amodule'),
                ['1.2.3', '2.1.5', '2.13.03', '2.13.1', '2.14', '7.3.0', ]
            )
        mock_system_stderr.reset_mock()
        mock_system_stderr.return_value = ''
        fsl_sub.shell_modules.get_modules.cache_clear()

        with self.subTest('Test module parent within name'):
            mock_system_stderr.return_value = [
                'Amodule/1.2.3',
                'Bmodule/3.2.1',
                'Cmodule-Amodule-Bmodule/2.3.4',
                'Cmodule-Amodule/3.4.5']
            self.assertListEqual(
                fsl_sub.shell_modules.get_modules('Amodule'),
                ['1.2.3']
            )
        mock_system_stderr.reset_mock()
        mock_system_stderr.return_value = ''
        fsl_sub.shell_modules.get_modules.cache_clear()
        with self.subTest('Test module parent within name 2'):
            mock_system_stderr.return_value = [
                'Amodule/1.2.3',
                'AmoduleA/3.2.1', ]
            self.assertListEqual(
                fsl_sub.shell_modules.get_modules('Amodule'),
                ['1.2.3']
            )
        mock_system_stderr.reset_mock()
        mock_system_stderr.return_value = ''
        fsl_sub.shell_modules.get_modules.cache_clear()
        with self.subTest('Test module parent within name 3'):
            mock_system_stderr.return_value = [
                'Amodule/1.2.3',
                'Amodule-3.2.1',
                'Amodule/submodule/4.3.2',
                'Amodulesubmodule/5.4.3',
            ]
            self.assertListEqual(
                fsl_sub.shell_modules.get_modules('Amodule'),
                ['1.2.3', '4.3.2', 'Amodule-3.2.1', ]
            )
        mock_system_stderr.reset_mock()
        mock_system_stderr.return_value = ''
        fsl_sub.shell_modules.get_modules.cache_clear()
        with self.subTest('Test 2'):
            mock_system_stderr.side_effect = subprocess.CalledProcessError(
                'acmd', 1
            )
            self.assertRaises(
                fsl_sub.shell_modules.NoModule,
                fsl_sub.shell_modules.get_modules, 'amodule')
        mock_system_stderr.reset_mock()
        mock_system_stderr.return_value = ''
        fsl_sub.shell_modules.get_modules.cache_clear()
        with self.subTest('Test 3'):
            self.assertRaises(
                fsl_sub.shell_modules.NoModule,
                fsl_sub.shell_modules.get_modules, 'amodule')

    @patch('fsl_sub.shell_modules.get_modules', autospec=True)
    def test_latest_module(self, mock_get_modules):
        with self.subTest('Test 1'):
            mock_get_modules.return_value = ['5.0', '5.5', ]
            self.assertEqual(
                fsl_sub.shell_modules.latest_module('amodule'),
                '5.5')
        fsl_sub.shell_modules.get_modules.cache_clear()
        with self.subTest('Test 2'):
            mock_get_modules.return_value = None
            self.assertFalse(
                fsl_sub.shell_modules.latest_module('amodule')
            )
        fsl_sub.shell_modules.get_modules.cache_clear()
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
