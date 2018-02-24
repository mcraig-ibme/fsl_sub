#!/usr/bin/env python
import unittest
from unittest.mock import patch
import fsl_sub.utils


class TestPlugins(unittest.TestCase):
    @patch('fsl_sub.utils.pkgutil.iter_modules', auto_spec=True)
    @patch('fsl_sub.utils.importlib.import_module', auto_spec=True)
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
            fsl_sub.utils.load_plugins(),
            {'fsl_sub_1': 'finder1',
             'fsl_sub_2': 'finder2', }
        )


class TestUtils(unittest.TestCase):
    def test_minutes_to_human(self):
        with self.subTest('Test 1'):
            self.assertEqual(
                fsl_sub.utils.minutes_to_human(10),
                '10m'
            )
        with self.subTest('Test 2'):
            self.assertEqual(
                fsl_sub.utils.minutes_to_human(23 * 60),
                '23h'
            )
        with self.subTest('Test 3'):
            self.assertEqual(
                fsl_sub.utils.minutes_to_human(48 * 60),
                '2d'
            )
        with self.subTest('Test 4'):
            self.assertEqual(
                fsl_sub.utils.minutes_to_human(23 * 59),
                '22.6h'
            )
        with self.subTest('Test 5'):
            self.assertEqual(
                fsl_sub.utils.minutes_to_human(48 * 58),
                '1.9d'
            )

    @patch.dict(
        'fsl_sub.utils.os.environ',
        {},
        clear=True)
    def test_control_threads(self):
        fsl_sub.utils.control_threads(
                ['THREADS', 'MORETHREADS', ],
                1)
        self.assertDictEqual(
            dict(fsl_sub.utils.os.environ),
            {'THREADS': '1', 'MORETHREADS': '1'}
        )

    @patch('fsl_sub.utils.shutil.which')
    def test_check_command(self, mock_which):
        mock_which.return_value = None
        self.assertRaises(
            fsl_sub.utils.CommandError,
            fsl_sub.utils.check_command, 'acommand'
        )

    @patch('fsl_sub.utils.check_command')
    def test_check_command_file(
            self, mock_check_command):
        with patch(
                'fsl_sub.utils.open',
                unittest.mock.mock_open(read_data='A')):
            self.assertEqual(
                fsl_sub.utils.check_command_file('afile'),
                1
            )
        with patch(
                'fsl_sub.utils.open',
                unittest.mock.mock_open(read_data='A')):
            mock_check_command.side_effect = fsl_sub.utils.CommandError()
            self.assertRaises(
                fsl_sub.utils.CommandError,
                fsl_sub.utils.check_command_file,
                'afile'
            )


if __name__ == '__main__':
    unittest.main()
