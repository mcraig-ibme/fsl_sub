#!/usr/bin/env python
import unittest
import utils

from unittest.mock import patch


class TestPlugins(unittest.TestCase):
    @patch('utils.pkgutil.iter_modules', auto_spec=True)
    @patch('utils.importlib.import_module', auto_spec=True)
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
            utils.load_plugins(),
            {'fsl_sub_1': 'finder1',
             'fsl_sub_2': 'finder2', }
        )


class TestUtils(unittest.TestCase):
    def test_minutes_to_human(self):
        with self.subTest('Test 1'):
            self.assertEqual(
                utils.minutes_to_human(10),
                '10m'
            )
        with self.subTest('Test 2'):
            self.assertEqual(
                utils.minutes_to_human(23 * 60),
                '23h'
            )
        with self.subTest('Test 3'):
            self.assertEqual(
                utils.minutes_to_human(48 * 60),
                '2d'
            )
        with self.subTest('Test 4'):
            self.assertEqual(
                utils.minutes_to_human(23 * 59),
                '22.6h'
            )
        with self.subTest('Test 5'):
            self.assertEqual(
                utils.minutes_to_human(48 * 58),
                '1.9d'
            )

    @patch.dict(
        'utils.os.environ',
        {},
        clear=True)
    def test_control_threads(self):
        utils.control_threads(
                ['THREADS', 'MORETHREADS', ],
                1)
        self.assertDictEqual(
            dict(utils.os.environ),
            {'THREADS': '1', 'MORETHREADS': '1'}
        )

    @patch('utils.shutil.which')
    def test_check_command(self, mock_which):
        mock_which.return_value = None
        self.assertRaises(
            utils.CommandError,
            utils.check_command, 'acommand'
        )

    @patch('utils.check_command')
    def test_check_command_file(
            self, mock_check_command):
        with patch(
                'utils.open',
                unittest.mock.mock_open(read_data='A')):
            self.assertEqual(
                utils.check_command_file('afile'),
                1
            )
        with patch(
                'utils.open',
                unittest.mock.mock_open(read_data='A')):
            mock_check_command.side_effect = utils.CommandError()
            self.assertRaises(
                utils.CommandError,
                utils.check_command_file,
                'afile'
            )


if __name__ == '__main__':
    unittest.main()
