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


class TestAffimativeNegative(unittest.TestCase):
    def test_affirmative(self):
        with self.subTest('yes'):
            self.assertTrue(
                fsl_sub.utils.affirmative('yes')
            )
        with self.subTest('y'):
            self.assertTrue(
                fsl_sub.utils.affirmative('y')
            )
        with self.subTest('true'):
            self.assertTrue(
                fsl_sub.utils.affirmative('true')
            )
        with self.subTest('YES'):
            self.assertTrue(
                fsl_sub.utils.affirmative('YES')
            )
        with self.subTest('Y'):
            self.assertTrue(
                fsl_sub.utils.affirmative('Y')
            )
        with self.subTest('True'):
            self.assertTrue(
                fsl_sub.utils.affirmative('True')
            )
        with self.subTest('TRue'):
            self.assertTrue(
                fsl_sub.utils.affirmative('TRue')
            )
        with self.subTest('TRUe'):
            self.assertTrue(
                fsl_sub.utils.affirmative('TRUe')
            )
        with self.subTest('TRUE'):
            self.assertTrue(
                fsl_sub.utils.affirmative('TRUE')
            )
        with self.subTest('False'):
            self.assertFalse(
                fsl_sub.utils.affirmative('False')
            )
        with self.subTest('Nothing'):
            self.assertFalse(
                fsl_sub.utils.affirmative('Nothing')
            )
        with self.subTest('n'):
            self.assertFalse(
                fsl_sub.utils.affirmative('n')
            )

    def test_negative(self):
        with self.subTest('no'):
            self.assertTrue(
                fsl_sub.utils.negative('no')
            )
        with self.subTest('n'):
            self.assertTrue(
                fsl_sub.utils.negative('n')
            )
        with self.subTest('false'):
            self.assertTrue(
                fsl_sub.utils.negative('false')
            )
        with self.subTest('NO'):
            self.assertTrue(
                fsl_sub.utils.negative('NO')
            )
        with self.subTest('N'):
            self.assertTrue(
                fsl_sub.utils.negative('N')
            )
        with self.subTest('False'):
            self.assertTrue(
                fsl_sub.utils.negative('False')
            )
        with self.subTest('FAlse'):
            self.assertTrue(
                fsl_sub.utils.negative('FAlse')
            )
        with self.subTest('FALse'):
            self.assertTrue(
                fsl_sub.utils.negative('FALse')
            )
        with self.subTest('FALSe'):
            self.assertTrue(
                fsl_sub.utils.negative('FALSe')
            )
        with self.subTest('FALSE'):
            self.assertTrue(
                fsl_sub.utils.negative('FALSE')
            )
        with self.subTest('True'):
            self.assertFalse(
                fsl_sub.utils.negative('True')
            )
        with self.subTest('Nothing'):
            self.assertFalse(
                fsl_sub.utils.negative('Nothing')
            )
        with self.subTest('y'):
            self.assertFalse(
                fsl_sub.utils.negative('y')
            )


class TestUtils(unittest.TestCase):
    def test_split_ram_by_slots(self):
        self.assertEqual(
            1,
            fsl_sub.utils.split_ram_by_slots(1, 1)
        )
        self.assertEqual(
            2,
            fsl_sub.utils.split_ram_by_slots(2, 1)
        )
        self.assertEqual(
            1,
            fsl_sub.utils.split_ram_by_slots(1, 2)
        )
        self.assertEqual(
            1,
            fsl_sub.utils.split_ram_by_slots(1, 3)
        )
        self.assertEqual(
            1,
            fsl_sub.utils.split_ram_by_slots(10, 11)
        )
        self.assertEqual(
            34,
            fsl_sub.utils.split_ram_by_slots(100, 3)
        )

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
        with patch(
                'fsl_sub.utils.open',
                unittest.mock.mock_open(read_data='A')):
            mock_check_command.side_effect = IOError('Oops')
            self.assertRaises(
                fsl_sub.utils.CommandError,
                fsl_sub.utils.check_command_file,
                'afile'
            )


if __name__ == '__main__':
    unittest.main()
