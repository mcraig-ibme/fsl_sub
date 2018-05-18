#!/usr/bin/env python
import subprocess
import sys
import unittest
from unittest.mock import patch
from fsl_sub.exceptions import CommandError
import fsl_sub.utils


class TestConversions(unittest.TestCase):
    def test_human_to_ram(self):
        with self.subTest('no units'):
            self.assertEqual(
                fsl_sub.utils.human_to_ram(10),
                10240
            )
        with self.subTest('Raises on bad unit specifier'):
            self.assertRaises(
                ValueError,
                fsl_sub.utils.human_to_ram,
                10,
                'H')
        with self.subTest('Raises on non-number'):
            self.assertRaises(
                ValueError,
                fsl_sub.utils.human_to_ram,
                "a",
                'H')
            self.assertRaises(
                ValueError,
                fsl_sub.utils.human_to_ram,
                "1..2",
                'H')
        with self.subTest('Raises on non-string units/output'):
            self.assertRaises(
                ValueError,
                fsl_sub.utils.human_to_ram,
                1,
                'T', 1)
            self.assertRaises(
                ValueError,
                fsl_sub.utils.human_to_ram,
                1,
                1, 'T')
        with self.subTest('TBs'):
            self.assertEqual(
                fsl_sub.utils.human_to_ram('10T'),
                10485760
            )
            self.assertEqual(
                fsl_sub.utils.human_to_ram(10, units='T'),
                10485760
            )
            self.assertEqual(
                fsl_sub.utils.human_to_ram('10TB'),
                10485760
            )
            self.assertEqual(
                fsl_sub.utils.human_to_ram('10Tb'),
                10485760
            )
            self.assertEqual(
                fsl_sub.utils.human_to_ram('10Ti'),
                10485760
            )
            self.assertEqual(
                fsl_sub.utils.human_to_ram('10Tib'),
                10485760
            )
            self.assertEqual(
                fsl_sub.utils.human_to_ram('10t'),
                10485760
            )
            self.assertEqual(
                fsl_sub.utils.human_to_ram('10ti'),
                10485760
            )
            self.assertEqual(
                fsl_sub.utils.human_to_ram('10tiB'),
                10485760
            )
        with self.subTest('PBs'):
            self.assertEqual(
                fsl_sub.utils.human_to_ram('10P'),
                10737418240
            )
        with self.subTest('PBs to GBs'):
            self.assertEqual(
                fsl_sub.utils.human_to_ram('10P', output='G'),
                10485760
            )
        with self.subTest('KBs to MBs'):
            self.assertEqual(
                fsl_sub.utils.human_to_ram('10K', output='M'),
                10 / 1024
            )
        with self.subTest('MBs to MBs'):
            self.assertEqual(
                fsl_sub.utils.human_to_ram('10M', output='M'),
                10
            )


class TestPlugins(unittest.TestCase):
    @patch('fsl_sub.utils.pkgutil.iter_modules', auto_spec=True)
    @patch('fsl_sub.utils.importlib.import_module', auto_spec=True)
    def test_load_plugins(
            self, mock_import_module, mock_iter_modules):
        mock_import_module.side_effect = [
            'finder1', 'finder2',
        ]
        mock_iter_modules.return_value = [
            ('finder1', 'fsl_sub_plugin_1', True, ),
            ('finder2', 'fsl_sub_plugin_2', True, ),
            ('nothing', 'notfsl', True, ),
            ]
        s_path = sys.path
        self.assertDictEqual(
            fsl_sub.utils.load_plugins(),
            {'fsl_sub_plugin_1': 'finder1',
             'fsl_sub_plugin_2': 'finder2', }
        )
        self.assertListEqual(
            s_path,
            sys.path
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


class TestFileIsImage(unittest.TestCase):
    @patch('fsl_sub.utils.os.path.isfile', autospec=True)
    @patch('fsl_sub.utils.system_stdout', autospec=True)
    def test_file_is_image(self, mock_sstdout, mock_isfile):
        mock_isfile.return_value = False
        self.assertFalse(fsl_sub.utils.file_is_image('a'))
        mock_isfile.return_value = True
        mock_sstdout.return_value = ['1', ]
        self.assertTrue(fsl_sub.utils.file_is_image('a'))
        mock_sstdout.return_value = ['0', ]
        self.assertFalse(fsl_sub.utils.file_is_image('a'))
        mock_sstdout.side_effect = subprocess.CalledProcessError(
            1, 'a', "failed")
        self.assertRaises(
            CommandError,
            fsl_sub.utils.file_is_image,
            'a'
        )


class TestArraySpec(unittest.TestCase):
    def test_parse_array_specifier(self):
        self.assertTupleEqual(
            fsl_sub.utils.parse_array_specifier('4'),
            (4, None, None)
        )
        self.assertTupleEqual(
            fsl_sub.utils.parse_array_specifier('1-3'),
            (1, 3, None)
        )
        self.assertTupleEqual(
            fsl_sub.utils.parse_array_specifier('4-8'),
            (4, 8, None)
        )
        self.assertTupleEqual(
            fsl_sub.utils.parse_array_specifier('1-4:2'),
            (1, 4, 2)
        )
        self.assertRaises(
            fsl_sub.utils.BadSubmission,
            fsl_sub.utils.parse_array_specifier,
            ''
        )
        self.assertRaises(
            fsl_sub.utils.BadSubmission,
            fsl_sub.utils.parse_array_specifier,
            'A'
        )
        self.assertRaises(
            fsl_sub.utils.BadSubmission,
            fsl_sub.utils.parse_array_specifier,
            '1-A'
        )
        self.assertRaises(
            fsl_sub.utils.BadSubmission,
            fsl_sub.utils.parse_array_specifier,
            '1-2:A'
        )


if __name__ == '__main__':
    unittest.main()
