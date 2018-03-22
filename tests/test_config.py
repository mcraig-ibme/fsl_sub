#!/usr/bin/env python
import unittest
import fsl_sub.config

from unittest.mock import patch


class TestConfig(unittest.TestCase):
    @patch('fsl_sub.config.os.path.expanduser', autospec=True)
    @patch('fsl_sub.config.os.path.exists', autospec=True)
    def test_find_config_file(
            self, mock_exists, mock_expanduser):
        with self.subTest('Expand user'):
            mock_exists.side_effect = [True]
            mock_expanduser.return_value = '/home/auser'
            self.assertEqual(
                fsl_sub.config.find_config_file(),
                '/home/auser/.fsl_sub.yml'
            )
        mock_exists.reset_mock()
        with patch.dict(
                'fsl_sub.config.os.environ',
                {'FSLDIR': '/usr/local/fsl', },
                clear=True):
            with self.subTest('FSLDIR'):
                mock_exists.side_effect = [False, True]
                self.assertEqual(
                    fsl_sub.config.find_config_file(),
                    '/usr/local/fsl/etc/fslconf/fsl_sub.yml'
                )
        mock_exists.reset_mock()
        with self.subTest('Missing configuration'):
            mock_exists.side_effect = [False, False]
            self.assertRaises(
                fsl_sub.config.BadConfiguration,
                fsl_sub.config.find_config_file
            )
        mock_exists.reset_mock()
        mock_exists.side_effect = [False, False]
        with self.subTest('Environment variable'):
            with patch.dict(
                    'fsl_sub.config.os.environ',
                    {'FSLSUB_CONF': '/etc/fsl_sub.yml', },
                    clear=True):
                self.assertEqual(
                    fsl_sub.config.find_config_file(),
                    '/etc/fsl_sub.yml'
                )

    @patch('fsl_sub.config.find_config_file', auto_spec=True)
    def test_read_config(self, mock_find_config_file):
        with self.subTest("Test good read"):
            example_yaml = '''
    adict:
        alist:
            - 1
            - 2
        astring: hello
    '''
            mock_find_config_file.return_value = '/etc/fsl_sub.conf'
            with patch(
                    'fsl_sub.config.open',
                    unittest.mock.mock_open(read_data=example_yaml)) as m:
                self.assertDictEqual(
                    fsl_sub.config.read_config(),
                    {'adict': {
                        'alist': [1, 2],
                        'astring': 'hello',
                    }}
                )
                m.assert_called_once_with('/etc/fsl_sub.conf', 'r')
        with self.subTest("Test bad read"):
            fsl_sub.config.read_config.cache_clear()
            bad_yaml = "unbalanced: ]["
            with patch(
                    'fsl_sub.config.open',
                    unittest.mock.mock_open(read_data=bad_yaml)) as m:
                self.assertRaises(
                    fsl_sub.config.BadConfiguration,
                    fsl_sub.config.read_config)

    @patch('fsl_sub.config.read_config', autospec=True)
    def test_method_config(self, mock_read_config):
        fsl_sub.config.method_config.cache_clear()
        with self.subTest('Test 1'):
            mock_read_config.return_value = {
                'method_opts': {'method': 'config', }, }
            self.assertEqual('config', fsl_sub.config.method_config('method'))
        fsl_sub.config.method_config.cache_clear()
        with self.subTest('Test 2'):
            self.assertRaises(
                TypeError,
                fsl_sub.config.method_config
            )
        fsl_sub.config.method_config.cache_clear()
        with self.subTest('Test 3'):
            mock_read_config.return_value = {
                'method_o': {'method': 'config', }, }
            with self.assertRaises(fsl_sub.config.BadConfiguration) as me:
                fsl_sub.config.method_config('method')
            self.assertEqual(
                me.exception.args[0],
                "Unable to find method configuration dictionary")
        fsl_sub.config.method_config.cache_clear()
        with self.subTest('Test 4'):
            mock_read_config.return_value = {
                'method_opts': {'method': 'config', }, }
            self.assertEqual('config', fsl_sub.config.method_config('method'))
            with self.assertRaises(fsl_sub.config.BadConfiguration) as me:
                fsl_sub.config.method_config('method2')
            self.assertEqual(
                me.exception.args[0],
                "Unable to find configuration for method2")

    @patch('fsl_sub.config.read_config', autospec=True)
    def test_coprocessor_config(self, mock_read_config):
        fsl_sub.config.coprocessor_config.cache_clear()
        with self.subTest('Test 1'):
            mock_read_config.return_value = {
                'coproc_opts': {'cuda': 'option', }, }
            self.assertEqual(
                'option', fsl_sub.config.coprocessor_config('cuda'))
        fsl_sub.config.coprocessor_config.cache_clear()
        with self.subTest('Test 2'):
            self.assertRaises(
                TypeError,
                fsl_sub.config.coprocessor_config
            )
        fsl_sub.config.coprocessor_config.cache_clear()
        with self.subTest('Test 3'):
            mock_read_config.return_value = {
                'coproc_o': {'cuda': 'option', }, }
            with self.assertRaises(fsl_sub.config.BadConfiguration) as me:
                fsl_sub.config.coprocessor_config('cuda')
            self.assertEqual(
                me.exception.args[0],
                "Unable to find coprocessor configuration dictionary")
        fsl_sub.config.coprocessor_config.cache_clear()
        with self.subTest('Test 4'):
            mock_read_config.return_value = {
                'coproc_opts': {'cuda': 'option', }, }
            with self.assertRaises(fsl_sub.config.BadConfiguration) as me:
                fsl_sub.config.coprocessor_config('phi')
            self.assertEqual(
                me.exception.args[0],
                "Unable to find configuration for phi")

    @patch('fsl_sub.config.read_config', autospec=True)
    def test_queue_config(self, mock_read_config):
        fsl_sub.config.queue_config.cache_clear()
        mock_read_config.return_value = {
            'queues': {'short.q': 'option', }, }
        with self.subTest('Test 1'):
            self.assertEqual(
                'option', fsl_sub.config.queue_config('short.q'))
        fsl_sub.config.queue_config.cache_clear()
        with self.subTest('Test 2'):
            self.assertDictEqual(
                {'short.q': 'option', },
                fsl_sub.config.queue_config())
        fsl_sub.config.queue_config.cache_clear()
        with self.subTest('Test 3'):
            with self.assertRaises(fsl_sub.config.BadConfiguration) as me:
                fsl_sub.config.queue_config('long.q')
            self.assertEqual(
                    me.exception.args[0],
                    "Unable to find definition for queue long.q")
        fsl_sub.config.queue_config.cache_clear()
        with self.subTest('Test 4'):
            mock_read_config.return_value = {
                'q': {'short.q': 'option', }, }
            with self.assertRaises(fsl_sub.config.BadConfiguration) as me:
                fsl_sub.config.queue_config()
            self.assertEqual(
                    me.exception.args[0],
                    "Unable to find queue definitions")


if __name__ == '__main__':
    unittest.main()
