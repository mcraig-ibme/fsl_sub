#!/usr/bin/env python
import unittest
import config

from unittest.mock import patch


class TestConfig(unittest.TestCase):
    @patch('config.os.path.expanduser')
    @patch('config.os.path.exists')
    def test_find_config_file(
            self, mock_exists, mock_expanduser):
        with self.subTest('Expand user'):
            mock_exists.side_effect = [True]
            mock_expanduser.return_value = '/home/auser'
            self.assertEqual(
                config.find_config_file(),
                '/home/auser/.fsl_sub.yml'
            )
        mock_exists.reset_mock()
        with patch.dict(
                'config.os.environ',
                {'FSLDIR': '/usr/local/fsl', },
                clear=True):
            with self.subTest('FSLDIR'):
                mock_exists.side_effect = [False, True]
                self.assertEqual(
                    config.find_config_file(),
                    '/usr/local/fsl/etc/fslconf/fsl_sub.yml'
                )
        mock_exists.reset_mock()
        with self.subTest('Missing configuration'):
            mock_exists.side_effect = [False, False]
            self.assertRaises(
                config.BadConfiguration,
                config.find_config_file
            )
        mock_exists.reset_mock()
        mock_exists.side_effect = [False, False]
        with self.subTest('Environment variable'):
            with patch.dict(
                    'config.os.environ',
                    {'FSLSUB_CONF': '/etc/fsl_sub.yml', },
                    clear=True):
                self.assertEqual(
                    config.find_config_file(),
                    '/etc/fsl_sub.yml'
                )

    @patch('config.find_config_file')
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
                    'config.open',
                    unittest.mock.mock_open(read_data=example_yaml)) as m:
                self.assertDictEqual(
                    config.read_config(),
                    {'adict': {
                        'alist': [1, 2],
                        'astring': 'hello',
                    }}
                )
                m.assert_called_once_with('/etc/fsl_sub.conf', 'r')
        with self.subTest("Test bad read"):
            config.read_config.cache_clear()
            bad_yaml = "unbalanced: ]["
            with patch(
                    'config.open',
                    unittest.mock.mock_open(read_data=bad_yaml)) as m:
                self.assertRaises(
                    config.BadConfiguration,
                    config.read_config)

    @patch('config.read_config')
    def test_method_config(self, mock_read_config):
        config.method_config.cache_clear()
        with self.subTest('Test 1'):
            mock_read_config.return_value = {
                'method_opts': {'method': 'config', }, }
            self.assertEqual('config', config.method_config('method'))
        config.method_config.cache_clear()
        with self.subTest('Test 2'):
            self.assertRaises(
                TypeError,
                config.method_config
            )
        config.method_config.cache_clear()
        with self.subTest('Test 3'):
            mock_read_config.return_value = {
                'method_o': {'method': 'config', }, }
            with self.assertRaises(config.BadConfiguration) as me:
                config.method_config('method')
                self.assertEqual(
                    me.msg, "Unable to find method configuration dictionary")
        config.method_config.cache_clear()
        with self.subTest('Test 4'):
            mock_read_config.return_value = {
                'method_opts': {'method': 'config', }, }
            self.assertEqual('config', config.method_config('method'))
            with self.assertRaises(config.BadConfiguration) as me:
                config.method_config('method2')
                self.assertEqual(
                    me.msg, "Unable to find configuration for method")

    @patch('config.read_config')
    def test_coprocessor_config(self, mock_read_config):
        config.coprocessor_config.cache_clear()
        with self.subTest('Test 1'):
            mock_read_config.return_value = {
                'coproc_opts': {'cuda': 'option', }, }
            self.assertEqual(
                'option', config.coprocessor_config('cuda'))
        config.coprocessor_config.cache_clear()
        with self.subTest('Test 2'):
            self.assertRaises(
                TypeError,
                config.coprocessor_config
            )
        config.coprocessor_config.cache_clear()
        with self.subTest('Test 3'):
            mock_read_config.return_value = {
                'coproc_o': {'cuda': 'option', }, }
            with self.assertRaises(config.BadConfiguration) as me:
                config.coprocessor_config('cuda')
                self.assertEqual(
                    me.msg,
                    "Unable to find coprocessor configuration dictionary")
        config.coprocessor_config.cache_clear()
        with self.subTest('Test 4'):
            mock_read_config.return_value = {
                'coproc_opts': {'cuda': 'option', }, }
            with self.assertRaises(config.BadConfiguration) as me:
                config.coprocessor_config('phi')
                self.assertEqual(
                    me.msg, "Unable to find configuration for phi")

    @patch('config.read_config')
    def test_queue_config(self, mock_read_config):
        config.queue_config.cache_clear()
        mock_read_config.return_value = {
            'queues': {'short.q': 'option', }, }
        with self.subTest('Test 1'):
            self.assertEqual(
                'option', config.queue_config('short.q'))
        config.queue_config.cache_clear()
        with self.subTest('Test 2'):
            self.assertDictEqual(
                {'short.q': 'option', },
                config.queue_config())
        config.queue_config.cache_clear()
        with self.subTest('Test 3'):
            with self.assertRaises(config.BadConfiguration) as me:
                config.queue_config('long.q')
                self.assertEqual(
                    me.msg,
                    "Unable to find definition for queue long.q")
        config.queue_config.cache_clear()
        with self.subTest('Test 4'):
            mock_read_config.return_value = {
                'q': {'short.q': 'option', }, }
            with self.assertRaises(config.BadConfiguration) as me:
                config.queue_config()
                self.assertEqual(
                    me.msg,
                    "Unable to find queue definitions")


if __name__ == '__main__':
    unittest.main()
