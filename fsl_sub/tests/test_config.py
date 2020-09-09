#!/usr/bin/env python
import unittest
import fsl_sub.config
import os
import shutil
import tempfile

from unittest.mock import patch


class TestConfig(unittest.TestCase):
    def test__merge_dict(self):
        a = {'avalue': {'another': 'dict'}, 'bvalue': 1, 'cvalue': [0, 1, ]}
        b = {'dvalue': 1}
        c = {'cvalue': [2, 3, 4, ]}
        d = {'avalue': {'something': 'else', 'yetanother': 'value'}}
        e = {'avalue': {'another': 'item', 'yetanother': 'value'}}
        with self.subTest('Add key/value'):
            self.assertDictEqual(
                fsl_sub.config._merge_dict(a, b),
                {'avalue': {
                    'another': 'dict'}, 'bvalue': 1,
                    'cvalue': [0, 1], 'dvalue': 1}
            )
        with self.subTest('Replace list'):
            self.assertDictEqual(
                fsl_sub.config._merge_dict(a, c),
                {'avalue': {'another': 'dict'}, 'bvalue': 1, 'cvalue': [2, 3, 4]}
            )
        with self.subTest('Augment dict'):
            self.assertDictEqual(
                fsl_sub.config._merge_dict(a, d),
                {'avalue': {
                    'another': 'dict', 'something': 'else',
                    'yetanother': 'value'}, 'bvalue': 1, 'cvalue': [0, 1]}
            )
        with self.subTest('Replace dict key/value'):
            self.assertDictEqual(
                fsl_sub.config._merge_dict(a, e),
                {'avalue': {
                    'another': 'item', 'yetanother': 'value'},
                    'bvalue': 1, 'cvalue': [0, 1]}
            )

    @patch('fsl_sub.config.os.path.expanduser', autospec=True)
    def test_find_config_file(
            self, mock_expanduser):
        test_dir = tempfile.mkdtemp()
        try:
            test_file = os.path.join(test_dir, '.fsl_sub.yml')
            open(test_file, 'w').close()

            with patch.dict(
                    'fsl_sub.config.os.environ',
                    {'RANDOMENV': 'A', },
                    clear=True):
                with self.subTest('Expand user'):
                    mock_expanduser.return_value = test_dir
                    self.assertEqual(
                        fsl_sub.config.find_config_file(),
                        test_file
                    )
            os.unlink(test_file)
            test_file = os.path.join(test_dir, 'fsl_sub.yml')
            open(test_file, 'w').close()
            with self.subTest('Environment variable'):

                with patch.dict(
                        'fsl_sub.config.os.environ',
                        {'FSLSUB_CONF': test_file, },
                        clear=True):
                    self.assertEqual(
                        fsl_sub.config.find_config_file(),
                        test_file
                    )
            os.unlink(test_file)
            fsl_dir = os.path.join(test_dir, 'etc', 'fslconf')
            os.makedirs(fsl_dir)
            test_file = os.path.join(fsl_dir, 'fsl_sub.yml')
            open(test_file, 'w').close()
            with patch.dict(
                    'fsl_sub.config.os.environ',
                    {'FSLDIR': test_dir, },
                    clear=True):
                with self.subTest('FSLDIR'):
                    self.assertEqual(
                        fsl_sub.config.find_config_file(),
                        os.path.realpath(test_file)
                    )
            shutil.rmtree(os.path.join(test_dir, 'etc'))

            with self.subTest('Missing configuration'):
                with patch(
                        'fsl_sub.config.os.path.exists',
                        return_value=False):
                    self.assertRaises(
                        fsl_sub.config.MissingConfiguration,
                        fsl_sub.config.find_config_file
                    )

            with self.subTest('No FSLDIR'):
                with patch.dict(
                        'fsl_sub.config.os.environ',
                        clear=True):
                    location = os.path.abspath(
                        os.path.join(
                            __file__,
                            '..',
                            '..',
                            'plugins',
                            'fsl_sub_shell.yml')
                    )

                    self.assertEqual(
                        fsl_sub.config.find_config_file(),
                        location
                    )
        finally:
            shutil.rmtree(test_dir)

    @patch('fsl_sub.config.get_plugin_example_conf')
    @patch('fsl_sub.config._internal_config_file')
    def test_example_conf(self, mock_dcf, mock_gpe):
        base_config = '''ram_units: 'G'
modulecmd: False
thread_control:
    - 'OMP_NUM_THREADS'
method_opts: {}
queues: {}
coproc_opts: {}
'''
        coproc_config = '''---
coproc_opts:
  bitblit:
    resource: bits
'''
        queue_config = '''---
queues:
  short.q:
    runtime: 100
'''
        method_config = (
            '''method: 'shell'
method_opts:
    shell:
        queues: False''',
            '''method: sge
method_opts:
    sge:
        queues: True''', )
        merged_method_config = '''method_opts:
    shell:
        queues: False
    sge:
        queues: True'''
        expected_output = (
            "---\nmethod: 'sge'\n"
            + base_config.replace(
                'method_opts: {}\n', '').replace(
                    'queues: {}\n', '').replace(
                        'coproc_opts: {}\n', '')
            + merged_method_config
            + coproc_config.replace('---\n', '\n')
            + queue_config.replace('---\n', '')
        )
        with self.subTest('Single quoted method'):
            with tempfile.NamedTemporaryFile(mode='w') as ntf:
                ntf.write("---\nmethod: 'shell'\n" + base_config)
                ntf.flush()
                with tempfile.NamedTemporaryFile(mode='w') as ntf_cp:
                    ntf_cp.write(coproc_config)
                    ntf_cp.flush()
                    with tempfile.NamedTemporaryFile(mode='w') as ntf_cq:
                        ntf_cq.write(queue_config)
                        ntf_cq.flush()
                        mock_dcf.side_effect = (ntf.name, ntf_cp.name, ntf_cq.name, )
                        mock_gpe.side_effect = method_config

                        e_conf = fsl_sub.config.example_config(method='sge')
                        self.assertEqual(e_conf, expected_output)
                        mock_dcf.reset_mock(return_value=True, side_effect=True)
                        mock_gpe.reset_mock(return_value=True, side_effect=True)

        with self.subTest('Double quoted method'):
            with tempfile.NamedTemporaryFile(mode='w') as ntf:
                ntf.write('---\nmethod: "shell"\n' + base_config)
                ntf.flush()
                with tempfile.NamedTemporaryFile(mode='w') as ntf_cp:
                    ntf_cp.write(coproc_config)
                    ntf_cp.flush()
                    with tempfile.NamedTemporaryFile(mode='w') as ntf_cq:
                        ntf_cq.write(queue_config)
                        ntf_cq.flush()
                        mock_dcf.side_effect = (ntf.name, ntf_cp.name, ntf_cq.name, )
                        mock_gpe.side_effect = method_config
                        self.assertEqual(e_conf, expected_output)
                        mock_dcf.reset_mock(return_value=True, side_effect=True)
                        mock_gpe.reset_mock(return_value=True, side_effect=True)

        with self.subTest('unquoted quoted method'):
            with tempfile.NamedTemporaryFile(mode='w') as ntf:
                ntf.write('---\nmethod: shell\n' + base_config)
                ntf.flush()
                with tempfile.NamedTemporaryFile(mode='w') as ntf_cp:
                    ntf_cp.write(coproc_config)
                    ntf_cp.flush()
                    with tempfile.NamedTemporaryFile(mode='w') as ntf_cq:
                        ntf_cq.write(queue_config)
                        ntf_cq.flush()
                        mock_dcf.side_effect = (ntf.name, ntf_cp.name, ntf_cq.name, )
                        mock_gpe.side_effect = method_config
                        self.assertEqual(e_conf, expected_output)
                        mock_dcf.reset_mock(return_value=True, side_effect=True)
                        mock_gpe.reset_mock(return_value=True, side_effect=True)

    @patch(
        'fsl_sub.config.load_default_config',
        autospec=True,
        return_value={'bdict': "somevalue", })
    @patch('fsl_sub.config.find_config_file', autospec=True)
    def test_read_config_merge(self, mock_find_config_file, mock_ldc):
        fsl_sub.config.read_config.cache_clear()
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
                {
                    'adict': {
                        'alist': [1, 2],
                        'astring': 'hello',
                    },
                    'bdict': 'somevalue',
                }
            )
            m.assert_called_once_with('/etc/fsl_sub.conf', 'r')

    @patch('fsl_sub.config._internal_config_file', autospec=True)
    @patch('fsl_sub.config.get_plugin_example_conf', autospec=True)
    @patch('fsl_sub.config.available_plugins', autospec=True)
    def test_load_default_config(self, mock_ap, mock_gpec, mock__icf):
        base_conf = '''---
method: 'shell'
thread_control: []
method_opts: {}
coproc_opts: {}
queues: {}
'''
        plugins = [
            '''---
method_opts:
  shell:
    queues: False
''',
            '''---
method: 'sge'
method_opts:
  sge:
    queues: True
''', ]
        expected_config = {
            'method': 'shell',
            'thread_control': [],
            'method_opts': {
                'shell': {
                    'queues': False,
                },
                'sge': {
                    'queues': True,
                },
            },
            'coproc_opts': {},
            'queues': {},
        }
        with tempfile.NamedTemporaryFile(mode='w') as ntf:
            ntf.write(base_conf)
            ntf.flush()
            mock_ap.return_value = ['shell', 'sge', ]
            mock__icf.return_value = ntf.name
            mock_gpec.side_effect = plugins
            self.assertDictEqual(fsl_sub.config.load_default_config(), expected_config)
            mock_ap.reset_mock()
            mock__icf.reset_mock()
            mock_gpec.reset_mock()
            plugins[1] = plugins[1].replace('method_opts:\n', "method: 'sge'\nmethod_opts:\n")
            mock_gpec.side_effect = plugins
            self.assertDictEqual(fsl_sub.config.load_default_config(), expected_config)

    @patch(
        'fsl_sub.config.load_default_config',
        autospec=True,
        return_value={})
    @patch('fsl_sub.config.find_config_file', autospec=True)
    def test_read_config(self, mock_find_config_file, mock_ldc):
        with self.subTest("Test good read"):
            fsl_sub.config.read_config.cache_clear()
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

    @patch(
        'fsl_sub.config.load_default_config',
        autospec=True,
        return_value={})
    @patch('fsl_sub.config.read_config', autospec=True)
    def test_method_config(self, mock_read_config, mock_ldc):
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

    @patch(
        'fsl_sub.config.load_default_config',
        autospec=True,
        return_value={})
    @patch('fsl_sub.config.read_config', autospec=True)
    def test_coprocessor_config(self, mock_read_config, mock_ldc):
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

    @patch(
        'fsl_sub.config.load_default_config',
        autospec=True,
        return_value={})
    @patch('fsl_sub.config.read_config', autospec=True)
    def test_queue_config(self, mock_read_config, mock_ldc):
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

    @patch(
        'fsl_sub.config.load_default_config',
        autospec=True,
        return_value={})
    @patch('fsl_sub.config.read_config', autospec=True)
    def test_uses_projects(self, mock_read_config, mock_ldc):
        fsl_sub.config.method_config.cache_clear()
        with self.subTest('Test 1'):
            mock_read_config.return_value = {
                'method': 'method',
                'method_opts': {'method': {'projects': False, }, }, }
            self.assertFalse(fsl_sub.config.uses_projects())
        fsl_sub.config.method_config.cache_clear()
        with self.subTest('Test 2'):
            mock_read_config.return_value = {
                'method': 'method',
                'method_opts': {'method': {'projects': True, }, }, }
            self.assertTrue(fsl_sub.config.uses_projects())
        fsl_sub.config.method_config.cache_clear()


if __name__ == '__main__':
    unittest.main()
