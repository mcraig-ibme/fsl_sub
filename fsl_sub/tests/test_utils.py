#!/usr/bin/env python
import datetime
import io
import json
import os
import os.path as op
import platform
import shutil
import stat
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch, mock_open
from fsl_sub.exceptions import (
    BadOS,
    CommandError,
    UpdateError,
    NotAFslDir,
    NoCondaEnv,
    NoCondaEnvFile,
    NoFsl,
    PackageError,
)
import fsl_sub.utils


class TestFlattenList(unittest.TestCase):
    def test_flatten_list(self):
        self.assertListEqual(
            fsl_sub.utils.flatten_list(
                [1, 2, '-v', [4, 5, ], {'key': 'value'}]),
            [1, 2, '-v', 4, 5, {'key': 'value'}]
        )


class TestConversions(unittest.TestCase):
    def test_human_to_ram(self):
        with self.subTest('Bytes output'):
            self.assertEqual(
                fsl_sub.utils.human_to_ram('1K', output='B', as_int=True),
                1024
            )
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
        with self.subTest('Fractions - Round up'):
            self.assertEqual(
                fsl_sub.utils.human_to_ram('8.5G', output="G"),
                9
            )
        with self.subTest('Fractions - float'):
            self.assertEqual(
                fsl_sub.utils.human_to_ram('8.5G', output="G", as_int=False),
                8.5
            )
        with self.subTest('Fractions - float < 1'):
            self.assertEqual(
                fsl_sub.utils.human_to_ram('8.5K', output="M", as_int=False),
                0.00830078125
            )
        with self.subTest('Fractions - Round up'):
            self.assertEqual(
                fsl_sub.utils.human_to_ram('8.5K', output="M"),
                1
            )
        with self.subTest('Fractions - Round down'):
            self.assertEqual(
                fsl_sub.utils.human_to_ram('8.1G', output="G", round_down=True),
                8
            )
        with self.subTest('Fractions - Round down (2)'):
            self.assertEqual(
                fsl_sub.utils.human_to_ram('8.8G', output="G", round_down=True),
                8
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
                fsl_sub.utils.human_to_ram('10K', output='M', as_int=False),
                10 / 1024
            )
        with self.subTest('MBs to MBs'):
            self.assertEqual(
                fsl_sub.utils.human_to_ram('10M', output='M'),
                10
            )
        with self.subTest("No units"):
            self.assertEqual(
                fsl_sub.utils.human_to_ram('10', output="G", units="G"),
                10
            )
        with self.subTest("No units - default unit input"):
            self.assertEqual(
                fsl_sub.utils.human_to_ram('10', output="G"),
                10
            )
        with self.subTest("No units - default unit output"):
            self.assertEqual(
                fsl_sub.utils.human_to_ram('1', units="G"),
                1024
            )
        with self.subTest("No units - float"):
            self.assertEqual(
                fsl_sub.utils.human_to_ram(
                    '10.5', output="G", units="G", as_int=False),
                10.5
            )
        with self.subTest("No units - float as_int default"):
            self.assertEqual(
                fsl_sub.utils.human_to_ram(
                    '10.5', output="G", units="G"),
                11
            )
        with self.subTest("No units - float as_int true"):
            self.assertEqual(
                fsl_sub.utils.human_to_ram(
                    '10.5', output="G", units="G", as_int=True),
                11
            )


@patch('fsl_sub.utils.user_input', autospec=True)
@patch(
    'fsl_sub.utils.os.path.exists', autospec=True
)
class TestFindFsldir(unittest.TestCase):
    def test_find_fromenviron(self, mock_exists, mock_ui):
        mock_exists.return_value = True
        fsl_sub.utils.find_fsldir.cache_clear()
        with patch.dict(
                'fsl_sub.utils.os.environ',
                {'FSLDIR': '/usr/local/fsl'},
                clear=True):
            self.assertEqual(
                fsl_sub.utils.find_fsldir(),
                '/usr/local/fsl'
            )

    def test_find_fromuser(self, mock_exists, mock_ui):
        mock_ui.return_value = '/usr/local/fsl'
        mock_exists.return_value = True
        fsl_sub.utils.find_fsldir.cache_clear()
        with patch.dict(
                'fsl_sub.utils.os.environ',
                {},
                clear=True):
            self.assertEqual(
                fsl_sub.utils.find_fsldir(),
                '/usr/local/fsl'
            )

    def test_find_fromuser_invalid(self, mock_exists, mock_ui):
        mock_ui.side_effect = ['/usr/local/fsl', '']
        mock_exists.side_effect = [False, True]

        fsl_sub.utils.find_fsldir.cache_clear()
        with io.StringIO() as text_trap:
            sys.stderr = text_trap
            with patch.dict(
                    'fsl_sub.utils.os.environ',
                    {},
                    clear=True):
                self.assertRaises(
                    NotAFslDir,
                    fsl_sub.utils.find_fsldir
                )
                mock_exists.assert_called_once_with(
                    '/usr/local/fsl/etc/fslconf',
                )

            self.assertEqual(
                text_trap.getvalue(),
                'Not an FSL dir.\n')

        sys.stderr = sys.__stderr__

    def test_find_emptyinput(self, mock_exists, mock_ui):
        mock_ui.return_value = ''
        mock_exists.return_value = True
        fsl_sub.utils.find_fsldir.cache_clear()
        with patch.dict(
                'fsl_sub.utils.os.environ',
                {},
                clear=True):
            self.assertRaises(
                NotAFslDir,
                fsl_sub.utils.find_fsldir
            )


@patch(
    'fsl_sub.utils.os.path.exists', autospec=True
)
class TestConda_fsl_env(unittest.TestCase):
    @patch(
        'fsl_sub.utils.find_fsldir',
        autospec=True, return_value='/usr/local/fsl'
    )
    def test_cf_no_fsldir(self, mock_ffsld, mock_exists):
        mock_exists.return_value = True
        fsl_sub.utils.find_fsldir.cache_clear()
        self.assertEqual(
            fsl_sub.utils.conda_fsl_env(),
            '/usr/local/fsl/fslpython/envs/fslpython'
        )

    def test_cf_exists(self, mock_exists):
        mock_exists.return_value = True
        self.assertEqual(
            fsl_sub.utils.conda_fsl_env(
                fsldir='/usr/local/fsl'
            ),
            '/usr/local/fsl/fslpython/envs/fslpython'
        )

    def test_cf_notpresent(self, mock_exists):
        mock_exists.return_value = False
        self.assertRaises(
            NoCondaEnv,
            fsl_sub.utils.conda_fsl_env,
            fsldir='/opt/local/fsl'
        )

    @patch(
        'fsl_sub.utils.find_fsldir',
        autospec=True, side_effect=NotAFslDir()
    )
    def test_cf_badfsldir(self, mock_ffs, mock_exists):
        mock_exists.return_value = False
        self.assertRaises(
            NoCondaEnv,
            fsl_sub.utils.conda_fsl_env
        )


class TestConda_stderr(unittest.TestCase):
    def test_conda_stderr_sl(self):
        self.assertEqual(
            fsl_sub.utils.conda_stderr('''
Some random text

sl_{
    "message": "output"
}
'''),
            'output')

    def test_conda_stderr(self):
        self.assertEqual(
            fsl_sub.utils.conda_stderr('''
Some random text

{
    "message": "output"
}
'''),
            'output')

    def test_conda_stderr_nojson(self):
        self.assertEqual(
            fsl_sub.utils.conda_stderr('''
Some random text
'''),
            '''
Some random text
''')

    def test_conda_stderr_extratxt(self):
        self.assertEqual(
            fsl_sub.utils.conda_stderr('''
Some random text

{
    "message": "output"
}

Some more
'''),
            'output')

    def test_conda_stderr_extratxt2(self):
        self.assertEqual(
            fsl_sub.utils.conda_stderr('''
Some random text

{
    "message": "output"
}

Some more
{
    "nothing": "more"
}
'''),
            'output')

    def test_conda_stderr_extratxt3(self):
        self.assertEqual(
            fsl_sub.utils.conda_stderr('''
Some random text

{
    "bobbins": "output"
}
'''),
            '''
Some random text

{
    "bobbins": "output"
}
''')

    def test_conda_stderr_extratxt4(self):
        self.assertEqual(
            fsl_sub.utils.conda_stderr('''
Some random text

{
    'bobbins': "output"
}
'''),
            '''
Some random text

{
    'bobbins': "output"
}
''')


class TestConda_stdout_error(unittest.TestCase):
    def test_conda_stdout_error_validjson(self):
        with io.StringIO() as text_trap:
            sys.stdout = text_trap

            self.assertEqual(
                fsl_sub.utils.conda_stdout_error('''
{
    "message": "output"
}
'''),
                'output'
            )

            sys.stdout = sys.__stdout__

    def test_conda_stdout_error_invalidjson(self):
        with io.StringIO() as text_trap:
            sys.stdout = text_trap
            self.assertEqual(
                fsl_sub.utils.conda_stdout_error(
                    '''
    {
        "message": "output"
    }
    '''),
                "output"
            )
            sys.stdout = sys.__stdout__

    def test_conda_stdout_error_invalidjson2(self):
        with io.StringIO() as text_trap:
            sys.stdout = text_trap
            self.assertEqual(
                fsl_sub.utils.conda_stdout_error('''
{
    "something": "output"
}
'''),
                '''
{
    "something": "output"
}
''')
            sys.stdout = sys.__stdout__


@patch(
    'fsl_sub.utils.conda_fsl_env',
    autospec=True, return_value="/usr/local/fsl/fslpython/envs/fslpython"
)
@patch(
    'fsl_sub.utils.os.path.exists', autospec=True
)
@patch(
    'fsl_sub.utils.os.access', autospec=True
)
@patch(
    'fsl_sub.utils.shutil.which', autospec=True
)
class TestConda_bin(unittest.TestCase):
    def test_exists_fsldir(self, mock_which, mock_access, mock_exists, mock_fsl_env):
        mock_exists.return_value = True
        mock_access.return_value = True
        self.assertEqual(
            fsl_sub.utils.conda_bin(),
            '/usr/local/fsl/fslpython/envs/fslpython/../../bin/conda'
        )

    def test_exists_nofsldir(self, mock_which, mock_access, mock_exists, mock_fsl_env):
        mock_exists.return_value = True
        mock_access.return_value = True
        self.assertEqual(
            fsl_sub.utils.conda_bin(),
            '/usr/local/fsl/fslpython/envs/fslpython/../../bin/conda'
        )

    def test_notpresent(self, mock_which, mock_access, mock_exists, mock_fsl_env):
        mock_exists.return_value = False
        self.assertRaises(
            NoCondaEnv,
            fsl_sub.utils.conda_bin
        )


@patch(
    'fsl_sub.utils.conda_pkg_dirs_writeable', autospec=True
)
@patch(
    'fsl_sub.utils.conda_json', autospec=True
)
class TestCondaFindPackages(unittest.TestCase):
    def setUp(self):
        self.example_search = '''{
  "fsl_sub": [
    {
      "arch": "x86_64",
      "build": "py35_1",
      "build_number": 1,
      "channel": "https://my.repo.com/pkgs/free/osx-64",
      "constrains": [],
      "date": "2019-11-05",
      "depends": [
        "distribute",
        "python 3.5*"
      ],
      "fn": "fsl_sub-1.0-py35_1.tar.bz2",
      "license": "FSL",
      "md5": "92235789a541bcc50e935411df8044df",
      "name": "fsl_sub",
      "platform": "darwin",
      "size": 11993,
      "subdir": "osx-64",
      "url": "https://my.repo.com/pkgs/free/osx-64/fsl_sub-1.0-py35_1.tar.bz2",
      "version": "1.0"
    },
    {
      "arch": "x86_64",
      "build": "py37_1",
      "build_number": 1,
      "channel": "https://my.repo.com/pkgs/free/osx-64",
      "constrains": [],
      "date": "2019-11-05",
      "depends": [
        "distribute",
        "python 3.7*"
      ],
      "fn": "fsl_sub-1.0-py37_1.tar.bz2",
      "license": "FSL",
      "md5": "92235789a541bcc50e935411df8044df",
      "name": "fsl_sub",
      "platform": "darwin",
      "size": 11993,
      "subdir": "osx-64",
      "url": "https://my.repo.com/pkgs/free/osx-64/fsl_sub-1.0-py37_1.tar.bz2",
      "version": "1.0"
    }
  ]
}
'''
        self.json = json.loads(self.example_search)

    def test_conda_find_packages(
            self, mock_conda_json, mock_writeable):
        mock_conda_json.return_value = self.json
        mock_writeable.return_value = True
        self.assertEqual(
            fsl_sub.utils.conda_find_packages(
                'fsl_sub',
                fsldir='/usr/local/fsl'),
            ['fsl_sub', ]
        )

    def test_conda_find_packages_no_write(
            self, mock_conda_json, mock_writeable):
        mock_conda_json.return_value = self.json
        mock_writeable.return_value = False
        with self.assertRaises(PackageError) as context:
            fsl_sub.utils.conda_find_packages('fsl_sub')
            self.assertEqual(
                str(context.exception),
                "No permission to change Conda environment folder, re-try with 'sudo'"
            )


@patch(
    'fsl_sub.utils.get_conda_packages', autospec=True,
    return_value=['fsl_sub', 'fsl_sub_plugin_sge', ]
)
@patch(
    'fsl_sub.utils.conda_pkg_dirs_writeable', autospec=True
)
@patch(
    'fsl_sub.utils.conda_fsl_env', autospec=True,
    return_value='/usr/local/fsl/fslpython/env/fslpython'
)
@patch(
    'fsl_sub.utils.conda_json', autospec=True
)
class TestCondaUpdate(unittest.TestCase):
    def setUp(self):
        self.example_update = '''{
  "actions": {
    "FETCH": [
      {
        "base_url": "https://my.repo.com/conda-fsl",
        "build_number": 1,
        "build_string": "1",
        "channel": "fsl",
        "dist_name": "fsl_sub-2.0.0-1",
        "name": "fsl_sub",
        "platform": "noarch",
        "version": "2.0.0"
      }
    ],
    "LINK": [
      {
        "base_url": null,
        "build_number": 1,
        "build_string": "1",
        "channel": "fsl",
        "dist_name": "fsl_sub-2.0.0-1",
        "name": "fsl_sub",
        "platform": null,
        "version": "2.0.0"
      }
    ],
    "PREFIX": "/usr/local/fsl/fslpython/envs/fslpython",
    "UNLINK": [
      {
        "base_url": null,
        "build_number": 0,
        "build_string": "1",
        "channel": "fsl",
        "dist_name": "fsl_sub-1.0.0-1",
        "name": "fsl_sub",
        "platform": null,
        "version": "1.0.0"
      }
    ]
  },
  "prefix": "/usr/local/fsl/fslpython/envs/fslpython",
  "success": true
}
'''
        self.json = json.loads(self.example_update)

    def test_conda_update(self, mock_json, mock_env, mock_writeable, mock_packages):
        mock_json.return_value = self.json
        mock_env.return_value = self.json['prefix']
        mock_writeable.return_value = True
        self.assertEqual(
            fsl_sub.utils.conda_update(
                fsldir='/usr/local/fsl'),
            {'fsl_sub': {'version': '2.0.0', 'old_version': '1.0.0', }, }
        )

    def test_conda_update_noupdates(self, mock_json, mock_env, mock_writeable, mock_packages):
        mock_json.return_value = {"message": "All requested packages already installed.", }
        mock_env.return_value = self.json['prefix']
        mock_writeable.return_value = True
        self.assertIsNone(
            fsl_sub.utils.conda_update(
                fsldir='/usr/local/fsl')
        )

    def test_conda_update_all(self, mock_json, mock_env, mock_writeable, mock_packages):
        mock_json.return_value = self.json
        mock_env.return_value = self.json['prefix']
        mock_writeable.return_value = True
        self.assertEqual(
            fsl_sub.utils.conda_update(
                fsldir='/usr/local/fsl'),
            {'fsl_sub': {'version': '2.0.0', 'old_version': '1.0.0', }, }
        )

    def test_conda_update_all_nofsldir(self, mock_json, mock_env, mock_writeable, mock_packages):
        mock_json.return_value = self.json
        mock_env.return_value = self.json['prefix']
        mock_writeable.return_value = True
        self.assertEqual(
            fsl_sub.utils.conda_update(),
            {'fsl_sub': {'version': '2.0.0', 'old_version': '1.0.0', }, }
        )

    def test_conda_update_sp_exception(self, mock_json, mock_env, mock_writeable, mock_packages):
        mock_json.return_value = self.json
        mock_env.side_effect = NoCondaEnv("no such file or directory")
        mock_writeable.return_value = True
        with self.assertRaises(UpdateError) as context:
            fsl_sub.utils.conda_update()
            self.assertEqual(
                str(context.exception),
                'Unable to update! (no such file or directory)'
            )

    def test_conda_update_pkg_no_write(self, mock_json, mock_env, mock_writeable, mock_packages):
        mock_json.return_value = self.json
        mock_env.return_value = "/usr/local/bin/env"
        mock_writeable.return_value = False
        with self.assertRaises(UpdateError) as context:
            fsl_sub.utils.conda_update()
            self.assertEqual(
                str(context.exception),
                "No permission to change Conda environment folder, re-try with 'sudo'"
            )


@patch(
    'fsl_sub.utils.subprocess.run', autospec=True
)
@patch(
    'fsl_sub.utils.conda_bin', autospec=True
)
@patch(
    'fsl_sub.utils.conda_channels', autospec=True)
class TestCondaJson(unittest.TestCase):
    def test_no_channel(self, mock_channel, mock_bin, mock_spr):
        mock_channel.return_value = []
        mock_bin.return_value = '/usr/local/bin/conda'
        mock_spr.return_value = subprocess.CompletedProcess(
            [
                '/usr/local/bin/conda',
                'search',
                '--json',
                'fsl_sub'
            ],
            0,
            stdout='''
{
    "message": "Found package"
}
'''
        )
        self.assertEqual(
            {'message': "Found package", },
            fsl_sub.utils.conda_json('search', 'fsl_sub')
        )

    def test_channel(self, mock_channel, mock_bin, mock_spr):
        mock_channel.return_value = 'fsl'
        mock_bin.return_value = '/usr/local/bin/conda'
        mock_spr.return_value = subprocess.CompletedProcess(
            [
                '/usr/local/bin/conda',
                'search',
                '--json',
                '-c',
                'fsl',
                'fsl_sub',
            ],
            0,
            stdout='''
{
    "message": "Found package"
}
'''
        )
        self.assertEqual(
            {'message': "Found package", },
            fsl_sub.utils.conda_json('search', 'fsl_sub')
        )

    def test_missing_channel_file(self, mock_channel, mock_bin, mock_spr):
        mock_channel.side_effect = NoCondaEnvFile("my error")
        mock_bin.return_value = '/usr/local/bin/conda'
        with self.assertRaises(PackageError) as pe:
            fsl_sub.utils.conda_json('search', 'fsl_sub')
            self.assertEqual(
                "FSL lacks Python distribution: my error. "
                "If running standalone unset all FSL environment variables.",
                str(pe.exception))

    def test_conda_json_exception(self, mock_channel, mock_bin, mock_spr):
        mock_channel.return_value = "fsl"
        mock_bin.return_value = '/usr/local/bin/conda'
        mock_spr.return_value = subprocess.CompletedProcess(
            [
                '/usr/local/fsl/fslpython/envs/fslpython/bin/conda',
                'search',
                '--json',
                '-q',
                '-y',
                '-p',
                '/usr/local/fsl/fslpython/envs/fslpython',
                '-c',
                'fsl',
                'fsl_sub'
            ],
            0,
            stdout='''
{
    'message': "Failed to find package"
}
'''
        )
        with self.assertRaises(PackageError) as context:
            fsl_sub.utils.conda_json('search', 'fsl_sub')
        self.assertEqual(
            str(context.exception),
            'Expecting property name enclosed in double quotes: line 3 column 5 (char 7)'
        )

    def test_conda_missing_package(self, mock_channel, mock_bin, mock_spr):
        mock_channel.return_value = "fsl"
        mock_bin.return_value = '/usr/local/bin/conda'
        mock_spr.side_effect = subprocess.CalledProcessError(
            cmd=[
                '/usr/local/fsl/fslpython/envs/fslpython/bin/conda',
                'search',
                '--json',
                '-q',
                '-y',
                '-p',
                '/usr/local/fsl/fslpython/envs/fslpython',
                '-c',
                'fsl',
                'fsl_sub'
            ],
            returncode=1,
            stderr=None,
            output='''
{
    "message": "Failed to find package"
}
'''
        )
        with self.assertRaises(PackageError) as context:
            fsl_sub.utils.conda_json('search', 'fsl_sub')
        self.assertEqual(
            str(context.exception),
            'Failed to find package'
        )


@patch(
    'fsl_sub.utils.find_fsldir', autospec=True,
    return_value='/usr/local/fsl'
)
@patch(
    'fsl_sub.utils.subprocess.run', autospec=True
)
@patch(
    'fsl_sub.utils.conda_bin', autospec=True,
    return_value="/usr/local/fsl/fslpython/envs/fslpython/bin/conda"
)
@patch(
    'fsl_sub.utils.conda_fsl_env', autospec=True,
    return_value="/usr/local/fsl/fslpython/envs/fslpython"
)
class TestCondaChannel(unittest.TestCase):
    def test_conda_channels(
            self, mock_env,
            mock_cbin, mock_spr, mock_fsldir):
        m_open = mock_open(read_data='''name: fslpython
channels:
 - https://fsl.fmrib.ox.ac.uk/fsldownloads/fslconda/channel
 - defaults
 - conda-forge
dependencies:
 - python=3.5.2
 ''')
        with patch('fsl_sub.utils.open', m_open):
            self.assertEqual(
                fsl_sub.utils.conda_channels(fsldir='/opt/fsl'),
                [
                    'https://fsl.fmrib.ox.ac.uk/fsldownloads/fslconda/channel',
                    'defaults',
                    'conda-forge',
                ]
            )

    def test_conda_channels_filemissing(
            self, mock_env,
            mock_cbin, mock_spr, mock_fsldir):
        m_open = mock_open(read_data='')
        with patch('fsl_sub.utils.open', m_open):
            m_open.side_effect = IOError()
            self.assertRaises(
                NoCondaEnvFile,
                fsl_sub.utils.conda_channels,
                fsldir='/opt/fsl',
            )


@patch(
    'fsl_sub.utils.conda_pkg_dirs_writeable', autospec=True
)
@patch(
    'fsl_sub.utils.conda_json', autospec=True
)
@patch(
    'fsl_sub.utils.conda_fsl_env', autospec=True,
    return_value="/usr/local/fsl/fslpython/envs/fslpython"
)
@patch(
    'fsl_sub.utils.get_conda_packages', autospec=True,
    return_value=['fsl_sub', ]
)
class TestConda(unittest.TestCase):
    def test_conda_check_update_conda_noupdate(
            self, mock_gcp, mock_env, mock_json, mock_writeable):
        mock_json.return_value = {
            "message": "All requested packages already installed.",
            "success": True, }
        mock_writeable.return_value = True
        self.assertIsNone(
            fsl_sub.utils.conda_check_update()
        )

    def test_conda_check_update_conda_updates(
            self, mock_gcp, mock_env, mock_json, mock_writeable):
        updates = {
            "actions": {
                "FETCH": [
                    {
                        "base_url": "https://fsl.fmrib.ox.ac.uk/fsldownloads/fslconda",
                        "build_number": 2,
                        "build_string": "blah_2",
                        "channel": "defaults",
                        "dist_name": "fsl_sub-2.0.0-blah_2",
                        "name": "fsl_sub",
                        "platform": "osx-64",
                        "version": "2.0.0"
                    }
                ],
                "LINK": [
                    {
                        "base_url": None,
                        "build_number": 0,
                        "build_string": "0",
                        "channel": "fsl",
                        "dist_name": "fsl_sub-2.0.0",
                        "name": "fsl_sub",
                        "platform": None,
                        "version": "2.0.0"
                    }
                ],
                "PREFIX": "/path/to/env",
                "UNLINK": [
                    {
                        "base_url": None,
                        "build_number": 1,
                        "build_string": "1",
                        "channel": "fsl",
                        "dist_name": "fsl_sub-1.0.0",
                        "name": "fsl_sub",
                        "platform": None,
                        "version": "1.0.0"
                    }
                ]
            },
            "prefix": "/path/to/env",
            "success": True
        }
        mock_json.return_value = updates
        mock_writeable.return_value = True
        update_dict = {
            'fsl_sub': {
                'version': '2.0.0',
                'old_version': '1.0.0',
            }
        }
        self.assertEqual(fsl_sub.utils.conda_check_update(), update_dict)


class TestPlugins(unittest.TestCase):
    def setUp(self):
        fsl_sub.utils.load_plugins.cache_clear()

    @patch('fsl_sub.utils.pkgutil.iter_modules', autospec=True)
    @patch('fsl_sub.utils.importlib.import_module', autospec=True)
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

    @patch('fsl_sub.utils.load_plugins')
    def test_available_plugins(self, mock_load_plugins):
        mock_load_plugins.return_value = {
            'fsl_sub_plugin_1': 'finder1',
            'fsl_sub_plugin_2': 'finder2', }
        plugins = fsl_sub.utils.available_plugins()
        plugins.sort()
        self.assertListEqual(
            ['1', '2', ],
            plugins
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
            {'THREADS': '1', 'MORETHREADS': '1', 'FSLSUB_PARALLEL': '1', }
        )
        with self.subTest("Add to list"):
            test_list = []
            fsl_sub.utils.control_threads(
                ['THREADS', 'MORETHREADS', ], 1, add_to_list=test_list)
            self.assertDictEqual(
                dict(fsl_sub.utils.os.environ),
                {'THREADS': '1', 'MORETHREADS': '1', 'FSLSUB_PARALLEL': '1', }
            )
            self.assertListEqual(
                test_list,
                ['THREADS=1', "MORETHREADS=1", "FSLSUB_PARALLEL=1", ]
            )
        with self.subTest("Mod list"):
            test_list = ['THREADS=4']
            fsl_sub.utils.control_threads(
                ['THREADS', 'MORETHREADS', ], 1, add_to_list=test_list)
            self.assertDictEqual(
                dict(fsl_sub.utils.os.environ),
                {'THREADS': '1', 'MORETHREADS': '1', 'FSLSUB_PARALLEL': '1', }
            )
            self.assertListEqual(
                test_list,
                ['THREADS=1', "MORETHREADS=1", 'FSLSUB_PARALLEL=1']
            )
        with self.subTest("Update dict"):
            test_dict = {}
            fsl_sub.utils.control_threads(
                ['THREADS', 'MORETHREADS', ], 1, env_dict=test_dict)
            self.assertDictEqual(
                dict(fsl_sub.utils.os.environ),
                {'THREADS': '1', 'MORETHREADS': '1', 'FSLSUB_PARALLEL': '1', }
            )
            self.assertDictEqual(
                test_dict,
                {
                    'THREADS': str(1),
                    'MORETHREADS': str(1),
                    'FSLSUB_PARALLEL': '1',
                }
            )

    def test_update_envvar_list(self):
        env_list = []
        fsl_sub.utils.update_envvar_list(env_list, 'VAR')
        self.assertListEqual(
            env_list,
            ['VAR'])
        fsl_sub.utils.update_envvar_list(env_list, 'VAR=1')
        self.assertListEqual(
            env_list,
            ['VAR=1'])
        fsl_sub.utils.update_envvar_list(env_list, 'VAR=2')
        self.assertListEqual(
            env_list,
            ['VAR=2'])
        fsl_sub.utils.update_envvar_list(env_list, 'VAR2')
        self.assertListEqual(
            env_list,
            ['VAR=2', "VAR2"])
        fsl_sub.utils.update_envvar_list(env_list, 'VAR=1', overwrite=False)
        self.assertListEqual(
            env_list,
            ['VAR=2', "VAR2"]
        )
        fsl_sub.utils.update_envvar_list(env_list, 'VAR=1', overwrite=True)
        self.assertListEqual(
            sorted(env_list),
            sorted(['VAR=1', "VAR2"])
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

    def test_check_command_file2(self):
        test_file = tempfile.NamedTemporaryFile(delete=False)
        test_file.write(b"/no/path/googoo /tmp")
        test_file.close()
        self.assertRaises(
            fsl_sub.utils.CommandError,
            fsl_sub.utils.check_command_file,
            test_file.name
        )
        os.remove(test_file.name)
        test_script = tempfile.NamedTemporaryFile(delete=False)
        test_script.write(b"ls /tmp")
        os.chmod(test_script.name, stat.S_IRUSR | stat.S_IXUSR)
        test_script.close()
        test_file = tempfile.NamedTemporaryFile(mode='w', delete=False)
        test_file.write(str(test_script.name))
        test_file.close()
        fsl_sub.utils.check_command_file(test_file.name)

        os.remove(test_script.name)
        os.remove(test_file.name)
        test_script = tempfile.NamedTemporaryFile(delete=False)
        test_script.write(b"ls /tmp")
        os.chmod(test_script.name, stat.S_IRUSR)
        test_script.close()
        self.assertRaises(
            fsl_sub.utils.CommandError,
            fsl_sub.utils.check_command_file,
            test_file.name
        )
        os.remove(test_script.name)
        test_file = tempfile.NamedTemporaryFile(delete=False)
        test_file.write(b"dummy\n")
        os.chmod(test_file.name, stat.S_IRUSR)
        test_file.close()
        fsl_sub.utils.check_command_file(test_file.name)
        os.remove(test_file.name)

        with self.subTest('|| with empty line'):
            with tempfile.NamedTemporaryFile(delete=False, mode='w') as test_file:
                test_file.write("ls\nls\n\n")
            os.chmod(test_file.name, stat.S_IRUSR)
            with self.assertRaises(fsl_sub.utils.CommandError) as eo:
                fsl_sub.utils.check_command_file(test_file.name)
            self.assertEqual(
                str(eo.exception),
                "Array task file contains a blank line at line 3"
            )
        os.remove(test_file.name)

        with self.subTest('|| with comment line line'):
            with tempfile.NamedTemporaryFile(delete=False, mode='w') as test_file:
                test_file.write("ls\n#\n\n")
            os.chmod(test_file.name, stat.S_IRUSR)
            with self.assertRaises(fsl_sub.utils.CommandError) as eo:
                fsl_sub.utils.check_command_file(test_file.name)
            self.assertEqual(
                str(eo.exception),
                "Array task file contains comment line (begins #) at line 2"
            )
        os.remove(test_file.name)

        with self.subTest('|| with quoted commands'):
            with tempfile.TemporaryDirectory() as tmpdir:
                spacydir = op.join(tmpdir, 'spacy dir')
                ls = shutil.which('ls')
                commands = op.join(tmpdir, 'commands.txt')
                os.mkdir(spacydir)
                shutil.copy(ls, spacydir)
                with open(commands, 'wt') as f:
                    f.write('\n'.join(["'ls'",
                                       '"ls"',
                                       '"{}"'.format(ls),
                                       '"{}/spacy dir/ls"'.format(tmpdir),
                                       r'{}/spacy\ dir/ls'.format(tmpdir)]))

                os.chmod(commands, stat.S_IRUSR)
                fsl_sub.utils.check_command_file(commands)


class TestFileIsImage(unittest.TestCase):
    @patch('fsl_sub.utils.os.path.isfile', autospec=True)
    @patch('fsl_sub.utils.system_stdout', autospec=True)
    def test_file_is_image(self, mock_sstdout, mock_isfile):
        with patch.dict(
                'fsl_sub.utils.os.environ',
                {'FSLDIR': '/usr/local/fsl', },
                clear=True):
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

        with patch.dict(
                'fsl_sub.utils.os.environ',
                {},
                clear=True):
            self.assertRaises(
                NoFsl,
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


@patch('fsl_sub.utils.os.umask', autospec=True)
class TestFixPerms(unittest.TestCase):
    def test_fixperms(self, mock_osum):
        with self.subTest('0002'):
            mock_osum.return_value = 0o0002
            with tempfile.NamedTemporaryFile(mode='wt') as ntf:
                fsl_sub.utils.fix_permissions(ntf.name, 0o666)
                perms = os.stat(ntf.name)
                self.assertEqual(
                    perms.st_mode & 0o777,
                    0o664)

        with self.subTest('0022'):
            mock_osum.return_value = 0o0022

            with tempfile.NamedTemporaryFile(mode='wt') as ntf:
                fsl_sub.utils.fix_permissions(ntf.name, 0o666)
                perms = os.stat(ntf.name)
                self.assertEqual(
                    perms.st_mode & 0o777,
                    0o644)


class TestBashCmd(unittest.TestCase):
    @patch('fsl_sub.utils.platform.system', return_value='Linux')
    @patch('fsl_sub.utils.which', return_value='/bin/bash')
    def test_linux(self, mock_which, mock_sp):
        self.assertEqual(
            '/bin/bash',
            fsl_sub.utils.bash_cmd()
        )

    @patch('fsl_sub.utils.platform.system', return_value='Linux')
    @patch('fsl_sub.utils.which', return_value=None)
    def test_linuxNoBash(self, mock_which, mock_sp):
        self.assertRaises(
            BadOS,
            fsl_sub.utils.bash_cmd
        )

    @patch('fsl_sub.utils.platform.system', return_value='Darwin')
    @patch(
        'fsl_sub.utils.platform.uname',
        return_value=platform.uname_result(
            system='Darwin', node='hostname.domain',
            release='18.7.0',
            version='Darwin Kernel Version 18.7.0: Mon Mar  8 22:11:48 PST 2021; root:xnu-4903.278.65~1/RELEASE_X86_64',
            machine='x86_64')
    )
    @patch('fsl_sub.utils.which', return_value='/bin/bash')
    def test_darwin_pre_catalina(self, mock_which, mock_pu, mock_sp):
        self.assertEqual(
            '/bin/bash',
            fsl_sub.utils.bash_cmd()
        )

    @patch('fsl_sub.utils.platform.system', return_value='Darwin')
    @patch(
        'fsl_sub.utils.platform.uname',
        return_value=platform.uname_result(
            system='Darwin', node='hostname.domain',
            release='19.7.0',
            version='Darwin Kernel Version 19.7.0: Mon Mar  8 22:11:48 PST 2021; root:xnu-4903.278.65~1/RELEASE_X86_64',
            machine='x86_64')
    )
    @patch('fsl_sub.utils.which', return_value='/bin/zsh')
    def test_darwin_catalina(self, mock_which, mock_up, mock_sp):
        self.assertEqual(
            '/bin/zsh',
            fsl_sub.utils.bash_cmd()
        )
        mock_which.assert_called_with('zsh')


class TestJobScript(unittest.TestCase):
    def setUp(self):
        self.now = datetime.datetime.now()
        self.strftime = datetime.datetime.strftime
        self.bash = '/bin/bash'
        self.qsub = '/usr/bin/qsub'
        self.patch_objects = {
            'fsl_sub.utils.datetime': {'autospec': True, },
            'fsl_sub.utils.bash_cmd': {'autospec': True, 'return_value': self.bash, },
        }

        self.patch_dict_objects = {}
        self.patches = {}
        for p, kwargs in self.patch_objects.items():
            self.patches[p] = patch(p, **kwargs)
        self.mocks = {}
        for o, p in self.patches.items():
            self.mocks[o] = p.start()

        self.dict_patches = {}
        for p, kwargs in self.patch_dict_objects.items():
            self.dict_patches[p] = patch.dict(p, **kwargs)

        for o, p in self.dict_patches.items():
            self.mocks[o] = p.start()
        self.mocks['fsl_sub.utils.datetime'].datetime.now.return_value = self.now
        self.mocks['fsl_sub.utils.datetime'].datetime.strftime = self.strftime
        self.addCleanup(patch.stopall)

    def TearDown(self):
        patch.stopall()

    @patch(
        'fsl_sub.utils.sys.argv', ['fsl_sub', '-q', 'short.q', './mycommand', 'arg1', 'arg2', ]
    )
    @patch(
        'fsl_sub.utils.VERSION', '1.0.0'
    )
    def test_job_scriptbasic(self):

        exp_head = [
            '#!' + self.bash,
            '',
            '#$ -q short.q',
        ]
        exp_cmd_start = list(exp_head)
        exp_cmd_mid = [
            '# Built by fsl_sub v.1.0.0 and fsl_sub_plugin_sge v.2.0.0',
            '# Command line: fsl_sub -q short.q ./mycommand arg1 arg2',
            '# Submission time (H:M:S DD/MM/YYYY): {0}'.format(self.now.strftime("%H:%M:%S %d/%m/%Y")),
            '',
        ]

        test_cmd = list(exp_cmd_start)
        test_cmd.extend(exp_cmd_mid)
        test_cmd.extend(['./mycommand arg1 arg2', ''])
        self.assertListEqual(
            fsl_sub.utils.job_script(
                ['./mycommand', 'arg1', 'arg2', ],
                [['-q', 'short.q']],
                '#$',
                ('sge', '2.0.0'),
                [],
                []
            ),
            test_cmd
        )
        with self.subTest("command as string"):
            self.assertListEqual(
                fsl_sub.utils.job_script(
                    './mycommand arg1 arg2',
                    [['-q', 'short.q']],
                    '#$',
                    ('sge', '2.0.0', ),
                    [],
                    []
                ),
                test_cmd
            )
        with self.subTest("modules"):
            test_cmd = list(exp_head)
            test_cmd.append('module load module1')
            test_cmd.append('module load module2')
            test_cmd.extend(exp_cmd_mid)
            test_cmd.extend(['./mycommand arg1 arg2', ''])
            self.assertListEqual(
                fsl_sub.utils.job_script(
                    './mycommand arg1 arg2',
                    [['-q', 'short.q']],
                    '#$',
                    ('sge', '2.0.0', ),
                    ['module1', 'module2'],
                    []
                ),
                test_cmd
            )

        with self.subTest("modules path"):
            test_cmd = list(exp_head)
            e_modp = ['/usr/local/shellmods', ]
            test_cmd.append('MODULEPATH=' + ':'.join(e_modp) + ':$MODULEPATH')
            test_cmd.append('module load module1')
            test_cmd.append('module load module2')
            test_cmd.extend(exp_cmd_mid)
            test_cmd.extend(['./mycommand arg1 arg2', ''])
            self.assertListEqual(
                fsl_sub.utils.job_script(
                    './mycommand arg1 arg2',
                    [['-q', 'short.q']],
                    '#$',
                    ('sge', '2.0.0', ),
                    ['module1', 'module2'],
                    [],
                    e_modp
                ),
                test_cmd
            )


class TestMergeDict(unittest.TestCase):
    def test_merge_dict(self):
        a = {'avalue': {'another': 'dict'}, 'bvalue': 1, 'cvalue': [0, 1, ]}
        b = {'dvalue': 1}
        c = {'cvalue': [2, 3, 4, ]}
        d = {'avalue': {'something': 'else', 'yetanother': 'value'}}
        e = {'avalue': {'another': 'item', 'yetanother': 'value'}}
        with self.subTest('Add key/value'):
            self.assertDictEqual(
                fsl_sub.utils.merge_dict(a, b),
                {'avalue': {
                    'another': 'dict'}, 'bvalue': 1,
                    'cvalue': [0, 1], 'dvalue': 1}
            )
        with self.subTest('Replace list'):
            self.assertDictEqual(
                fsl_sub.utils.merge_dict(a, c),
                {'avalue': {'another': 'dict'}, 'bvalue': 1, 'cvalue': [2, 3, 4]}
            )
        with self.subTest('Augment dict'):
            self.assertDictEqual(
                fsl_sub.utils.merge_dict(a, d),
                {'avalue': {
                    'another': 'dict', 'something': 'else',
                    'yetanother': 'value'}, 'bvalue': 1, 'cvalue': [0, 1]}
            )
        with self.subTest('Replace dict key/value'):
            self.assertDictEqual(
                fsl_sub.utils.merge_dict(a, e),
                {'avalue': {
                    'another': 'item', 'yetanother': 'value'},
                    'bvalue': 1, 'cvalue': [0, 1]}
            )


@patch('fsl_sub.utils.conda_json', autospec=True)
class TestGetCondaPackages(unittest.TestCase):
    def test_get_conda_packages_one(self, mock_conda_json):
        mock_conda_json.return_value = [
            {'name': 'fsl_sub', },
        ]

        self.assertListEqual(
            fsl_sub.utils.get_conda_packages(conda_env="/usr/local/fsl/fslpython/envs/fslpython"),
            ['fsl_sub', ]
        )

    def test_get_conda_packages_one_with_others(self, mock_conda_json):
        mock_conda_json.return_value = [
            {'name': 'fsl_sub', },
            {'name': 'feat'}
        ]

        self.assertListEqual(
            fsl_sub.utils.get_conda_packages(conda_env="/usr/local/fsl/fslpython/envs/fslpython"),
            ['fsl_sub', ]
        )

    def test_get_conda_packages_two(self, mock_conda_json):
        mock_conda_json.return_value = [
            {'name': 'fsl_sub', },
            {'name': 'fsl_sub_plugin_sge', },
        ]

        self.assertListEqual(
            fsl_sub.utils.get_conda_packages(conda_env="/usr/local/fsl/fslpython/envs/fslpython"),
            ['fsl_sub', 'fsl_sub_plugin_sge', ]
        )

    def test_get_conda_packages_two_with_others(self, mock_conda_json):
        mock_conda_json.return_value = [
            {'name': 'fsl_sub', },
            {'name': 'fsl_sub_plugin_sge', },
            {'name': 'feat'}
        ]

        self.assertListEqual(
            fsl_sub.utils.get_conda_packages(conda_env="/usr/local/fsl/fslpython/envs/fslpython"),
            ['fsl_sub', 'fsl_sub_plugin_sge', ]
        )


class TestBuildJobName(unittest.TestCase):
    def test_build_job_name(self):
        with self.subTest("single command simple"):
            self.assertEqual(
                fsl_sub.utils.build_job_name('/usr/bin/echo hello'),
                'echo'
            )
        with self.subTest("command as list"):
            self.assertEqual(
                fsl_sub.utils.build_job_name(['/usr/bin/echo', 'hello']),
                'echo'
            )
        with self.subTest("single command complex"):
            self.assertEqual(
                fsl_sub.utils.build_job_name('ENVAR=1; /usr/bin/echo $ENVVAR'),
                'echo'
            )
        with self.subTest("command as list complex"):
            self.assertEqual(
                fsl_sub.utils.build_job_name(['ENVAR=1; /usr/bin/echo $ENVVAR']),
                'echo'
            )
        with self.subTest("command as list complex 2"):
            self.assertEqual(
                fsl_sub.utils.build_job_name(['ENVAR=1; /usr/bin/echo', '$ENVVAR']),
                'echo'
            )


if __name__ == '__main__':
    unittest.main()
