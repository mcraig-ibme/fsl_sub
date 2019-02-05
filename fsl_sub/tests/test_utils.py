#!/usr/bin/env python
import subprocess
import sys
import unittest
from unittest.mock import patch, mock_open
from fsl_sub.exceptions import (
    CommandError,
    UpdateError,
    NotAFslDir,
    NoCondaEnv,
    NoChannelFound,
    NoCondaEnvFile,
    NoFsl,
    PackageError,
    InstallError,
)
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


class TestConda_stout_error(unittest.TestCase):
    def test_conda_stdout_error_validjson(self):
        self.assertEqual(
            fsl_sub.utils.conda_stdout_error('''
{
    "message": "output"
}
'''),
            'output'
        )

    def test_conda_stdout_error_invalidjson(self):
        self.assertEqual(
            fsl_sub.utils.conda_stdout_error('''
{
    'message': "output"
}
'''),
            '''
{
    'message': "output"
}
''')

    def test_conda_stdout_error_invalidjson2(self):
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


@patch(
        'fsl_sub.utils.conda_fsl_env',
        autospec=True, return_value="/usr/local/fsl/fslpython/envs/fslpython"
        )
@patch(
    'fsl_sub.utils.os.path.exists', autospec=True
)
class TestConda_bin(unittest.TestCase):
    def test_exists(self, mock_exists, mock_ffsld):
        mock_exists.return_value = True
        self.assertEqual(
            fsl_sub.utils.conda_bin(),
            '/usr/local/fsl/fslpython/envs/fslpython/bin/conda'
            )

    def test_notpresent(self, mock_exists, mock_ff):
        mock_exists.return_value = False
        self.assertRaises(
            NoCondaEnv,
            fsl_sub.utils.conda_bin
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
    'fsl_sub.utils.conda_fsl_env', auto_space=True,
    return_value="/usr/local/fsl/fslpython/envs/fslpython"
    )
@patch(
    'fsl_sub.utils.conda_channel', auto_space=True,
    return_value="fsl"
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

    def test_conda_find_packages(
            self, mock_ch, mock_env,
            mock_bin, mock_spr, mock_fsl):
        mock_spr.return_value = subprocess.CompletedProcess(
            [
                '/usr/local/fsl/fslpython/envs/fslpython/bin/conda',
                'search',
                '--json',
                '-c',
                'fsl',
                'fsl_sub'
            ],
            0,
            stdout=self.example_search
        )
        self.assertEqual(
            fsl_sub.utils.conda_find_packages(
                'fsl_sub',
                fsldir='/usr/local/fsl'),
            ['fsl_sub', ]
            )

    def test_conda_find_packages_nofsldir(
            self, mock_ch, mock_env,
            mock_bin, mock_spr, mock_fsl):
        mock_spr.return_value = subprocess.CompletedProcess(
            [
                '/usr/local/fsl/fslpython/envs/fslpython/bin/conda',
                'search',
                '--json',
                '-c',
                'fsl',
                'fsl_sub'
            ],
            0,
            stdout=self.example_search
        )
        self.assertEqual(
            fsl_sub.utils.conda_find_packages('fsl_sub'),
            ['fsl_sub', ]
            )

    def test_conda_find_packages_nofsldir_exception(
            self, mock_ch, mock_env,
            mock_bin, mock_spr, mock_fsl):
        mock_fsl.side_effect = NotAFslDir('/usr/local/fsl')
        mock_spr.return_value = subprocess.CompletedProcess(
            [
                '/usr/local/fsl/fslpython/envs/fslpython/bin/conda',
                'search',
                '--json',
                '-c',
                'fsl',
                'fsl_sub'
            ],
            0,
            stdout=self.example_search
        )
        self.assertRaises(
            NoCondaEnv,
            fsl_sub.utils.conda_find_packages,
            'fsl_sub'
            )

    def test_conda_find_packages_sp_exception(
            self, mock_ch, mock_env,
            mock_bin, mock_spr, mock_fsl):
        mock_spr.side_effect = subprocess.CalledProcessError(
            1,
            [
                '/usr/local/fsl/fslpython/envs/fslpython/bin/conda',
                'search',
                '--json',
                '-c',
                'fsl',
                'fsl_sub'
            ],
            output='''no such file or directory'''
        )
        with self.assertRaises(PackageError) as context:
            fsl_sub.utils.conda_find_packages('fsl_sub')
        self.assertEqual(
            str(context.exception),
            'Unable to search for packages! '
            '(no such file or directory)'
        )

    def test_conda_find_packages_sp_exception2(
            self, mock_ch, mock_env,
            mock_bin, mock_spr, mock_fsl):
        mock_spr.side_effect = subprocess.CalledProcessError(
            1,
            [
                '/usr/local/fsl/fslpython/envs/fslpython/bin/conda',
                'search',
                '--json',
                '-c',
                'fsl',
                'fsl_sub'
            ],
            output='''a warning

{
    "message": "Failed to find package"
}
'''
        )
        with self.assertRaises(PackageError) as context:
            fsl_sub.utils.conda_find_packages('fsl_sub')
        self.assertEqual(
            str(context.exception),
            'Unable to search for packages! '
            '''(a warning

{
    "message": "Failed to find package"
}
)'''
            )

    def test_conda_find_packages_sp_exception3(
            self, mock_ch, mock_env,
            mock_bin, mock_spr, mock_fsl):
        mock_spr.side_effect = subprocess.CalledProcessError(
            1,
            [
                '/usr/local/fsl/fslpython/envs/fslpython/bin/conda',
                'search',
                '--json',
                '-c',
                'fsl',
                'fsl_sub'
            ],
            stderr='''no such file or directory'''
        )
        with self.assertRaises(PackageError) as context:
            fsl_sub.utils.conda_find_packages('fsl_sub')
        self.assertEqual(
            str(context.exception),
            'Unable to search for packages! '
            '(no such file or directory)'
        )

    def test_conda_find_packages_sp_exception4(
            self, mock_ch, mock_env,
            mock_bin, mock_spr, mock_fsl):
        mock_spr.side_effect = subprocess.CalledProcessError(
            1,
            [
                '/usr/local/fsl/fslpython/envs/fslpython/bin/conda',
                'search',
                '--json',
                '-c',
                'fsl',
                'fsl_sub'
            ],
            output='''
{
    "message": "Failed to find package"
}
'''
        )
        with self.assertRaises(PackageError) as context:
            fsl_sub.utils.conda_find_packages('fsl_sub')
        self.assertEqual(
            str(context.exception),
            'Unable to search for packages! '
            '''(Failed to find package)'''
            )

    def test_conda_find_packages_sp_exception5(
            self, mock_ch, mock_env,
            mock_bin, mock_spr, mock_fsl):
        mock_spr.return_value = subprocess.CompletedProcess(
            [
                '/usr/local/fsl/fslpython/envs/fslpython/bin/conda',
                'search',
                '--json',
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
            fsl_sub.utils.conda_find_packages('fsl_sub')
        self.assertEqual(
            str(context.exception),
            'Unable to search for packages! '
            '''(Expecting property name enclosed '''
            '''in double quotes: line 3 column 5 (char 7))'''
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
    'fsl_sub.utils.conda_fsl_env', auto_space=True,
    return_value="/usr/local/fsl/fslpython/envs/fslpython"
    )
@patch(
    'fsl_sub.utils.conda_channel', auto_space=True,
    return_value="fsl"
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

    def test_conda_update(
            self, mock_ch, mock_env,
            mock_bin, mock_spr, mock_fsl):
        mock_spr.return_value = subprocess.CompletedProcess(
            [
                '/usr/local/fsl/fslpython/envs/fslpython/bin/conda',
                'update',
                '--json',
                '-q',
                '-y',
                '-p',
                '/usr/local/fsl/fslpython/envs/fslpython'
                '-c',
                'fsl',
                'fsl_sub'
            ],
            0,
            stdout=self.example_update
        )
        self.assertEqual(
            fsl_sub.utils.conda_update(
                'fsl_sub',
                fsldir='/usr/local/fsl'),
            {'fsl_sub': {'version': '2.0.0', 'old_version': '1.0.0', }, }
            )

    def test_conda_update_noupdates(
            self, mock_ch, mock_env,
            mock_bin, mock_spr, mock_fsl):
        mock_spr.return_value = subprocess.CompletedProcess(
            [
                '/usr/local/fsl/fslpython/envs/fslpython/bin/conda',
                'update',
                '--json',
                '-q',
                '-y',
                '-p',
                '/usr/local/fsl/fslpython/envs/fslpython'
                '-c',
                'fsl',
                'fsl_sub'
            ],
            0,
            stdout='''
{
    "message": "All requested packages already installed."
}
'''
        )
        self.assertIsNone(
            fsl_sub.utils.conda_update(
                'fsl_sub',
                fsldir='/usr/local/fsl')
            )

    def test_conda_update_all(
            self, mock_ch, mock_env,
            mock_bin, mock_spr, mock_fsl):
        mock_spr.return_value = subprocess.CompletedProcess(
            [
                '/usr/local/fsl/fslpython/envs/fslpython/bin/conda',
                'update',
                '--json',
                '-q',
                '-y',
                '-p',
                '/usr/local/fsl/fslpython/envs/fslpython'
                '-c',
                'fsl',
                '--all'
            ],
            0,
            stdout=self.example_update
        )
        self.assertEqual(
            fsl_sub.utils.conda_update(
                fsldir='/usr/local/fsl'),
            {'fsl_sub': {'version': '2.0.0', 'old_version': '1.0.0', }, }
            )

    def test_conda_update_all_nofsldir(
            self, mock_ch, mock_env,
            mock_bin, mock_spr, mock_fsl):
        mock_spr.return_value = subprocess.CompletedProcess(
            [
                '/usr/local/fsl/fslpython/envs/fslpython/bin/conda',
                'update',
                '--json',
                '-q',
                '-y',
                '-p',
                '/usr/local/fsl/fslpython/envs/fslpython'
                '-c',
                'fsl',
                '--all'
            ],
            0,
            stdout=self.example_update
        )
        self.assertEqual(
            fsl_sub.utils.conda_update(),
            {'fsl_sub': {'version': '2.0.0', 'old_version': '1.0.0', }, }
            )

    def test_conda_update_sp_exception(
            self, mock_ch, mock_env,
            mock_bin, mock_spr, mock_fsl):
        mock_spr.side_effect = subprocess.CalledProcessError(
            1,
            [
                '/usr/local/fsl/fslpython/envs/fslpython/bin/conda',
                'update',
                '--json',
                '-q',
                '-y',
                '-p',
                '/usr/local/fsl/fslpython/envs/fslpython',
                '-c',
                'fsl',
                '--all'
            ],
            output='''no such file or directory'''
        )
        with self.assertRaises(UpdateError) as context:
            fsl_sub.utils.conda_update()
        self.assertEqual(
            str(context.exception),
            'Unable to update! '
            '(no such file or directory)'
        )

    def test_conda_update_sp_exception2(
            self, mock_ch, mock_env,
            mock_bin, mock_spr, mock_fsl):
        mock_spr.side_effect = subprocess.CalledProcessError(
            1,
            [
                '/usr/local/fsl/fslpython/envs/fslpython/bin/conda',
                'update',
                '--json',
                '-q',
                '-y',
                '-p',
                '/usr/local/fsl/fslpython/envs/fslpython',
                '-c',
                'fsl',
                '--all'
            ],
            output='''a warning

{
    "message": "Failed to find package"
}
'''
        )
        with self.assertRaises(UpdateError) as context:
            fsl_sub.utils.conda_update()
        self.assertEqual(
            str(context.exception),
            'Unable to update! '
            '''(a warning

{
    "message": "Failed to find package"
}
)'''
            )

    def test_conda_update_exception3(
            self, mock_ch, mock_env,
            mock_bin, mock_spr, mock_fsl):
        mock_spr.side_effect = subprocess.CalledProcessError(
            1,
            [
                '/usr/local/fsl/fslpython/envs/fslpython/bin/conda',
                'update',
                '--json',
                '-q',
                '-y',
                '-p',
                '/usr/local/fsl/fslpython/envs/fslpython',
                '-c',
                'fsl',
                'fsl_sub'
            ],
            stderr='''no such file or directory'''
        )
        with self.assertRaises(UpdateError) as context:
            fsl_sub.utils.conda_update('fsl_sub')
        self.assertEqual(
            str(context.exception),
            'Unable to update! '
            '(no such file or directory)'
        )

    def test_conda_update_sp_exception4(
            self, mock_ch, mock_env,
            mock_bin, mock_spr, mock_fsl):
        mock_spr.side_effect = subprocess.CalledProcessError(
            1,
            [
                '/usr/local/fsl/fslpython/envs/fslpython/bin/conda',
                'update',
                '--json',
                '-q',
                '-y',
                '-p',
                '/usr/local/fsl/fslpython/envs/fslpython',
                '-c',
                'fsl',
                'fsl_sub'
            ],
            output='''
{
    "message": "Failed to find package"
}
'''
        )
        with self.assertRaises(UpdateError) as context:
            fsl_sub.utils.conda_update('fsl_sub')
        self.assertEqual(
            str(context.exception),
            'Unable to update! '
            '''(Failed to find package)'''
            )

    def test_conda_update_sp_exception5(
            self, mock_ch, mock_env,
            mock_bin, mock_spr, mock_fsl):
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
        with self.assertRaises(UpdateError) as context:
            fsl_sub.utils.conda_update('fsl_sub')
        self.assertEqual(
            str(context.exception),
            'Unable to check for updates '
            '''(Expecting property name enclosed '''
            '''in double quotes: line 3 column 5 (char 7))'''
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
    'fsl_sub.utils.conda_fsl_env', auto_space=True,
    return_value="/usr/local/fsl/fslpython/envs/fslpython"
    )
@patch(
    'fsl_sub.utils.conda_channel', auto_space=True,
    return_value="fsl"
    )
class TestCondaInstall(unittest.TestCase):
    def setUp(self):
        self.example_install = '''{
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
    "PREFIX": "/usr/local/fsl/fslpython/envs/fslpython"
  },
  "prefix": "/usr/local/fsl/fslpython/envs/fslpython",
  "success": true
}
'''

    def test_conda_install(
            self, mock_ch, mock_env,
            mock_bin, mock_spr, mock_fsl):
        mock_spr.return_value = subprocess.CompletedProcess(
            [
                '/usr/local/fsl/fslpython/envs/fslpython/bin/conda',
                'install',
                '--json',
                '-q',
                '-y',
                '-p',
                '/usr/local/fsl/fslpython/envs/fslpython'
                '-c',
                'fsl',
                'fsl_sub'
            ],
            0,
            stdout=self.example_install
        )
        self.assertEqual(
            fsl_sub.utils.conda_install(
                'fsl_sub',
                fsldir='/usr/local/fsl'),
            {'fsl_sub': {'version': '2.0.0', }, }
            )

    def test_conda_install_list(
            self, mock_ch, mock_env,
            mock_bin, mock_spr, mock_fsl):
        mock_spr.return_value = subprocess.CompletedProcess(
            [
                '/usr/local/fsl/fslpython/envs/fslpython/bin/conda',
                'install',
                '--json',
                '-q',
                '-y',
                '-p',
                '/usr/local/fsl/fslpython/envs/fslpython'
                '-c',
                'fsl',
                'fsl_sub',
                'fsl_sub_plugin_sge'
            ],
            0,
            stdout=self.example_install
        )
        self.assertEqual(
            fsl_sub.utils.conda_install(
                ['fsl_sub', 'fsl_sub_plugin_sge', ],
                fsldir='/usr/local/fsl'),
            {'fsl_sub': {'version': '2.0.0', }, }
            )

    def test_conda_install_alreadythere(
            self, mock_ch, mock_env,
            mock_bin, mock_spr, mock_fsl):
        mock_spr.return_value = subprocess.CompletedProcess(
            [
                '/usr/local/fsl/fslpython/envs/fslpython/bin/conda',
                'install',
                '--json',
                '-q',
                '-y',
                '-p',
                '/usr/local/fsl/fslpython/envs/fslpython'
                '-c',
                'fsl',
                'fsl_sub'
            ],
            0,
            stdout='''
{
    "message": "All requested packages already installed."
}
'''
        )
        self.assertIsNone(
            fsl_sub.utils.conda_install(
                'fsl_sub',
                fsldir='/usr/local/fsl')
            )

    def test_conda_update_all_nofsldir(
            self, mock_ch, mock_env,
            mock_bin, mock_spr, mock_fsl):
        mock_spr.return_value = subprocess.CompletedProcess(
            [
                '/usr/local/fsl/fslpython/envs/fslpython/bin/conda',
                'install',
                '--json',
                '-q',
                '-y',
                '-p',
                '/usr/local/fsl/fslpython/envs/fslpython'
                '-c',
                'fsl',
                'fsl_sub'
            ],
            0,
            stdout=self.example_install
        )
        self.assertEqual(
            fsl_sub.utils.conda_install('fsl_sub'),
            {'fsl_sub': {'version': '2.0.0', }, }
            )

    def test_conda_install_sp_exception(
            self, mock_ch, mock_env,
            mock_bin, mock_spr, mock_fsl):
        mock_spr.side_effect = subprocess.CalledProcessError(
            1,
            [
                '/usr/local/fsl/fslpython/envs/fslpython/bin/conda',
                'install',
                '--json',
                '-q',
                '-y',
                '-p',
                '/usr/local/fsl/fslpython/envs/fslpython',
                '-c',
                'fsl',
                'fsl_sub'
            ],
            output='''no such file or directory'''
        )
        with self.assertRaises(InstallError) as context:
            fsl_sub.utils.conda_install('fsl_sub')
        self.assertEqual(
            str(context.exception),
            'Unable to install! '
            '(no such file or directory)'
        )

    def test_conda_install_sp_exception2(
            self, mock_ch, mock_env,
            mock_bin, mock_spr, mock_fsl):
        mock_spr.side_effect = subprocess.CalledProcessError(
            1,
            [
                '/usr/local/fsl/fslpython/envs/fslpython/bin/conda',
                'install',
                '--json',
                '-q',
                '-y',
                '-p',
                '/usr/local/fsl/fslpython/envs/fslpython',
                '-c',
                'fsl',
                'fsl_sub'
            ],
            output='''a warning

{
    "message": "Failed to find package",
    "success": false
}
'''
        )
        with self.assertRaises(InstallError) as context:
            fsl_sub.utils.conda_install('fsl_sub')
        self.assertEqual(
            str(context.exception),
            'Unable to install! '
            '''(a warning

{
    "message": "Failed to find package",
    "success": false
}
)'''
            )

    def test_conda_install_exception3(
            self, mock_ch, mock_env,
            mock_bin, mock_spr, mock_fsl):
        mock_spr.side_effect = subprocess.CalledProcessError(
            1,
            [
                '/usr/local/fsl/fslpython/envs/fslpython/bin/conda',
                'install',
                '--json',
                '-q',
                '-y',
                '-p',
                '/usr/local/fsl/fslpython/envs/fslpython',
                '-c',
                'fsl',
                'fsl_sub'
            ],
            stderr='''no such file or directory'''
        )
        with self.assertRaises(InstallError) as context:
            fsl_sub.utils.conda_install('fsl_sub')
        self.assertEqual(
            str(context.exception),
            'Unable to install! '
            '(no such file or directory)'
        )

    def test_conda_install_sp_exception4(
            self, mock_ch, mock_env,
            mock_bin, mock_spr, mock_fsl):
        mock_spr.side_effect = subprocess.CalledProcessError(
            1,
            [
                '/usr/local/fsl/fslpython/envs/fslpython/bin/conda',
                'install',
                '--json',
                '-q',
                '-y',
                '-p',
                '/usr/local/fsl/fslpython/envs/fslpython',
                '-c',
                'fsl',
                'fsl_sub'
            ],
            output='''
{
    "message": "Failed to find package",
    "success": false
}
'''
        )
        with self.assertRaises(InstallError) as context:
            fsl_sub.utils.conda_install('fsl_sub')
        self.assertEqual(
            str(context.exception),
            'Unable to install! '
            '''(Failed to find package)'''
            )

    def test_conda_install_sp_exception5(
            self, mock_ch, mock_env,
            mock_bin, mock_spr, mock_fsl):
        mock_spr.return_value = subprocess.CompletedProcess(
            [
                '/usr/local/fsl/fslpython/envs/fslpython/bin/conda',
                'install',
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
    'message': "Failed to find package",
    "success": true
}
'''
        )
        with self.assertRaises(InstallError) as context:
            fsl_sub.utils.conda_install('fsl_sub')
        self.assertEqual(
            str(context.exception),
            'Unable to install '
            '''(Expecting property name enclosed '''
            '''in double quotes: line 3 column 5 (char 7))'''
            )

    def test_conda_install_sp_exception6(
            self, mock_ch, mock_env,
            mock_bin, mock_spr, mock_fsl):
        mock_spr.return_value = subprocess.CompletedProcess(
            [
                '/usr/local/fsl/fslpython/envs/fslpython/bin/conda',
                'install',
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
    "message": "Failed to find package",
    "success": false
}
'''
        )
        with self.assertRaises(InstallError) as context:
            fsl_sub.utils.conda_install('fsl_sub')
        self.assertEqual(
            str(context.exception),
            'Unable to install '
            '''(Failed to find package)'''
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
    'fsl_sub.utils.conda_fsl_env', auto_space=True,
    return_value="/usr/local/fsl/fslpython/envs/fslpython"
    )
class TestCondaChannel(unittest.TestCase):
    def test_conda_channel(
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
                fsl_sub.utils.conda_channel(fsldir='/opt/fsl'),
                'https://fsl.fmrib.ox.ac.uk/fsldownloads/fslconda/channel'
            )

    def test_conda_channel_missing(
            self, mock_env,
            mock_cbin, mock_spr, mock_fsldir):
        m_open = mock_open(read_data='''name: fslpython
channels:
 - defaults
 - conda-forge
dependencies:
 - python=3.5.2
 ''')
        with patch('fsl_sub.utils.open', m_open):
            self.assertRaises(
                NoChannelFound,
                fsl_sub.utils.conda_channel,
                fsldir='/opt/fsl'
            )

    def test_conda_channel_filemissing(
            self, mock_env,
            mock_cbin, mock_spr, mock_fsldir):
        m_open = mock_open(read_data='')
        with patch('fsl_sub.utils.open', m_open):
            m_open.side_effect = IOError()
            self.assertRaises(
                NoCondaEnvFile,
                fsl_sub.utils.conda_channel,
                fsldir='/opt/fsl',
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
    'fsl_sub.utils.conda_channel', auto_space=True,
    return_value="fsl"
    )
@patch(
    'fsl_sub.utils.conda_fsl_env', auto_space=True,
    return_value="/usr/local/fsl/fslpython/envs/fslpython"
    )
class TestConda(unittest.TestCase):
    def test_conda_check_update_conda_binfails(
        self, mock_env, mock_cchannel, mock_cbin, mock_spr, mock_fsldir
    ):
        mock_spr.side_effect = subprocess.CalledProcessError(
            cmd='conda',
            returncode=1,
            output='',
            stderr='-bash: conda: command not found'
        )
        self.assertRaises(
            UpdateError,
            fsl_sub.utils.conda_check_update)

    def test_conda_check_update_conda_noupdate(
            self, mock_env, mock_cchannel, mock_cbin, mock_spr, mock_fsldir):
        mock_spr.return_value = subprocess.CompletedProcess(
            [],
            0,
            '''{
  "message": "All requested packages already installed.",
  "success": true
}''',
            '')
        self.assertIsNone(
            fsl_sub.utils.conda_check_update(packages='fsl_sub')
        )
        mock_spr.assert_called_with(
            [
                '/usr/local/fsl/fslpython/envs/fslpython/bin/conda',
                'update',
                '--json',
                '-q',
                '-p',
                '/usr/local/fsl/fslpython/envs/fslpython',
                '-c',
                'fsl',
                '--dry-run',
                'fsl_sub',
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
            universal_newlines=True
        )

    def test_conda_check_update_conda_updates(
            self, mock_env, mock_cchannel, mock_cbin, mock_spr, mock_fsldir):
        mock_spr.return_value = subprocess.CompletedProcess(
            [],
            0,
            '''{
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
        "base_url": null,
        "build_number": 0,
        "build_string": "0",
        "channel": "fsl",
        "dist_name": "fsl_sub-2.0.0",
        "name": "fsl_sub",
        "platform": null,
        "version": "2.0.0"
      }
    ],
    "PREFIX": "/path/to/env",
    "UNLINK": [
      {
        "base_url": null,
        "build_number": 1,
        "build_string": "1",
        "channel": "fsl",
        "dist_name": "fsl_sub-1.0.0",
        "name": "fsl_sub",
        "platform": null,
        "version": "1.0.0"
      }
    ]
  },
  "prefix": "/path/to/env",
  "success": true
}
''',
            '')


class TestPlugins(unittest.TestCase):
    @patch(
            'fsl_sub.utils.available_plugins', autospec=True,
            return_value=['a', 'b', ])
    def test_available_plugin_packages(self, mock_ap):
        self.assertListEqual(
            fsl_sub.utils.available_plugin_packages(),
            ['fsl_sub_plugin_a', 'fsl_sub_plugin_b', ]
        )

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


if __name__ == '__main__':
    unittest.main()
