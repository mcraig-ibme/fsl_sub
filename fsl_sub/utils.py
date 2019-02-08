# fsl_sub python module
# Copyright (c) 2018, University of Oxford (Duncan Mortimer)

import importlib
import json
import math
import os
import pkgutil
import re
import shutil
import subprocess
import sys
import yaml
from functools import lru_cache
from math import ceil
from fsl_sub.exceptions import (
    CommandError,
    BadSubmission,
    BadConfiguration,
    InstallError,
    NotAFslDir,
    NoChannelFound,
    NoCondaEnv,
    NoCondaEnvFile,
    NoFsl,
    PackageError,
    UpdateError,
)
from fsl_sub.system import (
    system_stdout,
)


@lru_cache()
def load_plugins():
    plugin_path = []

    if 'FSLSUB_PLUGINPATH' in os.environ:
        plugin_path.extend(os.environ['FSLSUB_PLUGINPATH'].split(':'))
    here = os.path.dirname(os.path.abspath(__file__))
    plugin_path.append(os.path.join(here, 'plugins'))

    sys_path = sys.path
    ppath = list(plugin_path)
    ppath.reverse()
    for p_dir in ppath:
        sys.path.insert(0, p_dir)

    plugin_dict = {
        name: importlib.import_module(name)
        for finder, name, ispkg
        in pkgutil.iter_modules()
        if name.startswith('fsl_sub_plugin')
    }
    sys.path = sys_path
    return plugin_dict


def available_plugins():
    PLUGINS = load_plugins()

    plugs = []
    for p in PLUGINS.keys():
        (_, plugin_name) = p.split('plugin_')
        plugs.append(plugin_name)

    return plugs


def available_plugin_packages():
    return [
        'fsl_sub_plugin_' + a for a in available_plugins() if a.lower != 'none'
    ]


def get_plugin_example_conf(plugin_name):
    PLUGINS = load_plugins()
    grid_module = 'fsl_sub_plugin_' + plugin_name

    if grid_module not in PLUGINS:
        raise CommandError("Plugin {} not found". format(plugin_name))

    try:
        return PLUGINS[grid_module].example_conf()
    except AttributeError:
        raise BadConfiguration(
            "Plugin doesn't provide an example configuration."
        )


def minutes_to_human(minutes):
    if minutes < 60:
        result = "{}m".format(minutes)
    elif minutes < 60 * 24:
        result = "{:.1f}".format(minutes/60)
        (a, b) = result.split('.')
        if b == '0':
            result = a
        result += 'h'
    else:
        result = "{:.1f}".format(minutes/(60 * 24))
        (a, b) = result.split('.')
        if b == '0':
            result = a
        result += 'd'
    return result


def titlize_key(text):
    '''Remove _ and Title case a dict key'''

    return text.replace('_', ' ').title()


def blank_none(text):
    '''Return textual value or blank if value is None'''

    if text is None:
        return ''
    else:
        return str(text)


def human_to_ram(ram, output='M', units='G', as_int=True):
    '''Converts user supplied RAM quantity into output scale'''

    scale_factors = {
        'P': 50,
        'T': 40,
        'G': 30,
        'M': 20,
        'K': 10,
    }
    try:
        units = units.upper()
        output = output.upper()
    except AttributeError:
        raise ValueError("units and output must be strings")
    if units not in scale_factors or output not in scale_factors:
        raise ValueError('Unrecognised RAM multiplier')
    if isinstance(ram, (int, float)):
        ram = str(ram) + units
    if not isinstance(ram, str):
        raise ValueError('Unrecognised RAM string')
    try:
        if '.' in ram:
            float(ram)
        else:
            int(ram)
    except ValueError:
        pass
    else:
        ram = ram + units
    regex = r'(?P<ram>[\d.]+)(?P<units>[GgMmKkTtPp])[iI]?[bB]?'
    h_ram = re.match(regex, ram)
    if h_ram is None:
        raise ValueError("Supplied memory doesn't look right")
    match = h_ram.groupdict()
    units = match['units'].upper()
    try:
        if '.' in match['ram']:
            ram = float(match['ram'])
        else:
            ram = int(match['ram'])
    except ValueError:
        raise ValueError("RAM amount not a valid number")
    size = (
        ram * 2 ** scale_factors[units] /
        2 ** scale_factors[output])
    if as_int:
        size = int(math.ceil(size))
    return size


def affirmative(astring):
    '''Is the given string a pseudonym for yes'''
    answer = astring.lower()
    if answer == 'yes' or answer == 'y' or answer == 'true':
        return True
    else:
        return False


def negative(astring):
    '''Is the given string a pseudonym for no'''
    answer = astring.lower()
    if answer == 'no' or answer == 'n' or answer == 'false':
        return True
    else:
        return False


def check_command(cmd):
    if shutil.which(cmd) is None:
        raise CommandError("Cannot find script/binary '{}'".format(cmd))


def check_command_file(cmds):
    try:
        with open(cmds, 'r') as cmd_file:
            for lineno, line in enumerate(cmd_file.readlines()):
                cmd = line.split()[0]
                try:
                    check_command(cmd)
                except CommandError:
                    raise CommandError(
                        "Cannot find script/binary {0} on line {1}"
                        " of {2}".format(cmd, lineno + 1, cmd_file.name))
    except (IOError, FileNotFoundError):
        raise CommandError("Unable to read '{}'".format(cmds))
    return lineno + 1


def control_threads(env_vars, threads):
    '''Set the specified environment variables to the number of
    threads.'''

    for ev in env_vars:
        os.environ[ev] = str(threads)


def split_ram_by_slots(jram, jslots):
    return int(ceil(jram / jslots))


def file_is_image(filename):
    '''Is the specified file an image file?'''
    if os.path.isfile(filename):
        try:
            if system_stdout(
                command=[
                    os.path.join(
                        os.environ['FSLDIR'],
                        'bin',
                        'imtest'),
                    filename
                    ])[0] == '1':
                return True
        except KeyError:
            raise NoFsl(
                "FSLDIR environment variable not found")
        except subprocess.CalledProcessError as e:
            raise CommandError(
                "Error trying to check image file - " +
                str(e))
    return False


def parse_array_specifier(spec):
    if ':' in spec:
        (jrange, step) = spec.split(':')
        try:
            step = int(step)
        except ValueError:
            raise BadSubmission("Array step must be an integer")
    else:
        step = None
        jrange = spec
    if '-' in jrange:
        (jstart, jend) = jrange.split("-")
        try:
            jstart = int(jstart)
        except ValueError:
            raise BadSubmission("Array start index must be an integer")
        try:
            jend = int(jend)
        except ValueError:
            raise BadSubmission("Array end index must be an integer")
    else:
        jstart = spec
        try:
            jstart = int(jstart)
        except ValueError:
            raise BadSubmission("Array number of tasks must be an integer")
        jend = None
    return (jstart, jend, step)


def user_input(prompt):
    return input(prompt)


@lru_cache()
def find_fsldir():
    fsldir = None
    try:
        fsldir = os.environ['FSLDIR']
    except KeyError:
        while fsldir is None:
            fsldir = user_input(
                "Where is FSL installed? (hit return to cancel) ")
            if fsldir == "":
                raise NotAFslDir()
            if not os.path.exists(
                    os.path.join(fsldir, 'etc', 'fslconf')):
                print("Not an FSL dir.", file=sys.stderr)
                fsldir = None
    return fsldir


def conda_fsl_env(fsldir=None):
    try:
        if fsldir is None:
            fsldir = find_fsldir()
    except NotAFslDir as e:
        raise NoCondaEnv(str(e))

    env_dir = os.path.join(fsldir, 'fslpython', 'envs', 'fslpython')
    if not os.path.exists(env_dir):
        raise NoCondaEnv
    return env_dir


def conda_bin(fsldir=None):
    conda_bin = os.path.join(
        conda_fsl_env(fsldir),
        'bin',
        'conda'
    )
    if not os.path.exists(conda_bin):
        raise NoCondaEnv
    return conda_bin


def conda_channel(fsldir=None):
    try:
        if fsldir is None:
            fsldir = find_fsldir()
    except NotAFslDir as e:
        raise NoCondaEnv(str(e))

    try:
        fsl_pyenv = open(
                os.path.join(
                    fsldir,
                    'etc',
                    'fslconf',
                    'fslpython_environment.yml'),
                "r")
    except Exception as e:
        raise NoCondaEnvFile(
            "Unable to access fslpython_environment.yml file: " +
            str(e))
    conda_env = yaml.load(fsl_pyenv)
    for channel in conda_env['channels']:
        if channel.endswith('fslconda/channel'):
            return channel

    raise NoChannelFound()


def conda_stderr(output):
    '''Finds the actual error in the stderr output of conda --json
    This is often poluted with messages of no interest.'''
    json_lines = []
    json_found = False
    for line in output.splitlines():
        if line.startswith('sl_{') or line.startswith('{'):
            json_found = True
        if json_found:
            if line.startswith('sl_'):
                line = line.replace('sl_', '')
            if line.startswith('}'):
                json_lines.append('}')
                json_found = False
            else:
                json_lines.append(line)
    if json_lines:
        try:
            message_obj = json.loads('\n'.join(json_lines))
        except json.JSONDecodeError:
            message = None
            for line in output.splitlines():
                line = line.strip()
                if line.strip().strip('"').startswith('message'):
                    message = line.split(':')[1].strip().strip('"')
            if message is not None:
                message_obj = {'message': message, }
            else:
                message_obj = {'message': output, }
    else:
        message_obj = {'message': output, }
    try:
        message = message_obj['message']
    except KeyError:
        return output
    return message


def conda_stdout_error(output):
    '''Return the error message in stdout of conda --json'''
    try:
        message_obj = json.loads(output)
        message = message_obj['message']
    except (json.JSONDecodeError, KeyError):
        message = output
    return message


def conda_find_packages(match, fsldir=None):
    try:
        if fsldir is None:
            fsldir = find_fsldir()
    except NotAFslDir as e:
        raise NoCondaEnv(str(e))

    try:
        result = subprocess.run(
            [
                conda_bin(fsldir),
                'search',
                '--json',
                '-c',
                conda_channel(fsldir),
                match,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True)
    except subprocess.CalledProcessError as e:
        if e.stderr is None:
            message = conda_stdout_error(e.output)
        else:
            message = e.stderr
        raise PackageError(
            "Unable to search for packages! ({0})".format(message))

    conda_json = result.stdout
    try:
        conda_result = json.loads(conda_json)
    except json.JSONDecodeError as e:
        raise PackageError(
            "Unable to search for packages! ({0})".format(str(e))
        )
    return list(conda_result.keys())


def conda_check_update(packages=None, fsldir=None):
    try:
        if fsldir is None:
            fsldir = find_fsldir()
    except NotAFslDir as e:
        raise NoCondaEnv(str(e))

    if packages is None:
        packages = ['--all', ]
    if isinstance(packages, str):
        packages = (packages, )

    try:
        result = subprocess.run(
            [
                conda_bin(fsldir),
                'update',
                '--json',
                '-q',
                '-p',
                conda_fsl_env(fsldir),
                '-c',
                conda_channel(fsldir),
                '--dry-run',
                " ".join(packages),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            check=True)
    except subprocess.CalledProcessError as e:
        if e.stderr is None:
            message = conda_stdout_error(e.output)
        else:
            message = e.stderr
        raise UpdateError(
            "Unable to update! ({0})".format(message))

    conda_json = result.stdout
    try:
        conda_result = json.loads(conda_json)
    except json.JSONDecodeError as e:
        raise UpdateError(
            "Unable to check for updates ({0})".format(str(e))
        )
    try:
        if (conda_result['message'] ==
                'All requested packages already installed.'):
            return None
        else:
            to_link = conda_result['actions']['LINK']
            updates = {
                a['name']: {
                    'version': a['version'], } for a in
                to_link
            }
            to_unlink = conda_result['actions']['UNLINK']
            old_versions = {
                a['name']: a['version'] for a in
                to_unlink
            }
            for pkg in updates.keys():
                try:
                    updates[pkg]['old_version'] = old_versions[pkg]
                except KeyError:
                    pass
    except KeyError as e:
        raise UpdateError(
            "Unexpected update output ({0})".format(str(e))
        )
    return updates


def conda_update(packages=None, fsldir=None):
    try:
        if fsldir is None:
            fsldir = find_fsldir()
    except NotAFslDir as e:
        raise NoCondaEnv(str(e))

    if packages is None:
        packages = ['--all', ]
    if isinstance(packages, str):
        packages = (packages, )

    try:
        result = subprocess.run(
            [
                conda_bin(fsldir),
                'update',
                '--json',
                '-q',
                '-y',
                '-p',
                conda_fsl_env(fsldir),
                '-c',
                conda_channel(fsldir),
                packages,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            check=True)
    except subprocess.CalledProcessError as e:
        if e.stderr is None:
            message = conda_stdout_error(e.output)
        else:
            message = conda_stderr(e.stderr)
        raise UpdateError(
            "Unable to update! ({0})".format(message))

    conda_json = result.stdout
    try:
        conda_result = json.loads(conda_json)
    except json.JSONDecodeError as e:
        raise UpdateError(
            "Unable to check for updates ({0})".format(str(e))
        )
    try:
        if ('message' in conda_result and (
                conda_result['message'] ==
                'All requested packages already installed.')):
            return None
        if not conda_result['success']:
            raise UpdateError(conda_result['message'])
        to_link = conda_result['actions']['LINK']
        updates = {
            a['name']: {
                'version': a['version'], } for a in
            to_link
        }
        to_unlink = conda_result['actions']['UNLINK']
        old_versions = {
            a['name']: a['version'] for a in
            to_unlink
        }
        for pkg in updates.keys():
            try:
                updates[pkg]['old_version'] = old_versions[pkg]
            except KeyError:
                pass
        return updates

    except KeyError as e:
        raise UpdateError(
            "Unexpected update output ({0})".format(str(e))
        )


def conda_install(packages, fsldir=None):
    try:
        if fsldir is None:
            fsldir = find_fsldir()
    except NotAFslDir as e:
        raise NoCondaEnv(str(e))

    if isinstance(packages, str):
        packages = (packages, )

    try:
        result = subprocess.run(
            [
                conda_bin(fsldir),
                'install',
                '--json',
                '-q',
                '-y',
                '-p',
                conda_fsl_env(fsldir),
                '-c',
                conda_channel(fsldir),
                packages,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            check=True)
    except subprocess.CalledProcessError as e:
        if e.stderr is None:
            message = conda_stdout_error(e.output)
        else:
            message = conda_stderr(e.stderr)
        raise InstallError(
            "Unable to install! ({0})".format(message))

    conda_json = result.stdout
    try:
        conda_result = json.loads(conda_json)
    except json.JSONDecodeError as e:
        raise InstallError(
            "Unable to install ({0})".format(str(e))
        )
    try:
        if ('message' in conda_result and
                conda_result['message'] ==
                'All requested packages already installed.'):
            return None
        if not conda_result['success']:
            raise InstallError(
                "Unable to install ({0})".format(
                    conda_result['message']
                )
            )
        to_link = conda_result['actions']['LINK']
        updates = {
            a['name']: {
                'version': a['version'], } for a in
            to_link
        }
        return updates

    except KeyError as e:
        raise InstallError(
            "Unexpected update output - {0} missing".format(str(e))
        )
