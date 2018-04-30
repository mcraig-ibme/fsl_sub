import importlib
import os
import pkgutil
import re
import shutil
import subprocess
import sys
from math import ceil
from fsl_sub.exceptions import (
    CommandError,
)
from fsl_sub.system import (
    system_stdout,
)


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
        in pkgutil.iter_modules(path=plugin_path)
        if name.startswith('fsl_sub_')
    }
    sys.path = sys_path
    return plugin_dict


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


def human_to_ram(ram, output='M', units='G'):
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
    return (
        ram * 2 ** scale_factors[units] /
        2 ** scale_factors[output])


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
        raise CommandError("Cannot find script/binary " + cmd)


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
                        "of {2}".format(cmd, lineno + 1, cmd_file.name))
    except (IOError, FileNotFoundError) as e:
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
                    ]).strip() == '1':
                return True
        except subprocess.CalledProcessError as e:
            raise CommandError(
                "Error trying to check image file - " +
                str(e))
    return False
