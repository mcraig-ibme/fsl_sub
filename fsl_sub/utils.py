import importlib
import os
import pkgutil
import shutil
from exceptions import (
    CommandError,
)


def load_plugins():
    plugin_path = []

    try:
        plugin_path.append(os.environ['FSLSUB_PLUGINPATH'])
    except KeyError:
        pass
    plugin_path.append('./plugins')

    return {
        name: importlib.import_module(name)
        for finder, name, ispkg
        in pkgutil.iter_modules(path=plugin_path)
        if name.startswith('fsl_sub_')
    }


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
    except IOError as e:
        raise CommandError("Unable to read command_file")
    return lineno + 1


def control_threads(env_vars, threads):
    '''Set the specified environment variables to the number of
    threads.'''

    for ev in env_vars:
        os.environ[ev] = str(threads)
