# fsl_sub python module
# Copyright (c) 2018, University of Oxford (Duncan Mortimer)

import os
import re
import shlex
import shutil
import subprocess
from fsl_sub.exceptions import (
    LoadModuleError,
    NoModule,
)
from fsl_sub.config import (
    read_config,
)
from fsl_sub.system import system_stdout, system_stderr


def find_module_cmd():
    '''Locate the 'modulecmd' binary'''
    mcmd = shutil.which('modulecmd')
    if mcmd is None:
        config = read_config()
        if config['modulecmd']:
            mcmd = config['modulecmd']
        else:
            mcmd = False
    return mcmd


def read_module_environment(lines):
    '''Given output of modulecmd python add ... convert this to a dict'''
    module_env = {}
    regex = re.compile(
        r"os.environ\['(?P<variable>.*)'\] ?= ?'(?P<value>.*)'$")
    for line in lines:
        matches = regex.match(line.strip())
        if matches:
            module_env[matches.group('variable')] = matches.group('value')
    return module_env


def module_add(module_name):
    '''Returns a dict of variable: value describing the environment variables
    necessary to load a shell module into the current environment'''
    module_cmd = find_module_cmd()

    if module_cmd:
        try:
            environment = system_stdout(
                [module_cmd, "python", "add", module_name, ])
        except subprocess.CalledProcessError as e:
            raise LoadModuleError from e
        return read_module_environment(environment)
    else:
        return False


def load_module(module_name):
    '''Load a module into the environment of this python process.'''
    environment = module_add(module_name)
    if environment:
        for k, v in environment.items():
            os.environ[k] = v
        return True
    else:
        return False


def unload_module(module_name):
    '''Remove environment variables associated with module module_name
     from the environment of python process.'''
    environment = module_add(module_name)
    if environment:
        for k in environment:
            del os.environ[k]
        return True
    else:
        return False


def loaded_modules():
    '''Get list of loaded ShellModules'''
    # Modules stored in environment variable LOADEDMODULES
    try:
        modules_string = os.environ['LOADEDMODULES']
    except KeyError:
        return []
    return modules_string.split(':')


def get_modules(module_parent):
    '''Returns a list of available Shell Modules that setup the
    co-processor environment'''
    modules = []
    try:
        available_modules = system_stderr(
            "module -t avail " + shlex.quote(module_parent),
            shell=True)
        if available_modules:
            for line in available_modules:
                line = line.strip()
                if not line:
                    continue
                if ':' in line:
                    continue
                if '/' in line:
                    modules.append(line.split('/')[1])
                else:
                    modules.append(line)
        else:
            raise NoModule(module_parent)
    except subprocess.CalledProcessError:
        raise NoModule(module_parent)
    return sorted(modules)


def latest_module(module_parent):
    '''Return the module string that would load the latest version of a module.
    Returns False if module is determined to be not versioned and raises
    NoModule if module is not found.'''
    try:
        modules = get_modules(module_parent)
        if modules is None:
            return False
        else:
            return modules[-1]
    except NoModule:
        raise


def module_string(module_parent, module_version):
    if module_version:
        return "/".join((module_parent, module_version))
    else:
        return module_parent
