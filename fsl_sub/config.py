# fsl_sub python module
# Copyright (c) 2018, University of Oxford (Duncan Mortimer)

import os
import os.path
from shutil import which
import subprocess as sp
import warnings
import yaml

from fsl_sub.exceptions import (BadConfiguration, MissingConfiguration, )
from fsl_sub.utils import (
    get_plugin_example_conf,
    get_plugin_queue_defs,
    get_plugin_already_queued,
    available_plugins,
    add_nl,
    merge_dict,
)
from functools import lru_cache


def find_config_file():
    # Find most appropriate config file
    search_path = []
    try:
        env_config = os.environ['FSLSUB_CONF']
        search_path.append(env_config)
    except KeyError:
        pass

    search_path.append(
        os.path.join(
            os.path.expanduser("~"),
            '.fsl_sub.yml')
    )

    try:
        fsl_dir = os.environ['FSLDIR']
        default_conf = os.path.realpath(
            os.path.join(fsl_dir, 'etc', 'fslconf', 'fsl_sub.yml')
        )
        search_path.append(
            os.path.abspath(default_conf)
        )
    except KeyError:
        pass
    search_path.append(
        os.path.abspath(
            os.path.join(
                os.path.realpath(__file__),
                os.path.pardir,
                'plugins',
                'fsl_sub_shell.yml')))

    for p in search_path:
        if os.path.exists(p):
            return p

    raise MissingConfiguration("Unable to find fsl_sub config")


def _internal_config_file(filename):
    return os.path.join(os.path.realpath(os.path.dirname(__file__)), filename)


def load_default_config():
    dc_file = _internal_config_file("default_config.yml")
    dcc_file = _internal_config_file("default_coproc_config.yml")
    default_config = {}
    for d_conf_f in (dc_file, dcc_file, ):
        try:
            with open(d_conf_f, 'r') as yaml_source:
                default_config = merge_dict(
                    default_config, yaml.safe_load(yaml_source))
        except yaml.YAMLError as e:
            raise BadConfiguration(
                "Unable to understand default configuration: " + str(e))
        except FileNotFoundError:
            raise MissingConfiguration(
                "Unable to find default configuration file: " + d_conf_f)
        except PermissionError:
            raise MissingConfiguration(
                "Unable to open default configuration file: " + d_conf_f)

    for plugin in available_plugins():
        try:
            p_dc = yaml.safe_load(get_plugin_example_conf(plugin))
        except yaml.YAMLError as e:
            raise BadConfiguration(
                "Unable to understand plugin {0}'s default configuration: ".format(plugin) + str(e))

        default_config = merge_dict(default_config, p_dc)

    default_config['method'] = 'shell'
    return default_config


@lru_cache()
def read_config():
    default_config = load_default_config()
    config_file = find_config_file()
    try:
        with open(config_file, 'r') as yaml_source:
            config_dict = yaml.safe_load(yaml_source)
    except IsADirectoryError:
        raise BadConfiguration(
            "Unable to open configuration file - "
            "looks like FSLSUB_CONF may be pointing at a directory? " + config_file)
    except yaml.YAMLError as e:
        raise BadConfiguration(
            "Unable to understand configuration file: " + str(e))
    except (FileNotFoundError, PermissionError, ):
        raise BadConfiguration(
            "Unable to open configuration file: " + config_file
        )
    except MissingConfiguration:
        config_dict = {}
    this_config = merge_dict(default_config, config_dict)
    if config_dict.get('coproc_opts', {}):
        if 'cuda' not in config_dict['coproc_opts'].keys():
            if 'cuda' not in config_dict.get('silence_warnings', []):
                warnings.warn(
                    '(cuda) Coprocessors configured but no "cuda" coprocessor found. '
                    'FSL tools will not be able to autoselect CUDA versions of software.')
    return this_config


def method_config(method):
    '''Returns the configuration dict for the requested submission
    method, e.g. sge'''
    try:
        m_opts = read_config()['method_opts']
    except KeyError:
        raise BadConfiguration(
            "Unable to find method configuration dictionary"
        )
    try:
        return m_opts[method]
    except KeyError:
        raise BadConfiguration(
            "Unable to find configuration for {}".format(method)
        )


def _read_config_file(fname):
    '''Return content of file as string'''
    try:
        with open(fname, 'r') as default_source:
            e_conf = default_source.read().strip()
    except FileNotFoundError:
        raise MissingConfiguration(
            "Unable to find default configuration file: " + fname
        )
    return e_conf


def example_config(method=None):
    '''Merges the method default config output with the general defaults and returns
    the example config as a string'''
    methods = ['shell', ]
    e_conf = ''

    # Example config files
    dc_file = _internal_config_file("default_config.yml")
    dcc_file = _internal_config_file("default_coproc_config.yml")
    qc_file = _internal_config_file("example_queue_config.yml")
    cc_file = _internal_config_file("example_coproc_config.yml")

    # Load top-level config
    e_conf = _read_config_file(dc_file)
    e_conf = add_nl(e_conf)
    # If a plugin method has been provided set 'method' to this
    if method is not None and method not in methods:
        methods.append(method)
        for match in ("'shell'", '"shell"', 'shell'):
            e_conf = e_conf.replace("method: {0}\n".format(match), "method: '{0}'\n".format(method))

    # Remove method_opts and re-add at end of file:
    e_conf = e_conf.replace('method_opts: {}\n', '')
    e_conf += 'method_opts:\n'

    # Add the method opts for the methods ('shell' + value of method)
    for m in methods:
        plugin_conf = get_plugin_example_conf(m)

        for match in ("'{0}'".format(m), '"{0}"'.format(m), '{0}'.format(m)):
            plugin_conf = plugin_conf.replace("method: {0}\n".format(match), '')

        plugin_conf = plugin_conf.replace("method_opts:\n", '')
        e_conf = add_nl(e_conf)
        e_conf = e_conf + plugin_conf
        e_conf = add_nl(e_conf)

    dcp_conf = _read_config_file(dcc_file)
    if 'coproc_opts:\n  cuda:\n' in e_conf:
        dcp_conf = dcp_conf.replace('coproc_opts:\n  cuda:\n', '')
    e_conf += dcp_conf.replace('---\n', '')
    e_conf = add_nl(e_conf)

    if method is not None:
        # Add the example co-processor config
        ecp = _read_config_file(cc_file).replace('---\n', '')
        if 'coproc_opts:\n  cuda:\n' in e_conf:
            ecp = ecp.replace('coproc_opts:\n  cuda:\n', '')
        e_conf += ecp
        e_conf = add_nl(e_conf)
        e_conf = e_conf.replace('coproc_opts: {}\n', '')
        # Try to detect queues
        queue_defs = get_plugin_queue_defs(method)
        if queue_defs:
            e_conf += 'queues:\n'
            e_conf += queue_defs
        else:
            # Add the example queue config
            e_conf += _read_config_file(qc_file).replace('---\n', '')
        e_conf = e_conf.replace('queues: {}\n', '')
        e_conf = add_nl(e_conf)
    return e_conf


def has_queues(method=None):
    '''Returns True if method has queues and there are queues defined'''
    config = read_config()
    if method is None:
        method = config['method']
    mconf = method_config(method)
    return mconf['queues'] and config['queues']


def has_coprocessor(coproc):
    '''Is the specified coprocessor available on this system?'''
    config = read_config()
    method = config['method']
    queues = config.get('queues', {})
    coprocs = config.get('coproc_opts', {})
    if get_plugin_already_queued(method):
        method = 'shell'
    if method == 'shell':
        co_conf = coprocs.get(coproc, None)
        if co_conf is not None:
            tester = which(co_conf['presence_test'])
            if tester is None:
                return False
            else:
                output = sp.run(
                    [tester, ]
                )
                if output.returncode != 0:
                    return False
            return True
        else:
            # Unsupported coprocessor
            return False
    if queues:
        return any([(coproc in a.get('copros', {}).keys()) for qname, a in queues.items()])
    else:
        raise BadConfiguration("Grid backend specified but no queues configured")


def uses_projects(method=None):
    '''Returns True if method has projects'''
    if method is None:
        method = read_config()['method']
    m_config = method_config(method)
    return m_config['projects']


def coprocessor_config(coprocessor):
    '''Returns the configuration dict for the requested coprocessor,
    e.g. cuda'''
    try:
        cp_opts = read_config()['coproc_opts']
    except KeyError:
        raise BadConfiguration(
            "Unable to find coprocessor configuration dictionary"
        )
    try:
        return cp_opts[coprocessor]
    except KeyError:
        raise BadConfiguration(
            "Unable to find configuration for {}".format(coprocessor)
        )


def queue_config(queue=None):
    '''Returns the config dict for all queues or the config dict
    for the specified queue'''
    try:
        if queue is None:
            return read_config()['queues']
        else:
            return read_config()['queues'][queue]
    except KeyError:
        if queue is None:
            raise BadConfiguration(
                "Unable to find queue definitions"
            )
        else:
            raise BadConfiguration(
                "Unable to find definition for queue " + queue
            )
