# fsl_sub python module
# Copyright (c) 2018, University of Oxford (Duncan Mortimer)

import os
import yaml

from fsl_sub.exceptions import BadConfiguration
from functools import lru_cache


def valid_config(config):
    '''Check config file has required entries'''

    tl_keys = [
        'method', 'ram_units',
        'modulecmd', 'thread_control',
        'method_opts', ]

    mopts_keys = [
        'queues', 'large_job_split_pe',
        'mail_support', 'map_ram',
        'job_priorities', 'array_holds',
        'array_limit', 'architecture',
        'job_resources', 'script_conf',
        'projects',
    ]

    copro_opts_keys = [
        'uses_modules', 'classes',
        'no_binding', 'resource',
    ]

    copro_mod_opts_keys = [
        'module_parent'
    ]

    copro_class_opts_keys = [
        'default_class', 'include_more_capable',
        'class_types', 'class_resource'
    ]

    for k in tl_keys:
        if k not in config.keys():
            raise BadConfiguration(
                "Missing {} option in configuration file".format(
                    k
                ))
    for method, conf in config['method_opts'].items():
        for k in mopts_keys:
            if k not in conf.keys():
                raise BadConfiguration(
                    "Missing {0} option in method '{1}'s definition in "
                    "configuration file".format(
                        k, method
                    ))
    if 'coproc_opts' in config.keys():
        for copro, conf in config['coproc_opts'].items():
            for k in copro_opts_keys:
                if k not in conf.keys():
                    raise BadConfiguration(
                        "Missing {0} option in coprocessor {1}s definition in"
                        "configuration file".format(
                            k, method
                        )
                    )

            if conf['uses_modules']:
                for k in copro_mod_opts_keys:
                    if k not in conf.keys():
                        raise BadConfiguration(
                            "Missing {0} option in coprocessor {1}s"
                            " definition in configuration file".format(
                                k, copro
                            )
                        )
            if conf['classes']:
                for k in copro_class_opts_keys:
                    if k not in conf.keys():
                        raise BadConfiguration(
                            "Missing {0} option in coprocessor {1}s"
                            " definition in configuration file".format(
                                k, copro
                            )
                        )

    return True


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
                'fsl_sub_none.yml')))

    for p in search_path:
        if os.path.exists(p):
            return p

    raise BadConfiguration("Unable to find fsl_sub config")


@lru_cache()
def read_config():
    try:
        with open(find_config_file(), 'r') as yaml_source:
            config_dict = yaml.load(yaml_source)
    except Exception as e:
        raise BadConfiguration(
            "Unable to load configuration: " + str(e))
    return config_dict


@lru_cache()
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


@lru_cache()
def has_queues(method=None):
    '''Returns True if method has queues'''
    if method is None:
        method = read_config()['method']
    return method_config(method)['queues']


def uses_projects(method=None):
    '''Returns True if method has projects'''
    if method is None:
        method = read_config()['method']
    m_config = method_config(method)
    return m_config['projects']


@lru_cache()
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


@lru_cache()
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
