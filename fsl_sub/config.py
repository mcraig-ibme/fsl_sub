import os
import yaml

from exceptions import BadConfiguration
from functools import lru_cache


def find_config_file():
    # Find most appropriate config file
    user_home = os.path.expanduser("~")
    personal_config = os.path.join(user_home, '.fsl_sub.yml')
    if os.path.exists(personal_config):
        return personal_config

    try:
        env_config = os.environ['FSLSUB_CONF']
        return env_config
    except KeyError:
        pass

    default_conf = os.path.join(
        os.environ['FSLDIR'], 'etc',
        'fslconf', 'fsl_sub.yml')
    if os.path.exists(default_conf):
        return default_conf
    else:
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
