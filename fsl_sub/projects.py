import os
from fsl_sub.config import (
    read_config,
)
from fsl_sub.exceptions import (
    BadConfiguration,
)
from fsl_sub.utils import (
    load_plugins,
)


def project_list():
    PLUGINS = load_plugins()

    config = read_config()

    if config['method'] == 'shell':
        return None

    grid_module = 'fsl_sub_plugin_' + config['method']
    if grid_module not in PLUGINS:
        raise BadConfiguration(
            "{} not a supported method".format(config['method']))

    try:
        projects = PLUGINS[grid_module].project_list()
    except AttributeError:
        raise BadConfiguration(
            "Failed to load plugin " + grid_module
        )

    return projects


def project_exists(project):
    projects = project_list()
    if projects is None:
        return True
    return (project in projects)


def get_project_env(project):
    if project is None:
        try:
            project = os.environ['FSLSUB_PROJECT']
        except KeyError:
            pass
    return project
