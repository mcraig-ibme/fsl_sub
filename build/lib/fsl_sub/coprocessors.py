import sys
from collections import defaultdict
from fsl_sub.config import (
    coprocessor_config,
    queue_config,
)
from fsl_sub.exceptions import (
    BadConfiguration,
    UnrecognisedModule
)
from fsl_sub.shell_modules import (
    get_modules,
    load_module
)


def list_coprocessors():
    '''Return a list of coprocessors found in the queue definitions'''

    avail_cops = []

    for q in queue_config().values():
        try:
            avail_cops.extend(q['copros'].keys())
        except KeyError:
            pass

    return avail_cops


def max_coprocessors(coprocessor):
    '''Return the maximum number of coprocessors per node from the
    queue definitions'''

    num_cops = 0

    for q in queue_config().values():
        if 'copros' in q:
            try:
                num_cops = max(
                    num_cops,
                    q['copros'][coprocessor]['max_quantity'])
            except KeyError:
                pass

    return num_cops


def coproc_classes(coprocessor):
    '''Return whether a coprocessor supports multiple classes of hardware.
    Classes are sorted by capability'''
    classes = defaultdict(lambda: 1)
    copro_opts = coprocessor_config(coprocessor)
    for q in queue_config().values():
        if 'copros' in q:
            try:
                for c in q['copros'][coprocessor]['classes']:
                    classes[c] = copro_opts[c]['capability']
            except KeyError:
                continue
    if not classes:
        return None
    return sorted(classes.keys(), key=classes.get)


def coproc_toolkits(coprocessor):
    '''Return list of coprocessor toolkit versions.'''
    copro_conf = coprocessor_config(coprocessor)
    # Check that we have queues configured for this coproceesor
    if not all([q for q in queue_config() if (
            'copros' in q and coprocessor in q['copros'])]):
        raise BadConfiguration(
            "Coprocessor {} not available in any queues".format(
                coprocessor
            )
        )
    if not copro_conf['uses_modules']:
        return None
    return get_modules(copro_conf['module_parent'])


def coproc_class(coproc_class, coproc_classes):
    try:
        for c, i in enumerate(coproc_classes):
            if c['shortcut'] == coproc_class:
                break
    except KeyError:
        raise BadConfiguration(
            "Co-processor class {} not configured".format(coproc_class),
            file=sys.stderr)
    return coproc_classes[:i]


def coproc_load_module(coproc, module_version):
    if coproc['uses_modules']:
        modules_avail = get_modules(coproc['module_parent'])
        if modules_avail:
            if module_version not in modules_avail:
                raise UnrecognisedModule(module_version)
            else:
                load_module("/".join(
                    (coproc['module_parent'], module_version)))
