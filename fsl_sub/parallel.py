# fsl_sub python module
# Copyright (c) 2018-2021 University of Oxford (Duncan Mortimer)

from fsl_sub.config import (
    queue_config,
)
from fsl_sub.exceptions import (
    ArgumentError,
)


def parallel_envs(queues=None):
    '''Return the list of configured parallel environments
    in the supplied queue definition dict'''
    if queues is None:
        queues = queue_config()
    ll_envs = []
    for q in queues.values():
        try:
            ll_envs.extend(q.get('parallel_envs', []))
        except KeyError:
            pass
    if not ll_envs:
        return None
    return list(set(ll_envs))


def process_pe_def(pe_def, queues):
    '''Convert specified pe,slots into a tuples'''
    pes_defined = parallel_envs(queues)
    try:
        pe = pe_def.split(',')
    except ValueError:
        raise ArgumentError(
            "Parallel environment must be name,slots"
        )
    if pe[0] not in pes_defined:
        raise ArgumentError(
            "Parallel environment name {} "
            "not recognised".format(pe[0])
        )
    try:
        slots = int(pe[1])
    except TypeError:
        raise ArgumentError(
            "Slots requested not an integer"
        )
    return (pe[0], slots, )
