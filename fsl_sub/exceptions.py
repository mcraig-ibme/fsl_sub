# fsl_sub python module
# Copyright (c) 2018, University of Oxford (Duncan Mortimer)


class ArgumentError(Exception):
    pass


class LoadModuleError(Exception):
    pass


class NoModule(Exception):
    pass


class PluginError(Exception):
    pass


class BadCoprocessor(Exception):
    pass


class BadConfiguration(Exception):
    pass


class UnrecognisedModule(Exception):
    pass


class BadSubmission(Exception):
    pass


class GridOutputError(Exception):
    pass


class CommandError(Exception):
    pass


class UnknownJobId(Exception):
    pass


class NotAFslDir(Exception):
    pass


class NoCondaEnvFile(Exception):
    pass


class NoChannelFound(Exception):
    pass


class UpdateError(Exception):
    pass


class NoCondaEnv(Exception):
    pass


class PackageError(Exception):
    pass


class InstallError(Exception):
    pass


CONFIG_ERROR = 1
SUBMISSION_ERROR = 2
RUNNER_ERROR = 3
