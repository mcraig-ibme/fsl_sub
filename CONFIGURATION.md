# Configuration

## Introduction

When using outside of a cluster environment fsl\_sub requires no configuration, although some options are availble for controlling array task paralellism and CUDA hardware which you may wish to modify. See the section Standalone Configuration. For cluster environments, fsl_sub needs to be told about your queue setup (partitions on SLURM) and any CUDA (or other co-processors), see the Cluster Configuration section.

## Displaying the Current Configuration

At any time you can display the configuration that will apply when you run fsl_sub use the command:

~~~bash
fsl_sub --show_config
~~~

This will produce a YAML format file that could be saved and modified to change how fsl_sub operates.

## Location of Configuration

The YAML configuration file can be located in several places:

| Location | Description |
|----------|-------------|
| _\$FSLDIR/etc/fslconf/fsl\_sub.yml_ | This is one option for a multi-user setup. |
| _\$HOME/.fsl\_sub.yml_ | This is your personal configuration which overrides **all** settings in _\$FSLDIR/etc/fslconf/fsl\_sub.yml_. If you create this file, make sure to base it on the output of `fsl_sub --show_config`. |
| Environment variable _FSLSUB\_CONF_ | If you set _FSLSUB\_CONF_ to the path of your fsl_sub configuration file this will be used in preference to **all** locations above. |

## Standalone Configuration

There are only a few options of interest for non-cluster installs. If you need to change any of these settings create a file $HOME/.fsl_sub.yml with the following content (in YAML format <https://en.wikipedia.org/wiki/YAML>):

~~~yaml
method_opts:
    shell:
        run_parallel: <true|false>
        parallel_disable_matches:
            - '*_gpu'
            < - program name match ... >
~~~

The two options _run\_parallel_ and _parallel\_disable\_matches_ have the following meanings/effects:

| Option  | Description |
|---------|-------------|
| run_parallel | This allows you to enable (true) or disable (false) the ability to run array-task components in separate threads - this would, for example, enable FEAT's FLAME or FDT's bedpostx to utilise multiple CPU cores. By default the same number of jobs as CPU cores on the computer will be run, attempting to honour any CPU masking that may be in effect (on Linux). Threads can be limited using the `--array_limit` fsl_sub option or by setting the environment variable `FSLSUB_PARALLEL` to the maximum number of parallel processes.|
| parallel_disable_matches | Some software must never be run in parallel on a single machine, most notibly software that uses CUDA GPU hardware where only one such device is available.  This YAML list (each entry starts with a '-') is a list of program names, paths or basic wildcard match for a program name that will cause array tasks to run serially. Wildcards are denoted with a _*_ at the start or end of the name (only, mid-name wildcards are not supported) of the program and will match any program ending or starting with this word respectively. Where you wish to match a specific file provide the full path to the program (or wildcarded program). You should **always** include '*_gpu' which will match FSL's GPU accelerated software.

## Cluster Configuration

A configuration file in YAML format (<https://en.wikipedia.org/wiki/YAML>) is required to describe your cluster environment, examples are provided with the plugins and an example for an installed plugin can be generated with:

~~~bash
fsl_sub_config <backend_name>
~~~

where _backend\_name_ is the last component of the plugin packaged, e.g. _sge_. Note, if you have no plugins installed or only one, then `fsl_sub_config` with no arguments will display an appropriate configuration.

To write this to a file use:

~~~bash
fsl_sub_config <backend_name> > fsl_sub.yml
~~~

Where supported (check plugin), appropriate queue definitions will be created. You should check these for correctness, paying attention to any warnings in the comments of the queue definitions.

Place this file in one of the locations detailed in [Location of Configuration](#Location of Configuration) above.

The contents of the _fsl\_sub.yml_ file is as follows:

### Top Level

The top level of the configuration file defines the following:

| Option | Acceptable values (**default**) | Description
|---------|--|-------|
| method | Final component of plugin name | Name of plugin to use - _shell_ for no cluster submission engine, _sge_ or _slurm_ for appropriate installed plugin.
| modulecmd | **False**/_path to modulecmd binary_ | False or path to _modulecmd_ program - If you use _shell modules_ to configure your shell environment and the _modulecmd_ program is not in your default search path, set this to the path of the command, e.g. _/usr/local/bin/modulecmd_.
| thread_control | Null/list of environment variables | The list of environment variables that can be used to limit the number of threads used. By default this includes commonly encountered variables.
| silence_warnings | List of warnings | (Advanced) Silence warnings when generating example configurations.

### Method Options

The next section, _method\_opts_ defines options for your grid submission engine. If you are not using a grid submission engine then the _shell_ sub-section will be used.
If you have requested an example configuration script from your grid submission plugin of choice then the appropriate section will have all the expected configuration options listed with descriptions of their expected values. See the plugin documentation for details about the available settings.

### Shell Plugin Options

The shell plugin options apply for tasks that are submitted from within already queued tasks (e.g. if you were to create an array task of FEAT jobs). See [Standalone Configuration](#Standalone Configuration) for details of the options available.

### Coprocessor Options

This section defines what coprocessors are available in your cluster. This would typically be used to inform fsl\_sub of CUDA resources.

For each coprocessor hardware type you need a sub-section given an identifier than will be used to request this type of coprocessor. For CUDA processors this sub-section *must* be called 'cuda' to ensure that FSL tools can auto-detect and use CUDA hardware/queues, e.g.

~~~yaml
coproc_opts:
  cuda:
    presence_test: nvidia-smi
    uses_modules: false
    module_parent: cuda
~~~

Cluster plugins require additional configuration options which are described in the plugin's documentation.

For standalone installs the above example will be used, allowing fsl\_sub to detect the presence of CUDA hardware but not use shell modules to configure CUDA libraries. If you have shell modules setup for different CUDA library versions then you can change these options as described below:

| Key | Values (**default/recommended**) | Description |
| -- | -- | -- |
| presence\_test | _script/binary path_ | The name of a program that can be used to confirm that the co-processor is available, for example _nvidia-smi_ for CUDA devices. Program needs to return non-zero exit status if there are no available co-processors.
| uses\_modules | **True**/False | Is the coprocessor's software configured using a shell module?
| module\_parent | _String_ | If shell modules are used for configuration, what is the name of the parent module? e.g. _cuda_ if you're modules would be loaded with `module load cuda/10.2`

## Queue Options

The final section defines the queues (referred to as partitions in SLURM) available on your cluster. See the plugin documentation for details on the settings required.
