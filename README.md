# fsl_sub

Job submission to cluster queues
_Copyright 2018, University of Oxford (Duncan Mortimer)_

## Introduction

fsl_sub provides a consistent interface to various cluster backends, with a fall back to running tasks locally where no cluster is available.
If you wish to submit tasks to a cluster you will need to install and configure an appropriate grid backend plugin, two of which are provided alongside fsl_sub:

* fsl_sub_plugin_sge for Sun/Son of/Univa Grid Engine (Grid Engine)
* fsl_sub_plugin_slurm for Slurm

## Requirements

fsl_sub requires Python >=3.5 and PyYAML >=3.12

## Installation

### Installation within FSL

FSL ships with fsl_sub pre-installed but lacking any grid backends. If you wish to use fsl_sub with Grid Engine or Slurm you can easily install one of our provided backends.

* Grid Engine:

> $FSLDIR/fslpython/bin/conda install -n $FSLDIR/fslpython/envs/fslpython fsl_sub_plugin_sge
Where fsl_sub is to be used outside of the FSL distribution it is recommended that it is installed within a Conda or virtual environment.

### Installation in a virtual environment

Using Python 3.5+, create a virtual environment with:

> python -m venv /path/to/my/venv

(if you have multiple Python versions available, you can often choose 3.5 or 3.6 with _python3.5_ or _python3.6_)

Now activate this virtual environment with:

> source activate /path/to/my/venv/bin/activate

and fsl_sub can be installed with:

> pip install git+ssh://git@git.fmrib.ox.ac.uk/fsl/fsl_sub.git

### Installation with Conda

First, install Miniconda from <https://conda.io/miniconda.html>, install as per their instructions then create an environment and activate:

> conda create -n fsl_sub python=3.6
> source activate fsl_sub

and install fsl_sub with:

> conda install -c https://fsl.fmrib.ox.ac.uk/fsldownloads/fslconda/channel fsl_sub

### Installing plugins

If you only need to run programs locally, fsl_sub ships with a local job plugin, but if you wish to target a grid backend you need to install an appropriate plugin. At this time there are two plugins available, one for Son of/Univa/Sun Grid Engine and one for Slurm. To install a plugin, ensure your environment is activated and then install the plugin with:

#### virtualenv

> pip install git+ssh://git@git.fmrib.ox.ac.uk/fsl/fsl_sub_plugin_sge.git

#### conda

> conda install fsl_sub_plugin_sge

(or _fsl_sub_plugin_slurm.git_ and _fsl_sub_plugin_slurm_ respectively)

### Configuration

A configuration file in YAML format is required to describe your cluster environment, examples are provided with the plugins and an example for the installed plugin can be generated with:

> fsl_sub_config _backend\_name_

where _backend\_name_ is the last component of the plugin packaged, e.g. sge.

To write this to a file use:

> fsl_sub_config _backend\_name_ > fsl_sub.yml

This configuration file can be copied to _fsldir_/etc/fslconf calling it fsl_sub.yml, or
put in your home folder calling it .fsl_sub.yml. A copy in your home folder will override the file in _fsldir_/etc/fslconf.

Finally, the environment variable FSLSUB_CONF can be set to point at the configuration
file, this will override all other files.

#### Configuration Sections

##### Top Level

The top level of the configuration file defines the following:

* method: Name of plugin to use - _None_ for no cluster submission engine
* ram_units: Default G, one of K/M/G/T/P - When specifiying memory what units will this be in (Kilobytes, Megabytes, Gigabytes, Terabytes, Petabytes)
* modulecmd: Default False, False or path to _module_ program - If you use _shell modules_ to configure your shell environment and the _module_ program is not in your default search path, set this to the full path of the _module_ program/
* thread_control: Default None, None or list of environment variables to set to the requested number of threads, e.g.:

  thread_control:
    \- OMP_NUM_THREADS
    \- MKL_NUM_THREADS
    \- MKL_DOMAIN_NUM_THREADS
    \- OPENBLAS_NUM_THREADS
    \- GOTO_NUM_THREADS

##### Method Options

The next section, _method\_opts_ defines options for your grid submission engine. If you are not using a grid submission engine then the _None_ sub-section will be used.
If you have requested an example configuration script from your grid submission plugin of choice then the appropriate section will have all the expected configuration options listed with descriptions of their expected values.
For the default _None_ engine, the options should be left as below:

  None:
    queues: False
    large_job_split_pe: None
    mail_support: False
    map_ram: False
    job_priorities: False
    array_holds: False
    array_limit: False
    architecture: False
    job_resources: False
    script_conf: False
    projects: False

##### Coprocessor Options

The next section, _coproc\_opts_ defines options for coprocessor hardware. Information on how to configure this is provided in the plugin documentation.

##### Queue Options

The final section defines the queues (referred to as partitions in Slurm) available on your cluster. See the plugin documentation for details on the settings required.

## Building

Prepare the source distribution

> python setup.py sdist

To build a wheel you need to install wheel into your Python build environment

> pip install wheel

fsl_sub is only compatible with python 3 so you will be building a Pure Python Wheel

> python setup.py bdist_wheel

## Advanced Usage

### Specifying Memory Requirements

If fsl_sub is being called from within a software package such that you have no ability to specify memory requirements then you can achieve this by setting the environment variable FSLSUB_MEMORY_REQUIRED, e.g.

```bash
FSLSUB_MEMORY_REQUIRED=32G myscript_that_submits
```

If units are not specified then they will default to those configured in the YAML file.
If the memory is also specified in the fsl_sub arguments then the argument provided value will be used.

### Array task sub-task ID

When running as an array task you may wish to know which sub-task you are, this might be useful to automatically determine which member of the array you are. This information is provided in environment variables by the queuing software. To determine which variable to read, fsl_sub will set the following environment variables to the name of the equivalent queue variable:

* `FSLSUB_ARRAYTASKID_VAR`: The ID of the sub-task
* `FSLSUB_ARRAYSTARTID_VAR`: The ID of the first sub-task
* `FSLSUB_ARRAYENDID_VAR`: The ID of the last sub-task
* `FSLSUB_ARRAYSTEPSIZE_VAR`: The step between sub-task IDs (not available for all plugins)
* `FSLSUB_ARRAYCOUNT_VAR`: The number of tasks in the array (not available for all plugins)

Not all variables are set by all queue backends so ensure your software can cope with missing variables.