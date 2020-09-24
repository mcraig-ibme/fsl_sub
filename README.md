# fsl_sub

Job submission to cluster queues
_Copyright 2018-2020, University of Oxford (Duncan Mortimer)_

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

> conda install -c <https://fsl.fmrib.ox.ac.uk/fsldownloads/fslconda/channel> fsl_sub

### Installing plugins

If you only need to run programs locally, fsl_sub ships with a local job plugin, but if you wish to target a grid backend you need to install an appropriate plugin. At this time there are two plugins available, one for Son of/Univa/Sun Grid Engine and one for Slurm. To install a plugin, ensure your environment is activated and then install the plugin with:

#### virtualenv

> pip install git+ssh://git@git.fmrib.ox.ac.uk/fsl/fsl_sub_plugin_sge.git

#### conda

> conda install fsl_sub_plugin_sge

(or _fsl\_sub\_plugin\_slurm.git_ and _fsl\_sub\_plugin\_slurm_ respectively)

### Configuration

A configuration file in YAML format is required to describe your cluster environment, examples are provided with the plugins and an example for an installed plugin can be generated with:

> fsl_sub_config _backend\_name_

where _backend\_name_ is the last component of the plugin packaged, e.g. sge.

To write this to a file use:

> fsl_sub_config _backend\_name_ > fsl_sub.yml

Where supported (check plugin), appropriate queue definitions will be created. You should check these for correctness, paying attention to any warnings in the comments at the start.

This configuration file can be copied to _fsldir_/etc/fslconf calling it fsl_sub.yml, or
put in your home folder calling it .fsl_sub.yml. A copy in your home folder will override the file in _fsldir_/etc/fslconf.

Finally, the environment variable FSLSUB_CONF can be set to point at the configuration
file, this will override all other files.

It is not necessary to specify all the options, the default values are those given in the example configuration output, but as a bare minimum you will need, to specify the method (default 'shell') of running jobs and in the case of a cluster submission plugin, a list of queues available on the system.

```yaml
method: <plugin method>
queues:
    aqueue:
        time: 10000
```

Where \<plugin method> is the name of the plugin, e.g. 'sge'.

See the plugin documentation for details on how to configure the plugin, using the 'method_opts' section, e.g:

```yaml
method_ops:
    'sge':
        queues: true
```

See below for details.

#### Configuration Sections

##### Top Level

The top level of the configuration file defines the following:

* method: Name of plugin to use - _shell_ for no cluster submission engine
* ram_units: Default G, one of K/M/G/T/P - When specifiying memory what units will this be in (Kilobytes, Megabytes, Gigabytes, Terabytes, Petabytes)
* modulecmd: Default False, False or path to _module_ program - If you use _shell modules_ to configure your shell environment and the _module_ program is not in your default search path, set this to the full path of the _module_ program/
* thread_control: Default Null, Null or list of environment variables to set to the requested number of threads, e.g.:

    ```yaml
    thread_control:
        - OMP_NUM_THREADS
        - MKL_NUM_THREADS
        - MKL_DOMAIN_NUM_THREADS
        - OPENBLAS_NUM_THREADS
        - GOTO_NUM_THREADS
    ```

* silence_warnings: List of warnings to silence - name is the name given in () at the start of the
  warning message, e.g.

  ```yaml
  silence_warnings:
    - 'cuda'
  ```

##### Method Options

The next section, _method\_opts_ defines options for your grid submission engine. If you are not using a grid submission engine then the _shell_ sub-section will be used.
If you have requested an example configuration script from your grid submission plugin of choice then the appropriate section will have all the expected configuration options listed with descriptions of their expected values.

###### Shell Plugin

If you don't have a cluster and are running jobs locally then the built-in _shell_ job runner can offer some basic parallelisation of array tasks.
To change the operation of these plugin, create a .fsl_sub_yml file in your home folder (or in another location/filename with the environment variable FSLSUB_CONF set to the full path to the file) with the following content:

```yaml
method_opts:
  shell:
    run_parallel: True
    parallel_disable_matches:
      - '*_gpu'
```

These two options can take values as below:

* `run_parallel`: This turns on (True) or off (False) the ability to run array tasks in parallel
* `parallel_disable_matches`: This takes a list of patterns that match program names that *must* not be parallelised. By default the GPU optimised FSL programs will run linearly (programs ending `_gpu`) as these are often a limited resource. This takes a list of matches which may start or end with a '*' to match end or start of program name respectively), full paths can be specified to identify specific installs of a program.

The shell plugin will attempt to run up-to the same number of jobs as CPU cores on the computer, attempting to honour any CPU masking that may be in effect (on Linux). Threads can be limited using the `--array_limit` fsl_sub option or by setting the environment variable `FSLSUB_PARALLEL` to the maximum number of parallel processes.

##### Coprocessor Options

The next section, _coproc\_opts_ defines options for coprocessor hardware, e.g.

```yaml
coproc_opts:
  cuda:
    # The options here
```

*Any definition for CUDA capable hardware must be keyed on 'cuda' for FSL tools to automatically find the cuda queues and run the appropriate version, e.g.*

The options available are plugin dependent, but would typically include the following options as a basic set:

* resource: Grid resource that (GRES on Slurm), when requested, selects machines with the hardware present, often _gpu_.
* include\_more_capable: True/False - whether to automatically request all coprocessors of this type that are more capable than the requested class.
* uses\_modules: True/False - Is the coprocessor configured using a shell module?
* module\_parent: If you use shell modules, what is the name of the parent module? e.g. _cuda_ if you have a module folder _cuda_ with module files within for the various CUDA versions.
* no_binding: True/False - Where the grid software supports CPU core binding fsl\_sub will attempt to prevent tasks using more than the requested number of cores. This option allows you to override this setting when submitting coprocessor tasks as these machines often have signifcantly more CPU cores than GPU cores.
* class\_types: This contains the definition of the GPU classes...
  * class selector: This is the letter (or word) that is used to select this class of co-processor from the fsl\_sub commandline. In the case of CUDA GPUs it should be the letter designating the GPU family, e.g. K, P or V.
    * resource: This is the name of the complex/constraint that will be used to select this GPU family
    * doc: The description that appears in the fsl\_sub help text about this device
    * capability: An integer defining the feature set of the device, your most basic device should be given the value 1 and more capable devices higher values, e.g. GTX = 1, Kelper = 2, Pascal = 3, Volta = 4.
* default\_class: The _class selector_ for the class to assign jobs to where a class has not been specified in the fsl\_sub call.

Further information on how to configure these options and any additional configuration required for the backend is provided in the plugin documentation.

##### Queue Options

The final section defines the queues (referred to as partitions in Slurm) available on your cluster. See the plugin documentation for details on the settings required.

## Building

Prepare the source distribution

> python setup.py sdist

To build a wheel you need to install wheel into your Python build environment

> pip install wheel

fsl_sub is only compatible with python 3 so you will be building a Pure Python Wheel

> python setup.py bdist_wheel

## Usage

For detailed usage see:

```bash
fsl_sub --help
```

The options available will depend on how fsl_sub has been configured for your particular backend - see the plugin's documentation for details.

## Advanced Usage

### Multi-stage pipelines

Where you need to submit multiple stages in advance with job holds on the previous step but do not know in advance the command you wish to run you may create an array task file containing the text 'dummy'. Validation of the array task file will be skipped allowing the task to be submitted.
You should then arrange for a predecessor to populate the array task file with the relevant command(s) to run.

### Specifying Memory Requirements

If fsl_sub is being called from within a software package such that you have no ability to specify memory requirements then you can achieve this by setting the environment variable `FSLSUB_MEMORY_REQUIRED`, e.g.

```bash
FSLSUB_MEMORY_REQUIRED=32G myscript_that_submits
```

If units are not specified then they will default to those configured in the YAML file.
If the memory is also specified in the fsl_sub arguments then the argument provided value will be used.

### Specifying Accounting Project

On some clusters you may be required to submit jobs to different projects to ensure compute time is billed accordingly, or to gain access to restricted resources. You can specify a project with the `--project` option. If fsl_sub is being called from within a software package such that you have no ability to specify this option then you can select a project with the environment variable `FSLSUB_PROJECT`, e.g.

```bash
FSLSUB_PROJECT=myproj myscript_that_submits
```

### Array task sub-task ID

When running as an array task you may wish to know which sub-task you are, this might be useful to automatically determine which member of the array you are. This information is provided in environment variables by the queuing software. To determine which variable to read, fsl_sub will set the following environment variables to the name of the equivalent queue variable:

* `FSLSUB_JOBID_VAR`: The ID of the master job
* `FSLSUB_ARRAYTASKID_VAR`: The ID of the sub-task
* `FSLSUB_ARRAYSTARTID_VAR`: The ID of the first sub-task
* `FSLSUB_ARRAYENDID_VAR`: The ID of the last sub-task
* `FSLSUB_ARRAYSTEPSIZE_VAR`: The step between sub-task IDs (not available for all plugins)
* `FSLSUB_ARRAYCOUNT_VAR`: The number of tasks in the array (not available for all plugins)

Not all variables are set by all queue backends so ensure your software can cope with missing variables.

### Submitting tasks from submitted tasks

Most clusters will not allow a running job to submit a sub-task as it is fairly likely this will result in deadlocks. Consquently, subsquent calls to fsl\_sub will result in the use of the _shell_ plugin for job running. If this occurs from within a cluster job the job .o and .e files will have filenames of the form _\<job name>.[o|e]\<parent jobid>{.\<parent taskid>}-\<process id of fsl_sub>{.\<taskid>}_.

## Additional tools

Included with the `fsl_sub` command are several addtional tools detailed below.

### fsl_sub_config

This command outputs an example `fsl_sub` configuration file for the cluster backend requested. This file can be installed centrally (in `$FSLDIR/etc/fslconf`, called `fsl_sub.yml`), in your home folder (in `~/.fsl_sub.yml`) or in any folder and the environment variable `FSLSUB_CONF` set to point at the configuration file.

### fsl_sub_report

This command abstracts the cluster reporting tools, providing a common output no matter what the cluster backend.

#### fsl_sub_report Usage

```bash
fsl_sub_report [job_id] {--subjob_id [sub_id]} {--parsable}
```

Reports on job `job_id`, optionally on subtask `sub_id`. `--parsable` outputs machine readable information.

### fsl_sub_install_plugin

This command lists available plugins (as known to the central FSL software repository) and helps to install these into an FSL installation.

#### fsl_sub_install_plugin Usage

```bash
fsl_sub_install_plugin [--list|--install {plugin}]
```

`--list` lists available plugins and `--install` installs the requested plugin (displays the list and prompts for a choice if no plugin is specified).

## Python interface

The `fsl_sub` package is available for use directly within python scripts. Ensure that the fsl_sub folder is within your python search path and import as follows:

```python
import fsl_sub
```

### Querying Configuration

If your software needs to determine whether a CUDA card/queue is available or whether you are running in a queued environment you can use the has_* methods in the utils submodule. To include these functions:

```python
from fsl_sub.utils import (has_queues, has_coprocessor)
```

* has_queues: This function takes no arguments and returns True or False depending on whether there are usable queues (current execution method supports queueing and there are configured queues).
* has_coprocessor(coprocessor_name): Takes the name of a coprocessor configuration key and returns True or False depending on whether the system is configured for or supports this coprocessor. A correctly configured fsl_sub + cluster + CUDA devices should have a coprocessor definition of 'cuda' (users will be warned if this is not the case).

### report

Arguments: job_id, subjob_id=None

If using this, also import the `consts` sub-package.

```python
import fsl_sub.consts
```

This returns a dictionary describing the job (including completed tasks):

* id
* name
* script (if available)
* arguments (if available)
* submission_time
* tasks (dict keyed on sub-task ID):
  * status:
    * fsl_sub.consts.QUEUED
    * fsl_sub.consts.RUNNING
    * fsl_sub.consts.FINISHED
    * fsl_sub.consts.FAILEDNQUEUED
    * fsl_sub.consts.SUSPENDED
    * fsl_sub.consts.HELD
  * start_time
  * end_time
  * sub_time
  * utime
  * stime
  * exit_status
  * error_message
  * maxmemory (in Mbytes)
* parents (if available)
* children (if available)
* job_directory (if available)

### submit

Arguments:

* architecture=None
* array_task=False
* array_hold=None
* array_limit=None
* array_specifier=None
* as_tuple=True
* command
* coprocessor=None
* coprocessor_toolkit=None
* coprocessor_class=None
* coprocessor_class_strict=False
* coprocessor_multi="1"
* export_vars=[]
* jobhold=None
* jobram=None
* jobtime=None
* keep_jobscript=False
* logdir=None
* mail_on=None
* mailto=(defaults to username@hostname)
* name=None
* native_holds=False
* parallel_env=None
* priority=None
* project=None
* queue=None
* ramsplit=True
* requeueable=True
* resources=None
* threads=1
* usescript=False
* validate_command=True

Submit job(s) to a queue, returns the job id as an int (pass as_tuple=True
to return a single value tuple).

Single tasks require a command in the form of a list [command, arg1,
arg2, ...] or simple string "command arg1 arg2".

Array tasks (array_task=True) require a file name of the array task table
file unless array_specifier(=n[-m[:s]]) is specified in which case command
is as per a single task.

Required Arguments:

* command - string or list containing command to run
    or the file name of the array task file.
    If array_specifier is given then this must be
    a string/list containing the command to run.

Optional Arguments:

* job_name - Symbolic name for task (defaults to first component of command)
* array_hold - complex hold string
* array_limit - limit concurrently scheduled array
    tasks to specified number
* array_specifier - n[-m[:s]] n subtasks or starts at n, ends at m with
* array_task - is the command is an array task (defaults to False)
* as_tuple - if true then return job ID as a single value tuple
* coprocessor - name of coprocessor required
* coprocessor_toolkit - coprocessor toolkit version
* coprocessor_class - class of coprocessor required
* coprocessor_class_strict - whether to choose only this class
    or all more capable
* coprocessor_multi - how many coprocessors you need (or
    complex description) (string)
* export_vars - list of environment variables to copy from current environment
    or list of variable=setting to set new or override current variable
* jobhold - id(s) of jobs to hold for (string or list)
    a step of s.
* jobram - RAM required by job (total of all threads)
* jobtime - time (in minutes for task)
* keep_jobscript - whether to preserve (as jobid.sh) the script that was submitted
    to the cluster (ignored for 'shell' submission type)
* logdir - directory to put log files in
* mail_on - mail user on 'a'bort or reschedule, 'b'egin, 'e'nd,
    's'uspended, 'n'o mail
* mailto - email address to receive job info
* native_holds - whether to process the jobhold or array_hold input
* parallel_env - parallel environment name
* priority - job priority (0-1023)
* project - Cluster project to submit job to, defaults to None
* queue - Explicit queue to submit to - use jobram/jobtime in preference to
    this
* requeueable - job may be requeued on node failure
* resources - list of resource request strings
* ramsplit - break tasks into multiple slots to meet RAM constraints
* usescript - queue config is defined in script

### calc_slots

Arguments:

* job_ram
* slot_size
* job_threads

Calculates the number of queue slots necessary to achieve the RAM and thread requirements of a job. This is used internally within submit so wouldn't normally need to be run.

### getq_and_slots

Arguments:

* queues
* job_time=0
* job_ram=0,
* job_threads=1
* coprocessor=None
* ll_env=None

This returns a tuple consisting of the queue and the number of slots required for the specified job parameters.

### Killing jobs

You can request that a job is killed using the fsl_sub.delete_job function which takes the job ID (including task ID) and calls the appropriate cluster job deletion command.
This returns a tuple, text output from the delete command and the return code from the command. There is an equivalent shell command fsl\_sub --delete\_job \<jobID>

## Writing Plugins

Inside the plugins folder there is a template - template_plugin.py that can be modified to add support for different grid submission engines. This file should be renamed to fsl_sub_plugin_\<method>.py and placed somewhere on the Python search path. Inside the plugin change METHOD_NAME to \<method> and then modify the functions appropriately. The submit function carries out the job submission, and aims to either generate a command line with all the job arguments or to build a job submission script. The arguments should be added to the command_args list in the form of option flags and lists of options with arguments.
Also provide a fsl_sub_\<method>.yml file that provides the default configuration for the module.
