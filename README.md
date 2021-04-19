# fsl_sub

Job submission to cluster queues
_Copyright 2018-2020, University of Oxford (Duncan Mortimer)_

## Introduction

fsl_sub provides a consistent interface to various cluster backends, with a fall back to running tasks locally where no cluster is available.
If you wish to submit tasks to a cluster you will need to install and configure an appropriate grid backend plugin, two of which are provided alongside fsl_sub:

* fsl_sub_plugin_sge for Sun/Son of/Univa Grid Engine (Grid Engine)
* fsl_sub_plugin_slurm for Slurm

### Install backend

If you intend to use fsl_sub to run jobs on a cluster you will need to install one of the cluster backends. You can do this with he command:

~~~bash
fsl_sub_plugin --install
~~~

which will search for and install plugins.

For instructions on how to install fsl_sub outside of the FSL software suite, see the INSTALL.md file.

For instructions on how to configure fsl_sub once installed (essential if using a cluster plugin) see the CONFIGURATION.md file.

## Usage

For detailed usage see:

~~~bash
fsl_sub --help
~~~

The options available will depend on how fsl\_sub has been configured for your particular backend - this help text will reflect the availability of options with your chosen backend.

### Basic Submission

Submitting or running a job on the default queue is as simple as:

~~~bash
fsl_sub <command line>
~~~~

When running with a cluster it is recommended that you provide a job run time with `-T <time in minutes>` and a memory requirement with `-R <memory in GB>`, this allows fsl\_sub to automatically select an appropriate queue. Alternatively, you can specify a queue with `-q <queue name>`. For example, if you have two queues, _short_ and _long_ with maximum run-times of 60 minutes and 5 days respectively then:

~~~bash
fsl_sub -T 50 myjob
~~~

is the equivalent of `fsl_sub -q short myjob`, but enables you to potentially use this submission command (and any script based using this command) with any fsl\_sub enabled cluster, regardless of queue names.

Providing the memory required is also advisable as some cluster setups enforce memory limits but provide for multi-slot reservations to allocate multiples of the RAM limit to your task. fsl\_sub can be configured to automatically make these types of submission.

After validation of your job command and settings, fsl\_sub will either wait until job completion (with no cluster backend) or will return with the job ID of your submitted job. This number can then be used to monitor job progress.

### Job Monitoring - fsl_sub_report

In addition to the native job monitoring tools, fsl\_sub provides a cluster backend agnostic job monitoring tool, `fsl_sub_report`.

#### fsl_sub_report Usage

~~~bash
fsl_sub_report [job_id] {--subjob_id [sub_id]} {--parsable}
~~~

Reports on job `job_id`, optionally on subtask `sub_id` and returns information on both queued/running and completed jobs. `--parsable` outputs machine readable information.

## Advanced Usage

### Skipping Command Validation

By default fsl\_sub checks to see if the submitted command can actually be run. Where the software isn't available on the submission computer or you are prepending the command with some logic or setting an environment variable, this test will fail. You can disable validation with the `-n` (or `--novalidation`) option.

### Array Tasks

Array tasks are indepentent tasks that can be run in parallel as they do not need or generate date required by other members of the array. To create a simple array task create a text file where each line contains a command to run. Submit this as the argument to the `--array_task` option.

To control the number of array tasks run in parallel, use `--array_limit`, this is also useful for standalone installs as it will limit the number of threads used when running array tasks in parallel on your computer.

It is also possible to submit an array task where the submitted software works out what portion of the array it should be processing. In this mode with `--array_native`. The command will be launched multiple times (as specified in the `--array_native` argument _n[:m[:s]]_ (_n_umber of array members, _m_ start index, _s_ step in index number between array members)) with the following environment variables populated with the information necessary to workout what part of the array the program is to handle. As each cluster software suite sets different variables fsl\_sub will set the following variables to the _name_ of the environment variable your software can query to get the information:

| Variable | Points to the variable holding |
|---|---|
| `FSLSUB_JOBID_VAR` | The ID of the master job
| `FSLSUB_ARRAYTASKID_VAR` | The ID of the sub-task
| `FSLSUB_ARRAYSTARTID_VAR` | The ID of the first sub-task
| `FSLSUB_ARRAYENDID_VAR` | The ID of the last sub-task
| `FSLSUB_ARRAYSTEPSIZE_VAR` | The step between sub-task IDs (not available for all plugins)
| `FSLSUB_ARRAYCOUNT_VAR` | The number of tasks in the array (not available for all plugins)

Not all variables are set by all queue backends so ensure your software can cope with missing variables.

For example in BASH scripts you can get the ARRAYTASKID value with `${!FSLSUB_ARRAYTASKID_VAR}`.

### Setting Environment Variables In Job Environments

Some cluster setups don't support passing all environment variables in your current shell session to your jobs. fsl\_sub provides the `--export` option to allow you to choose which variables need to be passed on, or to set environment variables only within the job (not affecting your running shell session). To set a variable use the syntax `--export MYVAR=THEVALUE`. This can be repeated multiple times.

### Multi-stage pipelines

Where you need to queue up a complex pipeline, you can use returned job IDs with the `--job_hold` option to request that a submitted task wait for completion of a predecessor task. In addition, multi-stage array tasks can utilise interleaved job-holds with the option

### Array Task Validation

Where you need to submit multiple stages in advance with job holds on the previous step but do not know in advance the command you wish to run you may create an array task file containing the text 'dummy'. Validation of the array task file will be skipped allowing the task to be submitted. You should then arrange for a predecessor to populate the array task file with the relevant command(s) to run.

### Saving Submission Information

Under normal circumstances cluster backends generate a BASH script that describes your job's requirements to the cluster software and then calls your job (or array task file line). Using the `--keep_jobscript` option you can request that fsl\_sub leaves a copy of this file in the current folder with name _wrapper\_<jobid>.sh_. This file will contain information on the version of fsl\_sub (and plugin used) along with exact command-line used and as such is very useful for recording analyses carried out.

### Submitting Cluster Submission Scripts

If you have written your own cluster submission script or wish to re-run a task for which you preserved the _wrapper\_<jobid>.sh_ file then you can do so using the `--usescript` option, providing the script as the command to submit.

### Specifying Memory Requirements Without Using -R

If fsl\_sub is being called from within a software package such that you have no ability to specify memory requirements (for example FEAT) then you can achieve this by setting the environment variable `FSLSUB_MEMORY_REQUIRED`, e.g.

~~~bash
FSLSUB_MEMORY_REQUIRED=32G myscript_that_submits
~~~

If units are not specified then they will default to those configured in the YAML file.
If the memory is also specified in the fsl_sub arguments then the value provided in the argument will be used.

### Co-Processor Tasks

Where your sofware needs to use a co-processor, most commonly CUDA GPU cards, fsl\_sub offers the `--coprocessor` options. To run CUDA software you would typically add `--coprocessor=cuda` to your fsl\_sub commandline. Assuming the queue configration has been setup correctly there is no other configuration necessary as the correct queue/partition will be selected automatically. If your system has multiple versions of CUDA installed and selectable using _shell modules_ (and everything is configured correctly) you can select the cuda version using `--coprocessor_toolkit` option. Where multiple hardware versions are available then your system may have been configured to allow you to select specific card generations with `--coprocessor_class`, with `--coprocessor_class_strict` allowing you to force fsl\_sub to only select the class of card you request (as opposed to this class and all superior devices).

### Specifying Accounting Project

On some clusters you may be required to submit jobs to different projects to ensure compute time is billed accordingly, or to gain access to restricted resources. You can specify a project with the `--project` option. If fsl\_sub is being called from within a software package such that you have no ability to specify this option then you can select a project by setting the environment variable `FSLSUB_PROJECT`, e.g.

~~~bash
FSLSUB_PROJECT=myproj myscript_that_submits
~~~

### Submitting tasks from submitted tasks

Most clusters will not allow a running job to submit a sub-task as it is fairly likely this will result in deadlocks. Consquently, subsquent calls to fsl\_sub will result in the use of the _shell_ plugin for job running. If this occurs from within a cluster job the job .o and .e files will have filenames of the form _\<job name>.[o|e]\<parent jobid>{.\<parent taskid>}-\<process id of fsl_sub>{.\<taskid>}_. Where allowed by thread 'slot' requests array tasks in these sub-tasks will be parallelissed as if running locally.

### Native Resource Requests

Where your cluster system has a specific resource requirement that can't be automatically be fulfilled by fsl\_sub you can use the `-r` option to pass through a native resource request string.

### Deleting Jobs

`fsl_sub --delete_job <jobID>` will enable you to delete a cluster job, assuming you have permission to do so.

### Querying Capabilities

If you are writing non-Python software that needs to check on the availability of fsl\_sub features, for example whether queues are configured or CUDA hardware is available then you can use the following options:

| Option | Use |
|-----|----|
| --has\_coprocessor | Takes the name of a co-processor, exits with code 1 if this co-processor is not available. Assuming everything is correctly configured then `--has_coprocessor cuda` should be a viable test for CUDA hardware both when running standalone and on a cluster system |
| --has_queues | fsl\_sub will exit with return code 1 if there are no queues configured, e.g. this is a standalone computer
| --show_config | This outputs the currently applicable configuration as a YAML file, the content of this file will depend on the plugins installed and the configuration of your system so is not guaranteed to be identical on all platforms |

## Python interface

The fsl\_sub package is available for use directly within python scripts. Ensure that the fsl\_sub folder is within your Python search path and import the appropriate parent module (e.g. _fsl\_sub_ or _fsl\_sub.utils_)

### fsl_sub.utils.has_queues

Import: from fsl_sub.utils import has_queues
Arguments: None

This function takes no arguments and returns True or False depending on whether there are usable queues (current execution method supports queueing and there are configured queues).

### fsl_sub.utils.has_coprocessor

Import: from fsl_sub.utils import has_coprocessor
Arguments: Name of co-processor

 Takes the name of a coprocessor configuration key and returns True or False depending on whether the system is configured for or supports this coprocessor. A correctly configured fsl_sub + cluster + CUDA devices should have a coprocessor definition of 'cuda' (users will be warned if this is not the case).

### fsl_sub.report

Import: fsl_sub, fsl_sub.consts
Arguments: job_id, subjob_id=None

This returns a dictionary describing the job (including completed tasks):

~~~yaml
id: # job ID
name: # job 'name'
submission_time: # as a datetime object
tasks: # dict keyed on sub-task ID
  subtask_id:
    status: # One of:
      # fsl_sub.consts.QUEUED
      # fsl_sub.consts.RUNNING
      # fsl_sub.consts.FINISHED
      # fsl_sub.consts.FAILED
      # fsl_sub.consts.SUSPENDED
      # fsl_sub.consts.HELD
    start_time: # as a datetime object
    end_time: # as a datetime object
~~~

### fsl_sub.submit

Import: fsl_sub
Arguments:

| Argument (*Required) | Default (type) | Purpose |
|----------|----------------|---------|
| **command*** | (list of strings or string) | Command line, job-script or array task file to submit |
| architecture | None (string) | Select nodes of specific CPU architecture (where cluster consists of multiple types) |
| array_task | False (boolean) | Whether this is an array task |
| array_hold | None (string|integer|list) | Array hold request - format depends on the cluster backend but typically will be a Job ID (integer or string) or a list of job IDs |
| array_limit | None (integer) | Maximum array tasks to run concurrently
| array_specifier | None (string) | If not using an array task file, the definition of the array - `n[-m[:s]]`. In it's simplest form, `n` is the number of sub-tasks (sub-ID starts at 1), `n-m` starts at ID `n` and runs until sub-job ID `m`. Providing `:s` defines the step size between adjacent sub-job IDs |
| as_tuple | False (boolean) | Return job ID as a single element tuple |
| coprocessor | None (string) | The name of a co-processor your job requires - use has_coprocessor() to check for availability |
| coprocessor\_toolkit | None (string) | The name of the shell module variant to load to configure the environment for this co-processor task, e.g. if you have a shell module _cuda/10.2_ then this would be `10.2` (assuming that the co-processor configuration has `cuda` set as it's module parent) |
| coprocessor_class | None (string) | The name of the class (as defined in the configuration) of co-processor |
| coprocessor_class_strict | False (boolean) | Only submit to this class of GPU excluding more capable devices |
| coprocessor_multi | "1" (string) | Complex definition requesting multiple co-processors. At its most basic this is the number of co-processors per node you require but may take more complex values as required by your cluster setup |
| export\_vars | [] (list of string) | This is a list of environment variables to copy to your job's environment where your cluster is configured to not transfer your complete environment. This can be simple environment variable names or _NAME=VALUE_ strings that will set the environment variable to the specified value for this job alone.
| jobhold | None (integer, string or list of integers/strings) | Job ID(s) that must complete before this job can run |
| jobram | None (integer) | Amount of RAM required for your job in Gigabytes |
| jobtime | None (integer) | Time required for your job in minutes |
| keep_jobscript | False (boolean) | Whether to keep the generated job script as `wrapper_<jobid>.sh` |
| logdir | None (string) | Path to the directory where log files should be created |
| mail_on | None (string) | Mail user (if mail configured) on job 'a'bort/reschedule, 'b'egin, 'e'nd, 's'uspend or 'n'ever mail |
| mailto | _username@hostname_ (string) | Email address to send messages to |
| name | None (string) | Name of job, defaults to first item in the command line |
| parallel_env | None (string) | Name of parallel environment to request if the backend supports these, otherwise ignored |
| priority | None (signed integer) | Priority of job within configured range - typically user can only lower priority |
| project | None (string) | Project/Account name to use for job |
| queue | None (string) | Rather than using jobram\|jobtime\|coprocessor to automatically select a queue specify a specific queue |
| ramsplit | True (boolean) | Whether to enable the requesting multiple slots in a parallel environment sufficient to provide the RAM requested, if your cluster backend/setup has this configured |
| requeueable | True (boolean) | Can this job be safely restarted (rescheduled)? |
| resources | None (string) | Cluster resource request strings, e.g. softwarelicense=1 |
| threads | 1 (integer) | How many threads your software requires - attempts will be made to limit your task to this number of threads |
| usescript | False (boolean) | Have you provided a job script in the command argument? If so all other options are ignored |
| validate_command | True (boolean) | Whether to validate that the first item in the command line is an executable |

Submit job(s) to a queue, returns the job id as an integer.

Single tasks require a command in the form of a list [command, arg1,arg2, ...]
or simple string "command arg1 arg2" which will be shlex.split.

Array tasks (array_task=True) require a file name of the array task table
file unless array_specifier="n[-m[:s]]" is specified in which case command
is as per a single task.

### fsl_sub.delete_job

Import: fsl_sub
Arguments: job_id, (sub_job_id)

You can request that a job is killed using the fsl_sub.delete_job function which takes the job ID (including task ID) and calls the appropriate cluster job deletion command.
This returns a tuple, text output from the delete command and the return code from the command.

## Writing Plugins

Inside the plugins folder there is a template - `template_plugin.py` that can be modified to add support for different grid submission engines. This file should be renamed to `fsl_sub_plugin_<method>.py` and placed somewhere on the Python search path. Inside the plugin change METHOD_NAME to \<method> and then modify the functions appropriately. The submit function carries out the job submission, and aims to either generate a command line with all the job arguments or to build a job submission script. The arguments should be added to the command_args list in the form of option flags and lists of options with arguments.
Also provide a `fsl_sub_<method>.yml` file that provides the default configuration for the module.
To create an installable Conda/Pip package of this plugin look at the Grid Engine and SLURM plugins for example directory layouts and build scripts.

## Building

### Conda

To build a Conda package:

> cd condabuild
> conda build ..

### Pip

To build with PIP, prepare the source distribution:

> python setup.py sdist

To build a wheel you need to install wheel into your Python build environment

> pip install wheel

fsl_sub is only compatible with python 3 so you will be building a Pure Python Wheel

> python setup.py bdist_wheel
