#!/bin/bash

# Copyright (C) 2007-2017 University of Oxford
# Authors: Dave Flitney, Stephen Smith, Matthew Webster and Duncan Mortimer

# SHCOPYRIGHT

###########################################################################
# Edit this file in order to setup FSL to use your local compute
# cluster.
###########################################################################

# Exit states
INVALID_SYNTAX="64"
NO_CUDA="65"
NO_CUDA_TYPE="66"
MISS_CONFIGURED="67"
ARG_MISSING_VALUE="68"
MISSING_FILE="69"
NO_CUDA_VERSION="70"
NO_EXECUTABLE="71"
BAD_QUEUE="72"
NOT_ACHIEVABLE="73"

# Functions
# Is variable set to yes/1/YES/y or no/0/NO/n
is_true () {
    REGEXY="^yes$|^1$|^YES$|^Yes$|^y$"
    REGEXN="^no$|^0$|^NO$|^No$|^n$"
    if [ -z "$1" ]; then
        return 1
    elif [[ "$1" =~ $REGEXY ]]; then
        return 0
    elif [[ "$1" =~ $REGEXN ]]; then
        return 1
    else
        echo "Miss-configured, $1 should be yes or no!"
        exit "$MISS_CONFIGURED"
    fi
}

is_false () {
    is_true "$1"
    return $((1-$?))
}

# Validate passed arguments - takes number of arguments and the argument string
with_arguments () {
    nargs=$1
    REGEX="-.*"
    shift 2
    for (( arg=0; arg<"$nargs"; arg++ )); do
        if [[ "$1" =~ $REGEX ]]; then
            usage "$ARG_MISSING_VALUE"
        fi
        shift
    done
}

# Join the second argument array with the first argument delimeter
join_list ()
{
    delimeter=$1
    array=("${@:2}")
    array_string=$(printf "$delimeter%s" "${array[@]}")
    echo "${array_string:1}"
}

index_in_list ()
{
    local index=0
    local match="$1"
    local values="${*:2}"
    for value in $values; do
        if [ "$value" = "$match" ]; then
            echo "$index"
            return 0
        fi
        ((index++))
    done
    return 1
}

array_contains () {
    local e match="$1"
    shift
    for e; do [[ "$e" == "$match" ]] && return 0; done
    return 1
}

###########################################################################
# The following section determines what to do when fsl_sub is called
# by an FSL program. If SGE_ROOT is set it will attempt to pass the
# commands onto the cluster, otherwise it will run the commands
# itself. There are two values for the METHOD variable, "SGE" and
# "NONE". Note that a user can unset SGE_ROOT if they don't want the
# cluster to be used.
###########################################################################
METHOD=SGE

VERSION="2.0"
#
# Give the queue submission command name here
#
QSUB="qsub"

#
# Provide any queue name suffix here
#
QSUFFIX=".qc"

#
# Option that specifies the queue
#
QCMD="-q"

#
# Option that specifies a parallel task file and a limit on concurrent tasks
#
LLTASK="-t"
LLLIMIT="-tc"
#
# Option that specifies a job priority
#
PRIORITY_CMD="-p"

#
# Option that specifies a complex
#
COMPLEX_CMD="-l"

#
# Options that specifies stdout/err dir
#
STDOUT_DIR="-o"
STDERR_DIR="-e"

#
# Option that specifies a job hold
#
JHOLD_CMD="-hold_jid"

#
# Option that specifies a parallel job hold
#
LLJHOLD_CMD="-hold_jid_ad"

#
# Option that specifies mail address
#
MAILTO_CMD="-M"

#
# Option that specifies when to mail
#
MAILOPT_CMD="-m"

#
# Option that specifies the job name argument
#
JOBNAME_CMD="-N"

#
# Option that specifies that the environment should be copied to the job
#
COPY_ENV_CMD="-V"

#
# Should we copy the environment
#
COPY_ENV="yes"

###########################################################################
# If you have a Parallel Environment configured for OpenMP tasks then
# the variable omp_pe should be set to the name you have defined for that
# PE. The script will work out which queues have that PE setup on them.
# Note, we support openmp tasks even when Grid Engine is not in use.
###########################################################################

omp_pe='shmem'


###########################################################################
# If you wish to disable processor affinities under Grid Engine then
# comment the following line.
# This instructs Grid Engine to bind the task to the number of job slots
# allocated to the job (or PE)
###########################################################################
AFFINITY_CMD="-binding"
affinity_type="linear"
# How to specify linear affinity
affinity_control="threads"

###########################################################################
# The following sets up the default queue name, which you may want to
# change. It also sets up the basic emailing control.
###########################################################################

queue_list=("short${QSUFFIX}" "long${QSUFFIX}")
queue_list_times_minutes=("1440" "10080")
queue_list_times_doc=("24h" "7d")
queue_list_memory_limits=("16" "16")
default_queue="long${QSUFFIX}"
mailto=$(whoami)@$(hostname -f | cut -d . -f 2-)
MailOpts="a"

#
# Set this to the maximum amount of RAM available to a slot (in GB)
slot_ram_limit="16"
map_ram="Yes"
ram_resources=("m_mem_free" "h_vmem")
ram_units="G"
ram_units_doc="GB"
# Set this to the maximum number of shared memory slots available on the cluster
max_slots="16"

###########################################################################
# The following sets up the CUDA queue, which you will need to change to
# match your environment.
###########################################################################

# Change this to 0 if you do not have CUDA hardware.
cuda_available="yes"
# Name the CUDA queue here.
cuda_qsuffix=".q"
cuda_queue="gpu${cuda_qsuffix}"
# Job time limit
cuda_time_limit="300h"
# Set this to maximum amount of RAM available to a slot
cuda_slot_ram_limit="64"
cuda_map_ram="No"
# Define the Univa GPU complex here
cuda_gpu_resource="gpu"
# Do we care about GPU hardware classes?
cuda_classes_available="yes"
# GPU Hardware class configuration
cuda_class_resource="gputype"
# Define the CUDA hardware classes available here (e.g. what -y options
# are acceptable on your system) and the default type. The classes can only
# be single characters
cuda_types=("G" "K" "P" "V")
# Default CUDA type
cuda_type_default="K"
# And the resource strings that correspond to these
cuda_type_names=("TitanX" "k80"  "v100" "p100")
# Provide some helpful info to your user about the available classes
cuda_type_docs="Available CUDA hardware classes are
  G - TitanX. No-ECC, single-precision workloads
  K - Kepler. ECC, double- or single-precision workloads
  P - Pascal. ECC, double-, single- and half-precision workloads
  V - Volta. ECC, double-, single-, half- and quarter-precision workloads"

# If your system is configured to use Shell Modules for environment setup
# and you have appropriate versioned CUDA modules define this variable as '1'
cuda_uses_modules="yes"
# The next setting defines the parent to your versioned CUDA modules, e.g.
# 'cuda' would mean you would load CUDA 9 with 'module load cuda/9'
cuda_modules="cuda"

# This works out which modules you have available
if [ $cuda_uses_modules ]; then
    # Define the Shell Module prefix for the CUDA Libraries - module name
    # should match the toolkit version
    available_mods=$(module -t avail $cuda_modules 2>&1 | grep -v ':' | tail -n +2 | tr -c '[:print:]' ' ')
    IFS=' ' read -r -a cuda_modules_available <<< "${available_mods//$cuda_modules\//}"
    if [[ "${#cuda_modules_available[@]}" -eq 0 ]]; then
        cuda_uses_modules="no"
        echo "Warning - no CUDA modules found, disabling selection of CUDA toolkit by shell modules." >&2
    else
        cuda_modules_default="${cuda_modules_available[${#cuda_modules_available[@]}-1]}"
    fi
fi
# If you don't use Shell Modules make sure the libraries are available

#unset module
if [ -z "$SGE_ROOT" ] ; then
    METHOD=NONE
else
    QCONF=$(which qconf)
    if [ -z "$QCONF" ]; then
        METHOD=NONE
        echo "Warning: SGE_ROOT environment variable is set but Grid Engine software not found, will run locally" >&2
    fi
fi

# stop submitted scripts from submitting jobs themselves
if is_true "$FSLSUBALREADYRUN" ] ; then
    METHOD=NONE
    echo "Warning: job on queue attempted to submit parallel jobs - running jobs serially instead" >&2
fi

if [ "X$METHOD" = "XNONE" ]; then
    QCONF=echo
fi
FSLSUBALREADYRUN=true
export FSLSUBALREADYRUN

###########################################################################
# The following auto-decides what cluster queue to use. The calling
# FSL program will probably use the -T option when calling fsl_sub,
# which tells fsl_sub how long (in minutes) the process is expected to
# take (in the case of the -t option, how long each line in the
# supplied file is expected to take). You need to setup the following
# function to map ranges of timings into your cluster queues - it doesn't
# matter how many you setup, that's up to you.
###########################################################################

map_qname ()
{
    duration=$1

    local index=0
    for qtime in "${queue_list_times_minutes[@]}"; do
        if [[ "$qtime" == '-' ]]; then
            qduration="${queue_list[$index]}"
            break
        elif [[ "$duration" -le "$qtime" ]]; then
            qduration="${queue_list[$index]}"
            break
        fi
    done
    if [[ -z "$qduration" ]]; then
        return 1
    fi

    queue="${qduration}${QSUFFIX}"
    #echo "Estimated time was $1 mins: queue name is $queue"
}

###########################################################################
# The following auto-decides what CUDA queue to use. The calling
# FSL program will probably use the -T option when calling fsl_sub,
# which tells fsl_sub how long (in minutes) the process is expected to
# take (in the case of the -t option, how long each line in the
# supplied file is expected to take). You need to setup the following
# function to map ranges of timings into your cluster queues - it doesn't
# matter how many you setup, that's up to you.
###########################################################################

map_cuda_qname ()
{
    # No resource specific queues...
    queue="${cuda_queue}"
    if is_true "$verbose"; then
        echo "Estimated time was $1 mins: queue name is $queue" >&2
    fi
}

###########################################################################
# The following makes any modifications to queue parameters based on
# promised RAM requirements as specified by the -R option when calling 
# fsl_sub, which tells fsl_sub how much RAM the job is expected to require
# (in GB) (in the case of the -t option, the RAM promise applies to  each 
# line in the supplied file). You need to setup the following
# list to map ranges of timings into your cluster queues - it doesn't
# matter how many you setup, that's up to you.
###########################################################################

map_ram ()
{
    ram=$1
    if is_true "$cuda_job"; then
        max_ram="$cuda_slot_ram_limit"
    else
        max_ram="$slot_ram_limit"
    fi
    if [[ "$max_ram" == "-" ]]; then
        return
    fi
    slots=$(((ram+max_ram-1)/ max_ram))
    
    if [ "$slots" -gt 1 ]; then
        if [ -z "$peName" ]; then
            peName="$omp_pe"
            peThreads="$slots"
        else
            if [ "$peThreads" -lt "$slots" ]; then
                peThreads="$slots"
            fi
        fi
    fi

    if is_true "$verbose"; then
        echo "Estimated RAM was ${ram} GB, this needs ${slots}" >&2
    fi
}

###########################################################################
# The following makes any modifications to queue parameters based on
# requested GPU class (assuming gpu classes are configured) as defined
# by the --cuda_type option to fsl_sub.
###########################################################################

map_gpu_class ()
{
    class="$1"
    REGEX="$class "
    if [[ ! "${cuda_types[@]}" =~ $REGEX ]]; then
        echo "CUDA hardware class $class not recognised" >&2
        echo "$cuda_type_docs" >&2
        return $NO_CUDA_TYPE
    fi
    lowest_hardware=$(index_in_list "$class" "${cuda_types[@]}")
    if [ -z "$lowest_hardware" ]; then
        echo "Unknown CUDA hardware class $class" >&2
        return $MISS_CONFIGURED
    fi
    cuda_complex=$(join_list "|" "${cuda_type_names[@]:$lowest_hardware}")
    echo "$cuda_class_resource=$cuda_complex"
}

###########################################################################
# Don't change the following (but keep scrolling down!)
###########################################################################

this_command=$(basename "$0")

usage ()
{
  if [ -n "$1" ]; then
    rval="$1"
  else
    rval="$INVALID_SYNTAX"
  fi
  qdesc=()
  length="${#queue_list[@]}"
  for (( i=0; i<length; i++ )); do
    qname="${queue_list[$i]}"
    queue_description=''
    if [[ "${queue_list_times_minutes[$i]}" == "-" ]]; then
        queue_description=("${qname}: This queue has no time limit")
    else
        queue_description=("${qname}: This queue is for jobs which last under ${queue_list_times_doc[$i]}")
    fi
    if [[ "${queue_list_memory_limits[$i]}" != "-" ]]; then
        queue_description+=" with a memory limit of ${queue_list_memory_limits[$i]}GB"
    fi
    qdesc+=("$queue_description")
  done
  if [[ "$cuda_time_limit" == "-" ]]; then
    qdesc+=("$cuda_queue: This queue is for GPU (CUDA) tasks with no time limit")
  else
    qdesc+=("$cuda_queue: This queue is for GPU (CUDA) tasks which last under $cuda_time_limit")
  fi
  if is_true "$map_ram"; then
    qdesc+=("")
    qdesc+=("For tasks that require more memory than the limit of a queue a shared memory parallel environment will be requested with sufficient slots to run the task. You MUST provide the expected maximum RAM with the -R|--jobram option for this to operate.")
  else
    qdesc+=("")
    qdesc+=("If your task requires more memory than these queue limits then you can request a shared memory parallel environment with the -S|--autoslotsbyram to allow larger jobs to run.")
  fi
  width=$(tput cols)
  fold -s -w "$width" >&2 <<USAGE_EOF

$this_command V$VERSION - wrapper for job control system such as SGE

Usage: $this_command [options] <command>

$this_command gzip *.img *.hdr
$this_command -q short.q gzip *.img *.hdr

  -a|--arch <arch-name>        Architecture [e.g., darwin or lx24-amd64].
  -c|--cuda                    Request a CUDA capable device.
     --cuda_type               Request a specific CUDA device type.

                               $cuda_type_docs
     
     --cuda_version <version>  Request a specific CUDA version, e.g.
                               ${cuda_modules_available[@]}
     --multi_gpu               Request multiple GPUs, e.g. 2 for two GPUs or complex.
                               GPU request strings.
  -F|--usescript               Use flags embedded in scripts to set SGE queuing options.
  -j|--jobhold <jid>           Place a hold on this task until job jid has completed.
  --parallelhold <paralleljid> Place a parallel hold on the specified array task. Each
                               sub-task is held until the equivalent sub-task in the
                               parent array task completes.
  -l|--logdir <logdirname>     Where to output logfiles.
  -m|--mailoptions <mailoptions> Change the SGE mail options, see qsub for details.
  -M|--mailto <email-address>  Who to email, default = $(whoami)@$(hostname).
  -n|--novalidation            Don't check for presence of script/binary in your search
                               path (use where the software is only available on the 
                               compute node).
  -N|--name <jobname>          Specify jobname as it will appear on queue.
  -p|--priority <job-priority> Lower priority [0:-1024] default = 0.               
  -q|--queue <queuename>       Possible values for <queuename> are ${queue_list[*]}.
                               See below for details.
                               Default is "$default_queue". Instead of choosing a queue
                               specify the time required.
  -r|--resource                Pass a Grid Engine resource request string through to the
                               job scheduler.
  -R|--jobram <RAM>            Max total RAM to use for job (integer in $ram_units_doc).
                               *This is very important to set if your job requires more
                               than ${slot_ram_limit}${ram_units_doc} (CPU) or ${cuda_slot_ram_limit}${ram_units_doc} (GPU)
  -s|--parallelenv <pename>,<threads> Submit a multi-threaded task - requires a PE 
                               (<pename>) to be configured for the requested queues.
                               <threads> specifies the number of threads to run
                               e.g. '$omp_pe,2'.
  -S|--autoslotsbyram          Use the memory requirement specified by -R|--jobram to request
                               a parallel environment with sufficient slots to allow your job
                               to run.
  -t|--paralleltask <filename> Specify a task file of commands to execute in parallel.
  -T|--jobtime <minutes>       Estimated job length in minutes, used to auto-set queue name
  -v|--verbose                 Verbose mode.
  -V|--version                 Print version number and exit
  -x|--parallellimit <number>  Specify the maximum number of parallel job sub-tasks to run
                               concurrently.
  -z|--fileisimage <output>    If <output> image or file already exists, do nothing and exit
                        
  

Queues
======

There are several batch queues configured on the cluster:

$(printf "%s\n" "${qdesc[@]}")

USAGE_EOF
  exit "$rval"
}

nargs=$#
if [ "$nargs" -eq 0 ] ; then
  usage
fi

if [ -n "$FSLSUBVERBOSE" ] ; then
    verbose="$FSLSUBVERBOSE";
    echo "METHOD=$METHOD : args=" "$@" >&2
fi

autoslotsbyram="no"
scriptmode=0
cuda_job="no"
cuda_version="$cuda_modules_default"
cuda_type="$cuda_type_default"
cuda_multi_gpu=1
complexes=()
peThreads=1
queue=''
debug="no"
job_time=''
job_ram=''
slots=1
validation="yes"
if is_true "$verbose"; then
    echo "Command line is '$*'"
fi

while :
do
    case "$1" in
        -a|--arch)
            with_arguments 1 "$@"
            acceptable_arch="no"
            available_archs=$(qhost | tail -n +4 | awk '{print $2}' | sort | uniq)
            for a in "${available_archs[@]}"; do
                if [ "$2" = "$a" ] ; then
                    acceptable_arch="yes"
                fi
            done
            if is_true "$acceptable_arch"; then
                complexes+=("arch=$2")
            else
                echo "Sorry arch of $2 is not supported on this GE configuration!" >&2
                echo "Should be one of: ${available_archs[*]}" >&2
                exit "$INVALID_SYNTAX"
            fi
            shift 2
        ;;
        -c|--cuda)
            if is_true "$cuda_available"; then
                cuda_job="yes"
            else
                echo "CUDA devices not available" >&2
                exit "$NO_CUDA"
            fi
            shift
        ;;
        --cuda-type)
            with_arguments 1 "$@"
            cuda_type="$2"
            shift 2
        ;;
        --cuda-version)
            with_arguments 1 "$@"
            cuda_version="$2"
            if is_false "$cuda_uses_modules"; then
                echo "CUDA Shell Modules not configured, ignoring requested CUDA version" >&2
            fi
            shift 2
        ;;
        --debug)
            debug="yes"
            shift
        ;;
        -F|--use-script)
            scriptmode=1
            shift
        ;;
        -j|--job-hold)
            with_arguments 1 "$@"
            sge_hold="$2"
            shift 2
        ;;
        --parallel-hold)
            with_arguments 1 "$@"
            parallel_hold="$2"
            shift 2
        ;;
        -l|--log-dir)
            with_arguments 1 "$@"
            log_dir="$2"
            LogDir="${2}/";
            if [ ! -e "${LogDir}" ]; then 
                mkdir -p "${LogDir}"
            else
                REGEX="^/dev/null$"
                if [[ ! "${LogDir}" =~ $REGEX ]] && [ -f "${LogDir}" ]; then
                    echo "Log destination is a file (should be a folder)" >&2
                    usage "$INVALID_SYNTAX"
                fi
            fi
            shift 2
        ;;
        -m|--mail-options)
            MailOpts="$2";
            shift 2
        ;;
        -M|--mail-to)
            with_arguments 1 "$@"
            mailto="$2"
            shift 2
        ;;
        --multi-gpu)
            with_arguments 1 "$@"
            cuda_multi_gpu="$2"
            shift 2
        ;;
        -n|--no-validation)
            validation="no"
            shift
        ;;
        -N|--name)
            with_arguments 1 "$@"
            JobName="$2";
            shift 2
        ;;
        -p|--priority)
            with_arguments 1 "$@"
            sge_priority="$2"
            shift 2
        ;;
        -q|--queue)
            with_arguments 1 "$@"
            queue="$2"
            shift 2
        ;;
        -r|--resource)
            with_arguments 1 "$@"
            REGEX=".+=.+"
            if [[ "$2" =~ $REGEX ]]; then
                complexes+=("$2")
            else
                echo "Resource requests should take form, resource=value" >&2
                exit "$INVALID_SYNTAX"
            fi
            shift 2
        ;;
        -R|--job-ram)
            with_arguments 1 "$@"
            job_ram="$2"
            shift 2
        ;;
        -s|--parallel-env)
            with_arguments 1 "$@"
            pe_string="$2";
            peName=$(echo "$pe_string" | cut -d',' -f 1)
            peThreads=$(echo "$pe_string" | cut -d',' -f 2)
            shift 2
        ;;
        -S|--autoslotsbyram)
            autoslotsbyram="yes"
            shift
        ;;
        -t|--parallel-task)
            with_arguments 1 "$@"    
            taskfile="$2"
            if [ -f "$taskfile" ] ; then
                tasks=$(wc -l "$taskfile" | awk '{print $1}')
                if [ "$tasks" -ne 0 ]; then
                    sge_tasks="1-$tasks"
                else
                    echo "Task file ${taskfile} is empty" >&2
                    echo "Should be a text file listing all the commands to run!" >&2
                    usage "$INVALID_SYNTAX"
                fi
            else
                echo "Task file (${taskfile}) does not exist" >&2
                exit "$MISSING_FILE"
            fi
            shift 2
        ;;
        -T|--job-time)
            with_arguments 1 "$@"
            job_time "$2"
            shift 2
        ;;
        -v|--verbose)
            verbose=1
            shift
        ;;
        -V|--version)
            echo "$VERSION"
            exit 0
            shift
        ;;
        -x|--parallel-limit)
            with_arguments 1 "$@"
            max_parallel_jobs="$2"
            REGEX="[[:digit:]]+"
            if [[ ! "$max_parallel_jobs" =~ $REGEX ]]; then
                echo "Maximum concurrent parallel jobs should be an integer." >&2
                usage "$INVALID_SYNTAX"
            fi
            shift 2
        ;;
        -z|--file-is-image)
            with_arguments 1 "$@"
            if [ -e "$2" -o "$("${FSLDIR}/bin/imtest" "$2")" = 1 ] ; then
                exit 0
            fi
            shift 2
        ;;
        --)
            shift
            break
        ;;
        -*)
            echo "Unrecognised option $1" >&2
            usage "$INVALID_SYNTAX"
        ;;
        *)
            break
        ;;
    esac
done

the_command="$1"
###########################################################################
# Don't change the following (but keep scrolling down!)
###########################################################################

# If job resources are specified, choose the queue automatically
if [ -z "$queue" ]; then
    if [ -n "$job_time" ]; then
        if is_true "$cuda_job"; then
            map_cuda_qname "$job_time"
        else
            map_qname "$job_time"
        fi
    else
        if is_true "$cuda_job"; then
            queue="$cuda_queue"
        else
            queue="$default_queue"
        fi
    fi
fi

# Verify we can accomodate the requested PE Slots
if [[ -n "$peThreads" ]] && [[ "$peThreads" -gt "$max_slots" ]]; then
    echo "Unable to allocate $peThreads threads - maximum available on one system is $max_slots."
    exit "$NOT_ACHIEVABLE"
fi

# Queue modifications based on RAM
if [[ -n "$job_ram" ]]; then
    if ( is_true ${map_ram} || is_true ${cuda_map_ram} ) || is_true ${autoslotsbyram}; then
        map_ram "$job_ram"
    fi
    if [[ "$peThreads" -gt "$slots" ]]; then
        slots="$peThreads"
    elif [[ "$slots" -gt "$max_slots" ]]; then
        echo "Unable to allocate $slots threads on this queue as required by memory request ${job_ram}${ram_units_doc} - maximum available on one system is $max_slots."
        exit "$NOT_ACHIEVABLE"
    fi
    remainder=$((job_ram % slots))
    add_ram=$(((remainder+slots-1)/slots))
    job_ram=$((job_ram / slots))
    job_ram=$((job_ram+=add_ram))
    for rresource in "${ram_resources[@]}"; do
        complexes+=("${rresource}=${job_ram}${ram_units}")
    done
elif is_true ${map_ram}; then
    echo "Warning: Requesting parallel environments for large jobs enabled but no job RAM requirement specified. Consider adding this to ensure your job completes" >&2
fi

$QCONF -sq "$queue" >/dev/null 2>&1
if [ $? -eq 1 ]; then
    echo "Invalid queue specified!" >&2
    exit "$BAD_QUEUE"
fi

if is_true "$cuda_job"; then

    #
    # Configure CUDA hardware version
    #
    if [ -n "$cuda_type" ] && [ "$cuda_classes_available" ]; then
        gpu_complex="$(map_gpu_class "$cuda_type")"
        if [[ $? -ne 0 ]]; then
            exit $?
        fi
        complexes+=("$gpu_complex")
    fi
    #
    # Configure CUDA Toolkit version
    #
    if is_true "$cuda_uses_modules"; then
        if ! array_contains "${cuda_version}" "${cuda_modules_available[@]}"; then
            echo "CUDA SDK $cuda_version not found!" >&2
            width=$(tput cols)
            echo "Available modules: $cuda_modules_available" | fold -w "$width" >&2
            usage "$NO_CUDA_VERSION"
        fi
        # Check for existing CUDA modules
        cuda_modules_loaded=$(module -t list 2>&1 | tail -n +2 | grep "${cuda_modules}")
        if [ -n "${cuda_modules_loaded}" ]; then
            for loaded_module in ${cuda_modules_loaded}; do
                module unload "${loaded_module}"
            done
        fi 
        cuda_module="$cuda_modules/$cuda_version"
        module add "$cuda_module"
    fi

    #
    # Configure multi-GPU CUDA support
    #
    complexes+=("$cuda_gpu_resource=$cuda_multi_gpu")
fi

if [ -z "$taskfile" ] && [ -z "$the_command" ]; then
	echo "Either supply a command to run or a parallel task file" >&2
	usage "$NO_EXECUTABLE"
fi
if [ -z "$taskfile" ]; then
    if is_true "$validation"; then
        if ! command -v "$the_command" >/dev/null 2>&1; then
            echo "The command you have requested cannot be found or is not executable" >&2
            exit "$NO_EXECUTABLE"
        fi
    fi
fi

if [ -n "$taskfile" ]; then
    if is_true "$validation"; then
        # Validate commands in task file
        lno=1
        while read line; do
            tf_cmd=$(echo "$line" | cut -d ' ' -f1)
            if ! command -v "${tf_cmd}" >/dev/null 2>&1; then
                echo "The command $tf_cmd in the task file $taskfile, line $lno cannot be found or is not executable" >&2
                exit "$NO_EXECUTABLE"
            fi
            lno=$((lno + 1 ))
        done < "$taskfile"
    fi
else
    if [ -n "$max_parallel_tasks" ]; then
        echo "-x option ignored as not a parallel task job" >&2
        max_parallel_tasks=""
    fi
    if [ -n "$parallel_hold" ]; then
        echo "Cannot apply a parallel job hold on a non-array task." >&2
        usage "$INVALID_SYNTAX"
    fi
fi
if [ -z "$JobName" ] ; then 
    if [ -n "$taskfile" ] ; then
        JobName=$(basename "$taskfile")
    else
        JobName=$(basename "$the_command")
    fi
fi

if [ -n "$tasks" ] && [ -n "$the_command" ] ; then
    echo "You appear to have specified both a task file and a command to run" >&2
    usage "$INVALID_SYNTAX"
fi

if [ -n "$peName" ]; then
    # If the PE name is 'openmp' then limit the number of threads to those specified
    if [ "$peName" = "$omp_pe" ]; then
        OMP_NUM_THREADS="$peThreads"
        export OMP_NUM_THREADS
    fi
fi

case $METHOD in

###########################################################################
# The following is the main call to the cluster, using the "qsub" SGE
# program. If $tasks has not been set then qsub is running a single
# command, otherwise qsub is processing a text file of parallel
# commands.
###########################################################################

    SGE)
       ###########################################################################
       # Test Parallel environment options
       ###########################################################################
    if [ -n "$peName" ]; then
            # Is this a configured PE?

        $QCONF -sp "$peName" >/dev/null 2>&1

        if [ $? -eq 1 ]; then
            echo "$@" >&2
            echo "$peName is not a valid PE" >&2
            exit "$INVALID_SYNTAX"
        fi

        # Get a list of queues configured for this PE and confirm that the queue
        # we have submitted to has that PE set up.
        error=$(qstat -g c -pe "$peName" 2>&1)

        if [ $? -eq 1 ] || [[ "$error" = "*error: no such parallel environment*" ]]; then
            echo "No parallel environments configured!" >&2
            exit "$MISS_CONFIGURED"
        fi

        qstat -g c -pe "$peName" | sed '1,2d' | awk '{ print $1 }' | grep "^$queue" >/dev/null 2>&1

        if [ $? -eq 1 ]; then
            echo "$@"
            echo "PE $peName is not configured on $queue" >&2
            exit "$MISS_CONFIGURED"
        fi

        # The -w e option will result in the job failing if there are insufficient slots
        # on any of the cluster nodes
        pe_options=("-pe" "$peName" "$peThreads" "-w" "e")

    fi

    sge_args=()
    if is_true "$COPY_ENV"; then
        sge_args+=("$COPY_ENV_CMD")
    fi

    if [ -n "$affinity_type" ]; then
        case "$affinity_control" in
            threads)
                affinity_spec="$affinity_type:$peThreads"
            ;;
            slots)
                affinity_spec="$affinity_type:slots"
            ;;
        esac
        sge_args+=("$AFFINITY_CMD" "$affinity_spec")
    fi
    if [ -n "$sge_priority" ]; then
        sge_args+=("$PRIORITY_CMD" "$sge_priority")
    fi
    if [ -n "${complexes[*]}" ]; then
        sge_args+=("$COMPLEX_CMD" "$(join_list "," "${complexes[@]}")")
    fi
    if [ -n "$log_dir" ]; then
        sge_args+=("$STDOUT_DIR" "$log_dir" "$STDERR_DIR" "$log_dir")
    fi
    if [ -n "$sge_hold" ]; then
        sge_args+=("$JHOLD_CMD" "$sge_hold")
    fi
    if [ -n "$parallel_hold" ]; then
        sge_args+=("$LLJHOLD_CMD" "$parallel_hold")
    fi
    if [ -z "$tasks" ] ; then
        if is_false "$scriptmode"; then
            sge_args+=("$MAILTO_CMD" "$mailto")
            if [ -n "$MailOpts" ]; then
                sge_args+=("$MAILOPT_CMD" "$MailOpts")
            fi
            sge_args+=("$JOBNAME_CMD" "$JobName")
            sge_args+=("${pe_options[@]}")
            sge_args+=("-cwd" "-shell" "n" "-b" "y" "-r" "y" "$QCMD" "$queue")
        else
            sge_args+=("${pe_options[@]}")
        fi
        if is_true "$verbose" || is_true "$debug"; then 
            echo "sge_args: " "${sge_args[@]}" >&2
            echo "executing: $QSUB " "${sge_args[@]}" "$@" >&2
        fi
        if is_false "$debug"; then
            exec "$QSUB" "${sge_args[@]}" "$@" | awk '{print $3}'
        fi
    else
        sge_args+=("-cwd" "$QCMD" "$queue")
        if [ -n "$max_parallel_tasks" ]; then
            sge_args+=("$LLLIMIT" "$max_parallel_jobs")
        fi
        sge_args+=("$LLTASK" "$sge_tasks")
        sge_args+=("$MAILTO_CMD" "$mailto")
        if [ -n "$MailOpts" ]; then
            sge_args+=("$MAILOPT_CMD" "$MailOpts")
        fi
        sge_args+=("$JOBNAME_CMD" "$JobName")
        sge_args+=("${pe_options[@]}")
        if is_true "$verbose" || is_true "$debug"; then 
            echo "sge_args: " "${sge_args[@]}" >&2
            echo "control file: $taskfile" >&2
        fi
        if is_false "$debug"; then
            exec "$QSUB" "${sge_args[@]}" <<EOF | awk '{print $3}' | awk -F. '{print $1}'
#!/bin/sh

#$ -S /bin/sh

the_command=\$\(sed -n -e "\${SGE_TASK_ID}p" $taskfile\$\)

exec /bin/sh -c "\$the_command"
EOF
        fi
    fi
    ;;

###########################################################################
# Don't change the following - this runs the commands directly if a
# cluster is not being used.
###########################################################################

    NONE)
    if [ -z "$tasks" ] ; then
        if is_true "$verbose" || is_true "$debug"; then 
            echo "executing: " "$@" >&2
        fi
        if is_false "$debug"; then
            /bin/sh <<EOF1 > ${LogDir}${JobName}.o$$ 2> ${LogDir}${JobName}.e$$
$@
EOF1
            ERR=$?

            if [ "$ERR" -ne 0 ] ; then
                cat "${LogDir}${JobName}.e$$" >&2
                exit $ERR
            fi
        fi
    else
        if is_true "$verbose" || is_true "$debug"; then 
            echo "Running commands in: $taskfile" >&2
        fi
        n=1
        while [ "$n" -le "$tasks" ] ; do
            line=$(sed -n -e ''${n}'p' "$taskfile")
            if is_true "$verbose" || is_true "$debug"; then 
                echo executing: "$line" >&2
            fi
            if is_false "$debug"; then
                /bin/sh <<EOF2 > ${LogDir}${JobName}.o$$.$n 2> ${LogDir}${JobName}.e$$.$n
$line
EOF2
            fi
            n=$((n + 1))
	    done
	fi	
	echo $$
	;;

esac

###########################################################################
# Done.
###########################################################################