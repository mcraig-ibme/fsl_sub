# Installation

## Installation within FSL

FSL ships with fsl_sub pre-installed but lacking any grid backends. If you wish to use fsl\_sub with a supported cluster backend you can use the command `fsl_sub_plugin` to query and install the appropriate FSL distributed backend.

### Install backend

The command:

~~~bash
fsl_sub_plugin --install
~~~

will search for and allow you to install a plugin.

## Standalone Installation

Where fsl_sub is to be used outside of the FSL distribution it is recommended that it is installed within a Conda or virtual environment.

### Requirements

fsl_sub requires Python >=3.6 and PyYAML >=3.12

### Installation with Conda

First, install Miniconda from <https://conda.io/miniconda.html>, install as per their instructions then create an environment and activate:

~~~bash
conda create -n fsl_sub python=3.8
source activate fsl_sub
~~~

and install fsl_sub with:

~~~bash
conda install -c https://fsl.fmrib.ox.ac.uk/fsldownloads/fslconda/channel fsl_sub
~~~

Plugins can be installed with the `fsl_sub_plugin` command as described above.

### Installation in a virtual environment

Using Python 3.5+, create a virtual environment with:

~~~bash
python -m venv /path/to/my/venv
~~~

(if you have multiple Python versions available, you can often choose 3.5 or 3.6 with _python3.5_ or _python3.6_)

Now activate this virtual environment with:

~~~bash
source activate /path/to/my/venv/bin/activate
~~~

and fsl_sub can be installed with:

~~~bash
pip install git+ssh://git@git.fmrib.ox.ac.uk/fsl/fsl_sub.git
~~~

To install a plugin, ensure your environment is activated and then install the plugin with:

Grid Engine:

~~~bash
pip install git+ssh://git@git.fmrib.ox.ac.uk/fsl/fsl_sub_plugin_sge.git
~~~

SLURM:

~~~bash
pip install git+ssh://git@git.fmrib.ox.ac.uk/fsl/fsl_sub_plugin_slurm.git
~~~
