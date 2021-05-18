# Installation

## Installation within FSL

FSL ships with fsl_sub pre-installed but lacking any grid backends. If you wish to use fsl_sub with a supported cluster backend you can use the command `fsl_sub_plugin` to query and install the appropriate FSL distributed backend.

### Install backend

The command:

~~~bash
fsl_sub_plugin --install
~~~

will search for and allow you to install a plugin.

## Standalone Installation

Where fsl_sub is to be used outside of the FSL distribution it is recommended that it is installed within a Conda or virtual environment.

### Requirements

fsl_sub requires Python >=3.6 (3.8 recommended) and ruamel.yaml >=0.16.7

### Installation with Conda

First, install Miniconda from <https://conda.io/miniconda.html>, install as per their instructions then create an environment and activate:

~~~bash
conda create -n fsl_sub python=3
source activate fsl_sub
~~~

and install fsl_sub with:

~~~bash
conda install -c https://fsl.fmrib.ox.ac.uk/fsldownloads/fslconda/channel fsl_sub
~~~

Plugins can be installed with:

Grid Engine...

~~~bash
conda install -c https://fsl.fmrib.ox.ac.uk/fsldownloads/fslconda/channel fsl_sub_plugin_sge
~~~

SLURM...

~~~bash
conda install -c https://fsl.fmrib.ox.ac.uk/fsldownloads/fslconda/channel fsl_sub_plugin_slurm
~~~

### Installation in a virtual environment

Using Python 3.6+, create a virtual environment with:

~~~bash
python -m venv /path/to/my/venv
~~~

(if you have multiple Python versions available, you can often choose with _python3.8_ or _python3.9_)

Now activate this virtual environment with:

~~~bash
source activate /path/to/my/venv/bin/activate
~~~

and fsl_sub can be installed with:

~~~bash
pip install git+https://git.fmrib.ox.ac.uk/fsl/fsl_sub.git
~~~

To install a plugin, ensure your environment is activated and then install the plugin with:

Grid Engine:

~~~bash
pip install git+https//git.fmrib.ox.ac.uk/fsl/fsl_sub_plugin_sge.git
~~~

SLURM:

~~~bash
pip install git+https//git.fmrib.ox.ac.uk/fsl/fsl_sub_plugin_slurm.git
~~~
