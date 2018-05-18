# fsl_sub

_Copyright 2018, University of Oxford (Duncan Mortimer)_

*** Job submission to cluster queues ***

## Introduction

fsl_sub provides a consistent interface to various cluster backends, with a fall back to running tasks locally where no cluster is available.
If you wish to submit tasks to a cluster you will need to install and configure an appropriate grid backend plugin.

## Requirements

fsl_sub requires Python >=3.5 and PyYAML >=3.12

## Installation

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

First, install Miniconda from https://conda.io/miniconda.html, install as per their instructions then create an environment and activate:

> conda create -n fsl_sub python=3.6
> source activate fsl_sub

and install fsl_sub with:

> conda install fsl_sub

### Installing plugins

If you only need to run programs locally, fsl_sub ships with a local job plugin, but if you wish to target a grid backend you need to install an appropriate plugin. At this time there are two plugins available, one for Son of/Univa/Sun Grid Engine and one for Slurm. To install a plugin, ensure your environment is activated and then install the plugin with:

_(virtualenv)_

> pip install git+ssh://git@git.fmrib.ox.ac.uk/fsl/fsl_sub_plugin_SGE.git

_(conda)_

> conda install fsl_sub_plugin_SGE

(or _fsl_sub_plugin_Slurm.git_ and _fsl_sub_plugin_Slurm_ respectively)

### Configuration

A configuration file is required to describe your cluster environment, examples are provided with the plugins.

## Building

Prepare the source distribution

> python setup.py sdist

To build a wheel you need to install wheel into your Python build environment

> pip install wheel

fsl_sub is only compatible with python 3 so you will be building a Pure Python Wheel

> python setup.py bdist_wheel