#!/usr/bin/env python

from distutils.core import setup

setup(
    name='fsl_sub',
    version='2.0',
    description='FSL Cluster Submission Script',
    author='Duncan Mortimer',
    author_email='duncan.mortimer@ndcn.ox.ac.uk',
    url='https://git.fmrib.ox.ac.uk/fsl/fsl_sub',
    packages=['fsl_sub', 'fsl_sub.plugins', ],
    install_requires=['pyyaml', ],
    entry_points={
        'console_scripts': [
            'fsl_sub = fsl_sub.cmd:main',
        ]
    }
    )
