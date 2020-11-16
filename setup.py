#!/usr/bin/env python

from setuptools import setup, find_packages
from fsl_sub.version import VERSION

setup(
    name='fsl_sub',
    version=VERSION,
    description='FSL Cluster Submission Script',
    author='Duncan Mortimer',
    author_email='duncan.mortimer@ndcn.ox.ac.uk',
    url='https://git.fmrib.ox.ac.uk/fsl/fsl_sub',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Science/Research',
        'Topic :: Scientific/Engineering :: Medical Science Apps.',
        'License :: Other/Proprietary License',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Natural Language :: English',
        'Environment :: Console',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX :: Linux',
    ],
    keywords='FSL fsl Neuroimaging neuroimaging cluster'
             ' grid slurm grid engine',
    project_urls={
        'Documentation': 'https://fsl.fmrib.ox.ac.uk/fsl/fslwiki',
        'Source': 'https://git.fmrib.ox.ac.uk/fsl/fsl_sub'
    },
    packages=find_packages(),
    license='FSL License',
    install_requires=['ruamel.yaml>=0.16.7'],
    setup_requires=['ruamel.yaml'],
    python_requires='~=3.5',
    package_data={
        'fsl_sub': [
            'default_config.yml', 'default_coproc_config.yml',
            'example_queue_config.yml', 'example_coproc_config.yml',
            'README.md', 'INSTALL.md', 'CONFIGURATION.md', 'CHANGES.md', ],
        'fsl_sub.plugins': ['fsl_sub_shell.yml'],
    },
    include_package_data=True,
    entry_points={
        'console_scripts': [
            'fsl_sub=fsl_sub.cmdline:main',
            'fsl_sub_config=fsl_sub.cmdline:example_config',
            'fsl_sub_report=fsl_sub.cmdline:report_cmd',
            'fsl_sub_plugin=fsl_sub.cmdline:install_plugin',
            'fsl_sub_update=fsl_sub.cmdline:update',
        ]
    }
)
