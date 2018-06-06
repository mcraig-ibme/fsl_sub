#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name='fsl_sub',
    version='2.0.0RC3',
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
    install_requires=['pyyaml'],
    python_requires='~=3.5',
    package_data={
        'fsl_sub': ['fsl_sub.yml'],
        'fsl_sub.plugins': ['fsl_sub_none.yml'],
    },
    include_package_data=True,
    entry_points={
        'console_scripts': [
            'fsl_sub=fsl_sub.cmd:main',
            'fsl_sub_config=fsl_sub.cmd:example_config',
        ]
    }
    )
