#!/usr/bin/env python

from distutils.core import setup
from catkin_pkg.python_setup import generate_distutils_setup

setup_args = generate_distutils_setup(
    packages=[
        'birl_offline_data_handler',
        'birl_offline_data_handler_test',
        'birl_generic_data_handler',
    ],
    package_dir={'': 'src'}
)

setup(**setup_args)
