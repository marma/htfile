#!/usr/bin/env python3

from distutils.core import setup
import setuptools

setup(
    name='htfile',
    version='0.0.1a',
    description='HTTP request with Range support as file-like object',
    author='Martin Malmsten',
    author_email='martin@martinmalmsten.net',
    url='https://github.com/marma/htfile',
    install_requires=[ 'requests>=2.18.4' ],
    packages=[ 'htfile' ])

