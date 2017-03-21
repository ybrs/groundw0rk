#!/usr/bin/env python

from setuptools import setup, find_packages

VERSION = '0.0.1'
DESCRIPTION = 'extends diamond metric collector to send metrics to groundw0rk'

setup(
    name='diamond_gw0',
    version=VERSION,
    description=DESCRIPTION,
    author='ybrs',
    license='BSD',
    url="https://github.com/ybrs/groundw0rk/diamond_gw0/",
    author_email='aybars.badur@gmail.com',
    packages=['diamond_gw0'],
    install_requires=[
        'requests',
    ],
    classifiers = [
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Database',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ]
)
