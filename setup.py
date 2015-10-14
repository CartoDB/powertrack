# -*- coding: utf-8 -*-

import os
from setuptools import setup

README = open(os.path.join(os.path.dirname(__file__), 'README.md')).read()

# allow setup.py to be run from any path
os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))


def get_packages(package):
    """
    Return root package and all sub-packages.
    """
    return [dirpath
            for dirpath, dirnames, filenames in os.walk(package)
            if os.path.exists(os.path.join(dirpath, '__init__.py'))]

setup(
    name='python-powertrack',
    version='0.1.6',
    packages=get_packages('powertrack'),
    install_requires=[
        'requests',
    ],
    include_package_data=True,
    license='MIT',
    description="Access GNIP's Powertrack APIs in Python.",
    long_description=README,
    url='http://www.cartodb.com/',
    author='Daniel Carrión',
    author_email='daniel@cartodb.com',
    classifiers=[
        'Intended Audience :: Developers',
        'License :: Other/Proprietary License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
)
