#!/usr/bin/env python

from setuptools import setup, find_packages
version = '0.0.2'


requires = open('requirements.txt').read().strip().split('\n')

setup(
    name='intake-solr',
    version=version,
    description='SOLR plugin for Intake',
    url='https://github.com/ContinuumIO/intake-solr',
    maintainer='Martin Durant',
    maintainer_email='mdurant@anaconda.com',
    license='BSD',
    py_modules=['intake_solr'],
    packages=find_packages(),
    package_data={'': ['*.csv', '*.yml', '*.html']},
    include_package_data=True,
    install_requires=requires,
    long_description=open('README.rst').read(),
    zip_safe=False,
)
