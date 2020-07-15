#!/usr/bin/env python

from setuptools import setup, find_packages
import versioneer


requires = open('requirements.txt').read().strip().split('\n')

setup(
    name='intake-solr',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description='SOLR plugin for Intake',
    url='https://github.com/intake/intake-solr',
    maintainer='Martin Durant',
    maintainer_email='mdurant@anaconda.com',
    license='BSD',
    py_modules=['intake_solr'],
    packages=find_packages(),
    entry_points={
        'intake.drivers': [
            'solr = intake_solr.source:SOLRSequenceSource',
            'solrtab = intake_solr.source:SOLRTableSource',
        ]},
    package_data={'': ['*.csv', '*.yml', '*.html']},
    include_package_data=True,
    install_requires=requires,
    long_description=open('README.rst').read(),
    zip_safe=False,
)
