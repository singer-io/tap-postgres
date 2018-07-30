#!/usr/bin/env python

from setuptools import setup

setup(name='tap-postgres',
      version='0.0.19',
      description='Singer.io tap for extracting data from Oracle',
      author='Stitch',
      url='https://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      install_requires=[
          'singer-python==5.1.5',
          'requests==2.12.4',
	  'psycopg2==2.7.4',
	  'strict-rfc3339==0.7',
	  'nose==1.3.7'
      ],
      entry_points='''
          [console_scripts]
          tap-postgres=tap_postgres:main
      ''',
      packages=['tap_postgres', 'tap_postgres.sync_strategies']
)
