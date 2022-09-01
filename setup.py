#!/usr/bin/env python

from setuptools import setup

setup(name='tap-postgres',
      version='0.2.1',
      description='Singer.io tap for extracting data from PostgreSQL',
      author='Stitch',
      url='https://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      install_requires=[
          'singer-python==5.3.1',
          'psycopg2-binary==2.7.4',
          'strict-rfc3339==0.7',
      ],
      extras_require={
          'dev': [
              'ipdb',
              'pylint==2.6.0',
              'nose==1.3.7',
          ]
      },
      entry_points='''
          [console_scripts]
          tap-postgres=tap_postgres:main
      ''',
      packages=['tap_postgres', 'tap_postgres.sync_strategies']
)
