#!/usr/bin/env python

from setuptools import setup

setup(name='tap-postgres',
      version='0.1.0',
      description='Singer.io tap for extracting data from PostgreSQL',
      author='Stitch',
      url='https://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      install_requires=[
          'singer-python==5.3.1',
          'psycopg2==2.8.4',
          'strict-rfc3339==0.7',
          'sshtunnel==0.1.5'
      ],
      extras_require={
          'dev': [
              'autopep8>=1.5.3',
              'python-dotenv>=0.14.0',
              'nose>=1.3.7',
          ]
      },
      entry_points='''
          [console_scripts]
          tap-postgres=tap_postgres:main
      ''',
      packages=['tap_postgres', 'tap_postgres.sync_strategies']
      )
