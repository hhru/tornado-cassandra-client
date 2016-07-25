# coding=utf-8

import os

from setuptools import setup
from setuptools.command.build_py import build_py
from setuptools.command.test import test

from tassandra.version import version as __version__


class BuildHook(build_py):
    def run(self):
        build_py.run(self)

        build_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), self.build_lib, 'tassandra')
        with open(os.path.join(build_dir, 'version.py'), 'w') as version_file:
            version_file.write('version = "{0}"\n'.format(__version__))


class TestHook(test):
    def run_tests(self):
        import nose
        nose.main(argv=['tests', '-v'])

setup(
    name='tassandra',
    version=__import__('tassandra').__version__,
    description='Tornado cassandra client',
    long_description=open('README.md').read(),
    url='https://github.com/hhru/tornado-cassandra-client',
    cmdclass={'build_py': BuildHook, 'test': TestHook},
    packages=['tassandra'],
    install_requires=[
        'tornado',
        'cassandra-driver==2.1.3'
    ],
    tests_require=[
        'nose',
        'pep8',
        'ccm',
    ],
    zip_safe=False
)
