import os

from setuptools import setup
from setuptools.command.build_py import build_py
from setuptools.command.test import test

from tassandra.version import version


class BuildHook(build_py):
    def run(self):
        build_py.run(self)

        build_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), self.build_lib, 'tassandra')
        with open(os.path.join(build_dir, 'version.py'), 'w') as version_file:
            version_file.write('version = "{0}"\n'.format(version))


class TestHook(test):
    def run_tests(self):
        import nose
        nose.main(argv=['nosetests', 'tests', '-v'])

setup(
    name='tassandra',
    version=version,
    description='Tornado cassandra client',
    long_description=open('README.md').read(),
    url='https://github.com/hhru/tornado-cassandra-client',
    cmdclass={'build_py': BuildHook, 'test': TestHook},
    packages=['tassandra'],
    test_suite='tests',
    install_requires=[
        'tornado==6.3.2',
        'cassandra-driver==3.25.0'
    ],
    tests_require=[
        'ccm',
        'nose',
        'pycodestyle == 2.2.0',
    ],
    zip_safe=False
)
