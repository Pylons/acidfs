from setuptools import setup
from setuptools import find_packages
import sys

VERSION = '0.0'

requires = [
    'transaction',
]
tests_require = requires

if sys.version < '2.7':
    tests_require += 'unittest2'

testing_extras = tests_require + ['nose', 'coverage']

setup(name='acidfs',
      version=VERSION,
      packages=find_packages(),
      include_package_data=True,
      zip_safe=False,
      install_requires=requires,
      tests_require=tests_require,
      extras_require={'testing': testing_extras},
      test_suite="acidfs.tests")
