import os
from setuptools import setup
from setuptools import find_packages
import sys

VERSION = '1.1'

requires = [
    'transaction',
]

PY3 = sys.version_info[0] == 3
PY2 = sys.version_info[0] == 2

tests_require = requires + ['pytest', 'pytest-cov']

if PY2:
    tests_require += ['mock']

testing_extras = tests_require + ['tox']

doc_extras = ['Sphinx']

here = os.path.abspath(os.path.dirname(__file__))
try:
    README = open(os.path.join(here, 'README.rst')).read()
    CHANGES = open(os.path.join(here, 'CHANGES.txt')).read()
except IOError:
    README = CHANGES = ''

setup(
    name='acidfs',
    version=VERSION,
    description='ACID semantics for the filesystem.',
    long_description=README + '\n\n' + CHANGES,
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2.6",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.2",
        "Programming Language :: Python :: 3.3",
        "Programming Language :: Python :: Implementation :: CPython",
        #"Programming Language :: Python :: Implementation :: PyPy",
        "Topic :: Database",
        "License :: Repoze Public License",
    ],
    keywords='git acid filesystem transaction',
    author="Chris Rossi",
    author_email="pylons-discuss@googlegroups.com",
    url="http://pylonsproject.org",
    license="BSD-derived (http://www.repoze.org/LICENSE.txt)",
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    install_requires=requires,
    tests_require=tests_require,
    extras_require={
        'testing': testing_extras,
        'docs': doc_extras,
    },
    test_suite="acidfs.tests",
)
