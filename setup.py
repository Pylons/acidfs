import os
from setuptools import setup
from setuptools import find_packages

VERSION = "2.0"

requires = [
    "transaction",
]

tests_require = ["pytest", "pytest-cov", "nox"]
docs_require = ["Sphinx", "pylons-sphinx-themes"]

here = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(here, "README.rst")).read()

setup(
    name="acidfs",
    version=VERSION,
    description="ACID semantics for the filesystem.",
    long_description=README,
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Topic :: Database",
        "License :: Repoze Public License",
    ],
    keywords="git acid filesystem transaction",
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
        "testing": tests_require,
        "docs": docs_require,
    },
)
