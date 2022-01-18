"""Build and test configuration file. """
import os
import shutil

import nox

NOX_DIR = os.path.abspath(os.path.dirname(__file__))
DEFAULT_INTERPRETER = "3.9"
ALL_INTERPRETERS = ("3.6", "3.7", "3.8", "3.9")


def get_path(*names):
    return os.path.join(NOX_DIR, *names)


@nox.session(py=ALL_INTERPRETERS)
def unit(session):
    # Install all dependencies.
    session.install("-e", ".[testing]")

    # Run py.test against the unit tests.
    run_args = ["pytest"]
    if session.posargs:
        run_args.extend(session.posargs)
    else:
        run_args.extend(
            [
                "--cov=acidfs",
                "--cov=tests",
                "--cov-report=term-missing",
            ]
        )
    run_args.append(get_path("tests"))
    session.run(*run_args)


def run_black(session, use_check=False):
    args = ["black"]
    if use_check:
        args.append("--check")

    args.extend(
        [
            get_path("noxfile.py"),
            get_path("setup.py"),
            get_path("acidfs"),
            get_path("tests"),
        ]
    )

    session.run(*args)


@nox.session(py=DEFAULT_INTERPRETER)
def lint(session):
    """Run linters.

    Returns a failure if the linters find linting errors or sufficiently
    serious code quality issues.
    """
    session.install("flake8", "black")
    run_black(session, use_check=True)
    session.run("flake8", "acidfs", "tests", "setup.py")


@nox.session(py=DEFAULT_INTERPRETER)
def blacken(session):
    # Install all dependencies.
    session.install("black")
    # Run ``black``.
    run_black(session)


@nox.session(py=DEFAULT_INTERPRETER)
def docs(session):
    """Build the docs for this library."""

    session.install("-e", ".[docs]")

    shutil.rmtree(os.path.join("docs", "_build"), ignore_errors=True)
    session.run(
        "sphinx-build",
        "-W",  # warnings as errors
        "-T",  # show full traceback on exception
        "-b",
        "html",
        "-d",
        os.path.join("docs", "_build", "doctrees", ""),
        os.path.join("docs", ""),
        os.path.join("docs", "_build", "html", ""),
    )
