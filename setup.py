import builtins
import os
from distutils.core import setup

import setuptools


def package_files(directory):
    paths = []
    for path, directories, filenames in os.walk(directory):
        for filename in filenames:
            paths.append(os.path.join("..", path, filename))
    return paths


def setup_package():
    builtins.__INGESTIFY_SETUP__ = True
    import ingestify

    with open("README.md", "r") as f:
        readme = f.read()

    setup(
        name="ingestify",
        version=ingestify.__version__,
        author="Koen Vossen",
        author_email="info@koenvossen.nl",
        license="AGPL",
        description="Data Ingestion Framework",
        long_description=readme,
        long_description_content_type="text/markdown",
        packages=setuptools.find_packages(exclude=["tests"]),
        entry_points={"console_scripts": ["ingestify = ingestify.cmdline:main"]},
        install_requires=[
            "requests>=2.0.0,<3",
            "SQLAlchemy>=2,<3",
            "click>=8",
            "python-dotenv",
            "pyaml_env",
            "boto3",
            "pydantic>=2.0.0",
        ],
        extras_require={"test": ["pytest>=6.2.5,<7", "pytz"]},
    )


if __name__ == "__main__":
    setup_package()
