import os
from distutils.core import setup

import setuptools


def package_files(directory):
    paths = []
    for (path, directories, filenames) in os.walk(directory):
        for filename in filenames:
            paths.append(os.path.join('..', path, filename))
    return paths


def setup_package():
    setup(
        name="ingestify",
        version="0.0.1",
        author="Koen Vossen",
        author_email="info@koenvossen.nl",
        license="AGPL",
        packages=setuptools.find_packages(exclude=["tests"]),
        package_data={
            '': package_files('ingestify/static'),
        },
        entry_points={"console_scripts": ["ingestify = ingestify.cmdline:main"]},
        install_requires=[
            "requests>=2.0.0,<3",
            "SQLAlchemy",
            "dataclass_factory",
            "cloudpickle",
            "click",
            "jinja2",
            "python-dotenv",
            "pyaml_env"
        ],
    )


if __name__ == "__main__":
    setup_package()
