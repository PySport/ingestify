from distutils.core import setup

import setuptools


def setup_package():
    setup(
        name="ingestify",
        version="0.0.1",
        author="Koen Vossen",
        author_email="info@koenvossen.nl",
        license="AGPL",
        packages=setuptools.find_packages(exclude=["tests"]),
    )


if __name__ == "__main__":
    setup_package()
