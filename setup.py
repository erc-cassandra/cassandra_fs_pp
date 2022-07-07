from glob import glob
from os import path
from typing import Optional

from setuptools import setup

FULLVERSION = "0.0.1"
VERSION = FULLVERSION

write_version = True


def write_version_py(filename: Optional[str] = None) -> None:
    cnt = """\
version = '%s'
short_version = '%s'
"""
    if filename is None:
        filename = path.join(path.dirname(__file__), "cassandra_fs_pp", "version.py")

    a = open(filename, "w")
    try:
        a.write(cnt % (FULLVERSION, VERSION))
    finally:
        a.close()


if write_version:
    write_version_py()

# get all data in the datasets module

data_files = []

with open("README.md", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="Cassandra Firn Stations Post-Processing",
    version=FULLVERSION,
    description="Workflow for post-processing firn station data",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://www.github.com/erc-cassandra",
    author="Andrew Tedstone",
    license="BSD-3",
    packages=["fs_pp"]
    install_requires=[
        "pandas",
    ],
    scripts=["bin/fs_process_l1.py"],
    zip_safe=False,
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
        "License :: OSI Approved :: BSD License",
    ],
)
