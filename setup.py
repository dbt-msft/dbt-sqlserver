#!/usr/bin/env python
from setuptools import find_packages
from distutils.core import setup
import os
import re

this_directory = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_directory, 'README.md')) as f:
    long_description = f.read()


package_name = "dbt-sqlserver"


# get this from a separate file
def _dbt_sqlserver_version():
    _version_path = os.path.join(
        this_directory, 'dbt', 'adapters', 'sqlserver', '__version__.py'
    )
    _version_pattern = r'''version\s*=\s*["'](.+)["']'''
    with open(_version_path) as f:
        match = re.search(_version_pattern, f.read().strip())
        if match is None:
            raise ValueError(f'invalid version at {_version_path}')
        return match.group(1)


package_version = _dbt_sqlserver_version()
description = """A sqlserver adapter plugin for dbt (data build tool)"""

dbt_version = '0.20'
# the package version should be the dbt version, with maybe some things on the
# ends of it. (0.18.1 vs 0.18.1a1, 0.18.1.1, ...)
if not package_version.startswith(dbt_version):
    raise ValueError(
        f'Invalid setup.py: package_version={package_version} must start with '
        f'dbt_version={dbt_version}'
    )

setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=description,
    long_description_content_type="text/markdown",
    license="MIT",
    author="Mikael Ene",
    author_email="mikael.ene@eneanalytics.com",
    url="https://github.com/mikaelene/dbt-sqlserver",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "dbt-core~=0.20.0",
        "pyodbc~=4.0.31",
        "azure-identity>=1.6.0",
    ]
)
