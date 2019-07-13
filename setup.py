#!/usr/bin/env python
from setuptools import find_packages
from distutils.core import setup
import os

package_name = "dbt-sqlserver"
package_version = "0.1.1"
description = """The sqlserver adpter plugin for dbt (data build tool)"""

#this_directory = os.path.abspath(os.path.dirname(__file__))
#with open(os.path.join(this_directory, 'README.md')) as f:
#    long_description = f.read()

setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=description,
    long_description_content_type='text/markdown',
    author="Mikael Ene",
    author_email="mikael.ene@gmail.com",
    url="https://github.com/mikaelene/dbt-sqlserver",
    packages=find_packages(),
    package_data={
        'dbt': [
            'include/sqlserver/dbt_project.yml',
            'include/sqlserver/macros/*.sql',
            'include/sqlserver/macros/**/*.sql',
            'include/sqlserver/macros/**/**/*.sql',
        ]
    },
    install_requires=[
        'dbt-core>=0.14.0',
        'cython>=0.29.10',
        'pymssql>=2.1.4',
    ]
)
