#!/usr/bin/env python
from setuptools import find_packages
from distutils.core import setup

package_name = "dbt-synapse"
package_version = "0.18.0rc3"
description = """A Azure Synapse adpter plugin for dbt (data build tool)"""

authors_list = [
    'Nandan Hegde',
    'Anders Swanson'
]

setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=description,
    long_description_content_type='text/markdown',
    author=', '.join(authors_list),
    author_email="swanson.anders@gmail.com",
    url="https://github.com/swanderz/dbt-synapse",
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
        'dbt-core==0.18.0',
        'pyodbc>=4.0.27',
    ]
)
