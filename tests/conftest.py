import pytest
import os
import json

# Import the fuctional fixtures as a plugin
# Note: fixtures with session scope need to be local

pytest_plugins = ["dbt.tests.fixtures.project"]

# The profile dictionary, used to write out profiles.yml
@pytest.fixture(scope="class")
def dbt_profile_target():
    return {
        'type': 'sqlserver',
        'threads': 1,
        'driver': os.getenv('SQLSERVER_TEST_DRIVER'),
        'host': os.getenv('SQLSERVER_TEST_HOST'),
        'port': int(os.getenv('SQLSERVER_TEST_PORT')),
        'user': os.getenv('SQLSERVER_TEST_USER'),
        'pass': os.getenv('SQLSERVER_TEST_PASS'),
        'dbname': os.getenv('SQLSERVER_TEST_DBNAME'),
    }