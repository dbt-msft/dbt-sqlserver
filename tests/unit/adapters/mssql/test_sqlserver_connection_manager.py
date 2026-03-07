import pytest
from azure.identity import AzureCliCredential

from dbt.adapters.sqlserver.sqlserver_credentials import SQLServerCredentials

# See
# https://github.com/Azure/azure-sdk-for-python/blob/azure-identity_1.5.0/sdk/identity/azure-identity/tests/test_cli_credential.py
CHECK_OUTPUT = AzureCliCredential.__module__ + ".subprocess.check_output"


@pytest.fixture
def credentials() -> SQLServerCredentials:
    credentials = SQLServerCredentials(
        host="fake.sql.sqlserver.net",
        database="dbt",
        schema="sqlserver",
    )
    return credentials
