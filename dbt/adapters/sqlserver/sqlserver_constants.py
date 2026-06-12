"""Constants shared by the SQL Server adapter."""

from __future__ import annotations

SQLSERVER_BACKEND_PYODBC = "pyodbc"
SQLSERVER_BACKEND_MSSQL_PYTHON = "mssql-python"
SUPPORTED_SQLSERVER_BACKENDS = (
    SQLSERVER_BACKEND_PYODBC,
    SQLSERVER_BACKEND_MSSQL_PYTHON,
)
SUPPORTED_SQLSERVER_BACKENDS_MESSAGE = "Supported backends are 'pyodbc' and 'mssql-python'."

MSSQL_AUTH_ACTIVE_DIRECTORY_MSI = "ActiveDirectoryMSI"
MSSQL_AUTH_ACTIVE_DIRECTORY_INTEGRATED = "ActiveDirectoryIntegrated"
MSSQL_AUTH_ACTIVE_DIRECTORY_SERVICE_PRINCIPAL = "ActiveDirectoryServicePrincipal"
MSSQL_AUTH_ACTIVE_DIRECTORY_DEFAULT = "ActiveDirectoryDefault"
MSSQL_AUTH_ACTIVE_DIRECTORY_PASSWORD = "ActiveDirectoryPassword"
MSSQL_AUTH_ACTIVE_DIRECTORY_INTERACTIVE = "ActiveDirectoryInteractive"
MSSQL_AUTH_ACTIVE_DIRECTORY_DEVICE_CODE = "ActiveDirectoryDeviceCode"
MSSQL_AUTH_ACTIVE_DIRECTORY_ACCESS_TOKEN = "ActiveDirectoryAccessToken"
MSSQL_AUTH_CLI = "cli"
MSSQL_AUTH_ENVIRONMENT = "environment"

# pyodbc's token-fetch path uses short, token-oriented aliases. The backend
# builders handle the longer connection-string auth names separately.
PYODBC_AUTH_ALIASES: dict[str, str] = {
    "activedirectorymsi": "msi",
}

# Connection-string auth aliases are canonicalized separately for the
# mssql-python builder. Keep this map distinct from the pyodbc token aliases
# above so the two auth flows do not drift together accidentally.
CONNECTION_AUTH_ALIASES: dict[str, str] = {
    "msi": MSSQL_AUTH_ACTIVE_DIRECTORY_MSI,
    "activedirectorymsi": MSSQL_AUTH_ACTIVE_DIRECTORY_MSI,
    "activedirectoryintegrated": MSSQL_AUTH_ACTIVE_DIRECTORY_INTEGRATED,
    "adintegrated": MSSQL_AUTH_ACTIVE_DIRECTORY_INTEGRATED,
    "serviceprincipal": MSSQL_AUTH_ACTIVE_DIRECTORY_SERVICE_PRINCIPAL,
    "activedirectoryserviceprincipal": MSSQL_AUTH_ACTIVE_DIRECTORY_SERVICE_PRINCIPAL,
    "auto": MSSQL_AUTH_ACTIVE_DIRECTORY_DEFAULT,
    "default": MSSQL_AUTH_ACTIVE_DIRECTORY_DEFAULT,
    "activedirectorydefault": MSSQL_AUTH_ACTIVE_DIRECTORY_DEFAULT,
    "activedirectorypassword": MSSQL_AUTH_ACTIVE_DIRECTORY_PASSWORD,
    "activedirectoryinteractive": MSSQL_AUTH_ACTIVE_DIRECTORY_INTERACTIVE,
    "activedirectorydevicecode": MSSQL_AUTH_ACTIVE_DIRECTORY_DEVICE_CODE,
    "access_token": MSSQL_AUTH_ACTIVE_DIRECTORY_ACCESS_TOKEN,
    "activedirectoryaccesstoken": MSSQL_AUTH_ACTIVE_DIRECTORY_ACCESS_TOKEN,
    MSSQL_AUTH_CLI: MSSQL_AUTH_CLI,
    MSSQL_AUTH_ENVIRONMENT: MSSQL_AUTH_ENVIRONMENT,
}

CONNECTION_AUTH_PASSTHROUGH_KEYS: frozenset[str] = frozenset(
    {
        MSSQL_AUTH_CLI,
        MSSQL_AUTH_ENVIRONMENT,
    }
)

# Canonical pyodbc auth labels that should trigger Azure token caching and
# retryable InterfaceErrors. `ActiveDirectoryAccessToken` is included because
# it still flows through the same token-auth retry policy even though the token
# itself is supplied directly by the caller.
AAD_TOKEN_AUTHENTICATIONS: frozenset[str] = frozenset(
    {
        MSSQL_AUTH_CLI,
        MSSQL_AUTH_ENVIRONMENT,
        "auto",
        "msi",
        "serviceprincipal",
        "activedirectoryaccesstoken",
    }
)

MSSQL_PYTHON_UNSUPPORTED_AUTHENTICATIONS = {
    MSSQL_AUTH_CLI,
    MSSQL_AUTH_ENVIRONMENT,
    MSSQL_AUTH_ACTIVE_DIRECTORY_ACCESS_TOKEN,
}

# Keys whose values must never appear in log output. Keep this scoped to the
# exact connection-string fields that carry secrets so non-secret auth metadata
# does not get redacted.
SENSITIVE_CONNECTION_STRING_KEYS: frozenset[str] = frozenset(
    {
        "pwd",
        "password",
        "clientsecret",
        "accesstoken",
        "accountkey",
        "sharedaccesskey",
        "sharedaccesssignature",
        "uid",
        "userid",
        "user",
        "username",
        "clientid",
        "secret",
    }
)
# https://github.com/mkleehammer/pyodbc/wiki/Data-Types
datatypes = {
    "str": "varchar",
    "uuid.UUID": "uniqueidentifier",
    "uuid": "uniqueidentifier",
    "float": "float",
    "int": "int",
    "bytes": "varbinary",
    "bytearray": "varbinary",
    "bool": "bit",
    "datetime.date": "date",
    "datetime.datetime": "datetime2(6)",
    "datetime.time": "time",
    "decimal.Decimal": "decimal",
}
