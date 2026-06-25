"""Authentication and token helpers for the SQL Server adapter.

This module owns the shared normalization rules for auth labels, plus the
pyodbc-facing Azure token helpers used by the connection manager.
"""

from __future__ import annotations

import struct
import time
from itertools import chain, repeat
from typing import TYPE_CHECKING, Any, Callable, Dict, Mapping, Optional, cast

import dbt_common.exceptions

from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.sqlserver.sqlserver_constants import (
    AAD_TOKEN_AUTHENTICATIONS,
    CONNECTION_AUTH_ALIASES,
    CONNECTION_AUTH_PASSTHROUGH_KEYS,
    PYODBC_AUTH_ALIASES,
    SQLSERVER_BACKEND_MSSQL_PYTHON,
)
from dbt.adapters.sqlserver.sqlserver_runtime import (
    AZURE_CREDENTIAL_SCOPE,
    AccessTokenProtocol,
    _get_azure_access_token_class,
    _get_azure_identity_module,
    _get_cached_access_token,
)

if TYPE_CHECKING:
    from dbt.adapters.sqlserver.sqlserver_credentials import SQLServerBackend, SQLServerCredentials


logger = AdapterLogger("sqlserver")
AZURE_AUTH_FUNCTION_TYPE = Callable[[Any, Optional[str]], AccessTokenProtocol]


def is_mssql_python_backend(backend: "SQLServerBackend") -> bool:
    """Return whether the coerced backend enum targets ``mssql-python``."""

    return backend.value == SQLSERVER_BACKEND_MSSQL_PYTHON


def normalize_authentication_key(value: Optional[str]) -> str:
    """Normalize a SQL Server auth or lookup key for cross-layer comparisons."""

    return "" if value is None else value.replace("_", "").replace(" ", "").lower()


def is_active_directory_authentication(authentication: Optional[str]) -> bool:
    """Return whether an auth label targets one of the ActiveDirectory modes."""

    return normalize_authentication_key(authentication).startswith("activedirectory")


def normalize_mssql_python_authentication(
    authentication: Optional[str],
) -> Optional[str]:
    """Backend-layer auth normalization used while building connection strings."""

    authentication = authentication or ""
    key = normalize_authentication_key(authentication)
    if not key:
        return None

    if key in CONNECTION_AUTH_PASSTHROUGH_KEYS:
        return authentication.strip()

    if key in CONNECTION_AUTH_ALIASES:
        return CONNECTION_AUTH_ALIASES[key]

    return authentication.strip()


def normalize_pyodbc_authentication(authentication: Optional[str]) -> str:
    """Normalize auth labels for the pyodbc token path.

    Only the token-oriented aliases that participate in cached access-token
    retrieval are normalized here. Connection-string auth aliases such as
    ``ActiveDirectoryServicePrincipal`` are handled by the backend builders.
    """

    if key := normalize_authentication_key(authentication):
        return PYODBC_AUTH_ALIASES.get(key, key)
    return ""


def normalize_connection_authentication(
    authentication: Optional[str], mssql_python_backend: bool
) -> str:
    """Normalize auth labels for connection-string generation.

    Call this from connection-string builders and validation, not from profile
    parsing. The ``mssql-python`` path canonicalizes long-form connection
    strings, while the pyodbc path preserves its raw token-auth labels so
    ``get_pyodbc_attrs_before_credentials`` can apply its narrower alias map.
    """

    authentication = authentication or ""
    if mssql_python_backend:
        return normalize_mssql_python_authentication(authentication) or ""
    return authentication.strip()


def uses_aad_token_authentication(credentials: "SQLServerCredentials") -> bool:
    """Return whether pyodbc should request and cache an Azure access token.

    This is used by retry policy as well as token fetching, so manual
    ``ActiveDirectoryAccessToken`` profiles stay in the same retry bucket as
    the other AAD token modes.
    """

    authentication = normalize_pyodbc_authentication(credentials.authentication)
    return authentication in AAD_TOKEN_AUTHENTICATIONS


def get_environment_access_token(
    credentials: SQLServerCredentials, scope: Optional[str] = AZURE_CREDENTIAL_SCOPE
) -> AccessTokenProtocol:
    """
    Get an Azure access token by reading environment variables

    Parameters
    -----------
    credentials: SQLServerCredentials
        Credentials.

    Returns
    -------
    out : AccessToken
        The access token.
    """
    azure_identity = _get_azure_identity_module()
    return azure_identity.EnvironmentCredential().get_token(
        scope, timeout=credentials.login_timeout
    )


def get_msi_access_token(
    _credentials: SQLServerCredentials, scope: Optional[str] = AZURE_CREDENTIAL_SCOPE
) -> AccessTokenProtocol:
    """
    Get an Azure access token from the system's managed identity

    Parameters
    -----------
    credentials: SQLServerCredentials
        Credentials.

    Returns
    -------
    out : AccessToken
        The access token.
    """
    azure_identity = _get_azure_identity_module()
    return azure_identity.ManagedIdentityCredential().get_token(scope or AZURE_CREDENTIAL_SCOPE)


def convert_bytes_to_mswindows_byte_string(value: bytes) -> bytes:
    """
    Convert bytes to a Microsoft windows byte string.

    Parameters
    ----------
    value : bytes
        The bytes.

    Returns
    -------
    out : bytes
        The Microsoft byte string.
    """
    encoded_bytes = bytes(chain.from_iterable(zip(value, repeat(0))))
    return struct.pack("<i", len(encoded_bytes)) + encoded_bytes


def convert_access_token_to_mswindows_byte_string(token: AccessTokenProtocol) -> bytes:
    """
    Convert an access token to a Microsoft windows byte string.

    Parameters
    ----------
    token : AccessTokenProtocol
        The token.

    Returns
    -------
    out : bytes
        The Microsoft byte string.
    """
    value = bytes(token.token, "UTF-8")
    return convert_bytes_to_mswindows_byte_string(value)


def get_cli_access_token(
    credentials: SQLServerCredentials, scope: Optional[str] = AZURE_CREDENTIAL_SCOPE
) -> AccessTokenProtocol:
    """
    Get an Azure access token using the CLI credentials

    First login with:

    ```bash
    az login
    ```

    Parameters
    ----------
    credentials: SQLServerCredentials
        The credentials.

    Returns
    -------
    out : AccessToken
        Access token.
    """
    azure_identity = _get_azure_identity_module()
    return azure_identity.AzureCliCredential().get_token(scope, timeout=credentials.login_timeout)


def get_auto_access_token(
    credentials: SQLServerCredentials, scope: Optional[str] = AZURE_CREDENTIAL_SCOPE
) -> AccessTokenProtocol:
    """
    Get an Azure access token automatically through azure-identity

    Parameters
    -----------
    credentials: SQLServerCredentials
        Credentials.

    Returns
    -------
    out : AccessToken
        The access token.
    """
    azure_identity = _get_azure_identity_module()
    return azure_identity.DefaultAzureCredential().get_token(
        scope, timeout=credentials.login_timeout
    )


def get_sp_access_token(
    credentials: SQLServerCredentials, scope: Optional[str] = AZURE_CREDENTIAL_SCOPE
) -> AccessTokenProtocol:
    """
    Get an Azure access token using the SP credentials.

    Parameters
    ----------
    credentials : SQLServerCredentials
        Credentials.

    Returns
    -------
    out : AccessToken
        The access token.
    """
    azure_identity = _get_azure_identity_module()
    return azure_identity.ClientSecretCredential(
        str(credentials.tenant_id),
        str(credentials.client_id),
        str(credentials.client_secret),
    ).get_token(scope or AZURE_CREDENTIAL_SCOPE)


AZURE_AUTH_FUNCTIONS: Mapping[str, AZURE_AUTH_FUNCTION_TYPE] = {
    "cli": get_cli_access_token,
    "auto": get_auto_access_token,
    "environment": get_environment_access_token,
    "serviceprincipal": get_sp_access_token,
    "msi": get_msi_access_token,
}


def get_pyodbc_attrs_before_credentials(credentials: SQLServerCredentials) -> Dict:
    """Build the pyodbc authentication attrs used by the connection manager."""

    sql_copt_ss_access_token = 1256  # ODBC constant for access token

    authentication = normalize_pyodbc_authentication(credentials.authentication)

    if authentication in AZURE_AUTH_FUNCTIONS:
        token = _get_cached_access_token(
            credentials,
            authentication,
            AZURE_CREDENTIAL_SCOPE,
            lambda: AZURE_AUTH_FUNCTIONS[authentication](credentials, AZURE_CREDENTIAL_SCOPE),
        )
        token_bytes = convert_access_token_to_mswindows_byte_string(token)
        return {sql_copt_ss_access_token: token_bytes}

    if authentication == "activedirectoryaccesstoken":
        if credentials.access_token is None or credentials.access_token_expires_on is None:
            raise dbt_common.exceptions.DbtRuntimeError(
                "Access token and a non-zero access token expiry epoch timestamp are "
                "required for ActiveDirectoryAccessToken authentication."
            )

        if credentials.access_token_expires_on == 0:
            logger.warning(
                "ActiveDirectoryAccessToken expiry is 0; defaulting expiry to 75 minutes. "
                "Set access_token_expires_on explicitly to remove this message."
            )

        access_token = cast(
            AccessTokenProtocol,
            _get_azure_access_token_class()(
                token=credentials.access_token,
                expires_on=int(
                    time.time() + 4500.0
                    if credentials.access_token_expires_on == 0
                    else credentials.access_token_expires_on
                ),
            ),
        )
        return {
            sql_copt_ss_access_token: convert_access_token_to_mswindows_byte_string(access_token)
        }

    return {}
