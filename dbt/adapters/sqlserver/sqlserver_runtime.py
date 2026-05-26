"""Internal runtime state for optional backend imports and token caches."""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, NamedTuple, Optional, Protocol, Type, cast

import dbt_common.exceptions

_UNSET = object()

AZURE_CREDENTIAL_SCOPE = "https://database.windows.net//.default"


class AccessTokenProtocol(Protocol):
    token: str
    expires_on: int


class TokenCredentialProtocol(Protocol):
    def get_token(self, *scopes: Optional[str], **kwargs: Any) -> AccessTokenProtocol: ...


class CredentialFactory(Protocol):
    def __call__(self, *args: Any, **kwargs: Any) -> TokenCredentialProtocol: ...


class AzureIdentityModuleProtocol(Protocol):
    AzureCliCredential: CredentialFactory
    DefaultAzureCredential: CredentialFactory
    EnvironmentCredential: CredentialFactory
    ManagedIdentityCredential: CredentialFactory
    ClientSecretCredential: CredentialFactory


class AzureCredentialsModuleProtocol(Protocol):
    AccessToken: Type[AccessTokenProtocol]


class PyodbcModuleProtocol(Protocol):
    InternalError: type[Exception]
    OperationalError: type[Exception]
    InterfaceError: type[Exception]
    DatabaseError: type[Exception]
    pooling: bool

    def connect(self, *args: Any, **kwargs: Any) -> Any: ...


class MssqlPythonModuleProtocol(Protocol):
    InternalError: type[Exception]
    OperationalError: type[Exception]
    InterfaceError: type[Exception]
    DatabaseError: type[Exception]

    def pooling(
        self,
        max_size: int = 100,
        idle_timeout: int = 600,
        enabled: bool = True,
    ) -> None: ...

    def connect(self, *args: Any, **kwargs: Any) -> Any: ...


@dataclass
class AccessToken:
    token: str
    expires_on: int


@dataclass(frozen=True)
class SQLServerRuntimeSnapshot:
    """Shallow copy of mutable runtime state used by focused tests."""

    pyodbc_module: Any
    pyodbc_import_error: Optional[ModuleNotFoundError]
    mssql_python_module: Any
    mssql_python_import_error: Optional[ModuleNotFoundError]
    azure_credentials_module: Any
    azure_credentials_import_error: Optional[ModuleNotFoundError]
    azure_identity_module: Any
    azure_identity_import_error: Optional[ModuleNotFoundError]
    access_token_cache: dict[Any, Any]
    timeout_warning_logged: bool


class SQLServerRuntimeState:
    """Own the mutable state behind lazy imports and shared caches.

    Lifecycle and ownership:
    - This singleton is the only supported home for optional backend module
      imports, cached DatabaseError classes, Azure access tokens, and the
      one-shot timeout warning flag.
    - Public helpers in ``sqlserver_runtime.py`` are the intended access
      points; callers should avoid reading or mutating the fields directly.

    Thread-safety:
    - ``module_load_lock`` protects lazy imports and cached exception types.
    - ``access_token_cache_lock`` protects token reads/writes.
    - ``timeout_warning_lock`` ensures the warning is emitted at most once.
    """

    def __init__(self) -> None:
        self.pyodbc_module: Any = None
        self.pyodbc_import_error: Optional[ModuleNotFoundError] = None
        self.mssql_python_module: Any = None
        self.mssql_python_import_error: Optional[ModuleNotFoundError] = None
        self.azure_credentials_module: Any = None
        self.azure_credentials_import_error: Optional[ModuleNotFoundError] = None
        self.azure_identity_module: Any = None
        self.azure_identity_import_error: Optional[ModuleNotFoundError] = None
        self.access_token_cache: dict[Any, Any] = {}
        self.timeout_warning_logged = False
        self._pyodbc_db_error: Optional[type[Exception]] = None
        self._mssql_python_db_error: Optional[type[Exception]] = None

        self.module_load_lock = threading.Lock()
        self.access_token_cache_lock = threading.Lock()
        self.timeout_warning_lock = threading.Lock()

    def reset_modules(self) -> None:
        with self.module_load_lock:
            self.pyodbc_module = None
            self.pyodbc_import_error = None
            self.mssql_python_module = None
            self.mssql_python_import_error = None
            self.azure_credentials_module = None
            self.azure_credentials_import_error = None
            self.azure_identity_module = None
            self.azure_identity_import_error = None
            self._pyodbc_db_error = None
            self._mssql_python_db_error = None

    def reset_access_token_cache(self) -> None:
        with self.access_token_cache_lock:
            self.access_token_cache.clear()

    def reset_timeout_warning(self) -> None:
        with self.timeout_warning_lock:
            self.timeout_warning_logged = False

    def reset(self) -> None:
        self.reset_modules()
        self.reset_access_token_cache()
        self.reset_timeout_warning()

    def get_pyodbc_database_error(self) -> Optional[type[Exception]]:
        with self.module_load_lock:
            if self._pyodbc_db_error is not None:
                return self._pyodbc_db_error
            if self.pyodbc_module is not None:
                self._pyodbc_db_error = self.pyodbc_module.DatabaseError
                return self._pyodbc_db_error
        return None

    def get_mssql_python_database_error(self) -> Optional[type[Exception]]:
        with self.module_load_lock:
            if self._mssql_python_db_error is not None:
                return self._mssql_python_db_error
            if self.mssql_python_module is not None:
                self._mssql_python_db_error = self.mssql_python_module.DatabaseError
                return self._mssql_python_db_error
        return None

    def get_cached_access_token(
        self,
        cache_key: Any,
        loader: Callable[[], Any],
        *,
        refresh_buffer_seconds: int = 300,
    ) -> Any:
        """Return a cached token without holding the lock during refresh."""

        with self.access_token_cache_lock:
            token = self.access_token_cache.get(cache_key)
            if token and (token.expires_on - time.time() >= refresh_buffer_seconds):
                return token

        token = loader()

        with self.access_token_cache_lock:
            cached_token = self.access_token_cache.get(cache_key)
            if cached_token and (cached_token.expires_on - time.time() >= refresh_buffer_seconds):
                return cached_token
            self.access_token_cache[cache_key] = token
            return token

    def take_timeout_warning(self) -> bool:
        with self.timeout_warning_lock:
            if self.timeout_warning_logged:
                return False
            self.timeout_warning_logged = True
            return True

    def snapshot(self) -> SQLServerRuntimeSnapshot:
        with self.module_load_lock:
            pyodbc_module = self.pyodbc_module
            pyodbc_import_error = self.pyodbc_import_error
            mssql_python_module = self.mssql_python_module
            mssql_python_import_error = self.mssql_python_import_error
            azure_credentials_module = self.azure_credentials_module
            azure_credentials_import_error = self.azure_credentials_import_error
            azure_identity_module = self.azure_identity_module
            azure_identity_import_error = self.azure_identity_import_error
        with self.access_token_cache_lock:
            access_token_cache = dict(self.access_token_cache)
        with self.timeout_warning_lock:
            timeout_warning_logged = self.timeout_warning_logged

        return SQLServerRuntimeSnapshot(
            pyodbc_module=pyodbc_module,
            pyodbc_import_error=pyodbc_import_error,
            mssql_python_module=mssql_python_module,
            mssql_python_import_error=mssql_python_import_error,
            azure_credentials_module=azure_credentials_module,
            azure_credentials_import_error=azure_credentials_import_error,
            azure_identity_module=azure_identity_module,
            azure_identity_import_error=azure_identity_import_error,
            access_token_cache=access_token_cache,
            timeout_warning_logged=timeout_warning_logged,
        )

    def configure_for_test(
        self,
        *,
        pyodbc_module: Any = _UNSET,
        pyodbc_import_error: Any = _UNSET,
        mssql_python_module: Any = _UNSET,
        mssql_python_import_error: Any = _UNSET,
        azure_credentials_module: Any = _UNSET,
        azure_credentials_import_error: Any = _UNSET,
        azure_identity_module: Any = _UNSET,
        azure_identity_import_error: Any = _UNSET,
        access_token_cache: Any = _UNSET,
        timeout_warning_logged: Any = _UNSET,
    ) -> None:
        """Targeted mutation helper used by tests instead of poking globals."""

        with self.module_load_lock:
            if pyodbc_module is not _UNSET:
                self.pyodbc_module = pyodbc_module
            if pyodbc_import_error is not _UNSET:
                self.pyodbc_import_error = pyodbc_import_error
            if mssql_python_module is not _UNSET:
                self.mssql_python_module = mssql_python_module
            if mssql_python_import_error is not _UNSET:
                self.mssql_python_import_error = mssql_python_import_error
            if azure_credentials_module is not _UNSET:
                self.azure_credentials_module = azure_credentials_module
            if azure_credentials_import_error is not _UNSET:
                self.azure_credentials_import_error = azure_credentials_import_error
            if azure_identity_module is not _UNSET:
                self.azure_identity_module = azure_identity_module
            if azure_identity_import_error is not _UNSET:
                self.azure_identity_import_error = azure_identity_import_error

        if access_token_cache is not _UNSET:
            with self.access_token_cache_lock:
                self.access_token_cache = dict(access_token_cache)

        if timeout_warning_logged is not _UNSET:
            with self.timeout_warning_lock:
                self.timeout_warning_logged = bool(timeout_warning_logged)


_RUNTIME_STATE = SQLServerRuntimeState()


class _AccessTokenCacheKey(NamedTuple):
    """Dimensions that uniquely identify a cached Azure access token.

    Keeping these fields in one named type means future changes to caching
    strategy (e.g. adding a subscription dimension) only require edits here
    rather than hunting through the cache dict type hint and the builder.
    """

    authentication: str
    scope: str
    backend: Any
    tenant_id: Optional[str]
    client_id: Optional[str]


def _access_token_cache_key(
    credentials: Any,
    authentication: str,
    scope: str,
) -> _AccessTokenCacheKey:
    """Build the cache key used to memoize Azure access tokens."""

    return _AccessTokenCacheKey(
        authentication=authentication,
        scope=scope,
        backend=credentials.backend,
        tenant_id=credentials.tenant_id,
        client_id=credentials.client_id,
    )


def _get_azure_access_token_class() -> Type[Any]:
    """Return the Azure ``AccessToken`` class or the local fallback."""

    with _RUNTIME_STATE.module_load_lock:
        if _RUNTIME_STATE.azure_credentials_module is not None:
            return _RUNTIME_STATE.azure_credentials_module.AccessToken

        if _RUNTIME_STATE.azure_credentials_import_error is not None:
            return AccessToken

        try:
            # type: ignore[import]
            import azure.core.credentials as azure_credentials
        except ModuleNotFoundError as exc:
            _RUNTIME_STATE.azure_credentials_import_error = exc
            return AccessToken

        _RUNTIME_STATE.azure_credentials_module = cast(
            AzureCredentialsModuleProtocol, azure_credentials
        )
        return _RUNTIME_STATE.azure_credentials_module.AccessToken


def _missing_azure_identity_error() -> dbt_common.exceptions.DbtRuntimeError:
    return dbt_common.exceptions.DbtRuntimeError(
        "Azure authentication requires the optional dependency 'azure-identity'. "
        "Install it with `pip install azure-identity` or use a non-Azure "
        "authentication mode."
    )


def _get_azure_identity_module() -> AzureIdentityModuleProtocol:
    """Import and cache ``azure.identity`` when Azure auth is requested."""

    with _RUNTIME_STATE.module_load_lock:
        if _RUNTIME_STATE.azure_identity_module is not None:
            return _RUNTIME_STATE.azure_identity_module

        if _RUNTIME_STATE.azure_identity_import_error is not None:
            raise _missing_azure_identity_error() from _RUNTIME_STATE.azure_identity_import_error

        try:
            import azure.identity as azure_identity  # type: ignore[import]
        except ModuleNotFoundError as exc:
            _RUNTIME_STATE.azure_identity_import_error = exc
            raise _missing_azure_identity_error() from exc

        _RUNTIME_STATE.azure_identity_module = cast(AzureIdentityModuleProtocol, azure_identity)
        return _RUNTIME_STATE.azure_identity_module


def reset_runtime_state_for_test() -> None:
    """Clear optional-backend runtime state in focused tests."""

    _RUNTIME_STATE.reset()


def get_runtime_state_for_test() -> SQLServerRuntimeSnapshot:
    """Return a shallow snapshot of optional-backend runtime state for tests."""

    return _RUNTIME_STATE.snapshot()


def configure_runtime_state_for_test(**kwargs: Any) -> None:
    """Update selected runtime-state fields in focused tests."""

    _RUNTIME_STATE.configure_for_test(**kwargs)


def _missing_pyodbc_error() -> dbt_common.exceptions.DbtRuntimeError:
    return dbt_common.exceptions.DbtRuntimeError(
        "The legacy `pyodbc` backend was requested, but the optional dependency "
        "`pyodbc` is not installed. Install it with `pip install pyodbc` "
        "or set `backend: mssql-python` in the profile."
    )


def _get_pyodbc() -> PyodbcModuleProtocol:
    """Import and cache ``pyodbc`` on first use.

    Expected Inputs: None.
    Invariants: Thread-safe lazy import protected by module_load_lock. Raises
    DbtRuntimeError if pyodbc is missing.
    Integration: Provides the pyodbc module to the connection manager and auth handlers.
    """

    with _RUNTIME_STATE.module_load_lock:
        if _RUNTIME_STATE.pyodbc_module is not None:
            return _RUNTIME_STATE.pyodbc_module

        if _RUNTIME_STATE.pyodbc_import_error is not None:
            raise _missing_pyodbc_error() from _RUNTIME_STATE.pyodbc_import_error

        try:
            import pyodbc as imported_pyodbc  # type: ignore[import]
        except ModuleNotFoundError as exc:
            _RUNTIME_STATE.pyodbc_import_error = exc
            raise _missing_pyodbc_error() from exc

        _RUNTIME_STATE.pyodbc_module = cast(PyodbcModuleProtocol, imported_pyodbc)
        return _RUNTIME_STATE.pyodbc_module


def _missing_mssql_python_error() -> dbt_common.exceptions.DbtRuntimeError:
    return dbt_common.exceptions.DbtRuntimeError(
        "The `mssql-python` backend was requested, but the optional dependency "
        "`mssql-python` is not installed. Install it with `pip install mssql-python` "
        "or set `backend: pyodbc` in the profile."
    )


def _get_mssql_python() -> MssqlPythonModuleProtocol:
    """Import and cache the optional ``mssql_python`` backend on demand.

    Expected Inputs: None.
    Invariants: Thread-safe lazy import protected by module_load_lock. Raises
    DbtRuntimeError if mssql_python is missing.
    Integration: Provides the mssql_python module to the connection manager.
    """

    with _RUNTIME_STATE.module_load_lock:
        if _RUNTIME_STATE.mssql_python_module is not None:
            return _RUNTIME_STATE.mssql_python_module

        if _RUNTIME_STATE.mssql_python_import_error is not None:
            raise _missing_mssql_python_error() from _RUNTIME_STATE.mssql_python_import_error

        try:
            # type: ignore[import]
            import mssql_python as imported_mssql_python
        except ModuleNotFoundError as exc:
            _RUNTIME_STATE.mssql_python_import_error = exc
            raise _missing_mssql_python_error() from exc

        _RUNTIME_STATE.mssql_python_module = cast(MssqlPythonModuleProtocol, imported_mssql_python)
        return _RUNTIME_STATE.mssql_python_module


def _get_cached_access_token(
    credentials: Any,
    authentication: str,
    scope: str,
    loader: Callable[[], Any],
) -> AccessTokenProtocol:
    """Return a cached Azure token using the shared runtime state."""

    cache_key = _access_token_cache_key(credentials, authentication, scope)
    return cast(AccessTokenProtocol, _RUNTIME_STATE.get_cached_access_token(cache_key, loader))
