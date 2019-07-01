from contextlib import contextmanager

import pymssql

import dbt.exceptions
from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager
from dbt.logger import GLOBAL_LOGGER as logger


SQLSERVER_CREDENTIALS_CONTRACT = {
    'type': 'object',
    'additionalProperties': False,
    'properties': {
        'database': {
            'type': 'string',
        },
        'server': {
            'type': 'string',
        },
        'user': {
            'type': 'string',
        },
        'password': {
            'type': 'string',
        },
        'port': {
            'type': 'integer',
            'minimum': 0,
            'maximum': 65535,
        },
        'schema': {
            'type': 'string',
        },
    },
    'required': ['database', 'server', 'user', 'password', 'port', 'schema'],
}


class SQLServerCredentials(Credentials):
    SCHEMA = SQLSERVER_CREDENTIALS_CONTRACT

    @property
    def type(self):
        return 'sqlserver'

    def _connection_keys(self):
        # return an iterator of keys to pretty-print in 'dbt debug'
        return ('server', 'port', 'user', 'database', 'schema')


class SQLServerConnectionManager(SQLConnectionManager):
    TYPE = 'sqlserver'

    @contextmanager
    def exception_handler(self, sql):
        try:
            yield

        except pymssql.DatabaseError as e:
            logger.debug('SQL Server error: {}'.format(str(e)))

            try:
                # attempt to release the connection
                self.release()
            except pymssql.Error:
                logger.debug("Failed to release connection!")
                pass

            raise dbt.exceptions.DatabaseException(
                dbt.compat.to_string(e).strip())

        except Exception as e:
            logger.debug("Error running SQL: %s", sql)
            logger.debug("Rolling back transaction.")
            self.release()
            if isinstance(e, dbt.exceptions.RuntimeException):
                # during a sql query, an internal to dbt exception was raised.
                # this sounds a lot like a signal handler and probably has
                # useful information, so raise it without modification.
                raise

            raise dbt.exceptions.RuntimeException(e)


    @classmethod
    def open(cls, connection):
        if connection.state == 'open':
            logger.debug('Connection is already open, skipping open.')
            return connection

        credentials = cls.get_credentials(connection.credentials.incorporate())
        kwargs = {}

        try:
            handle = pymssql.connect(
                database=credentials.database,
                user=credentials.user,
                server=credentials.server,
                password=credentials.password,
                port=credentials.port,
                autocommit=True,
                **kwargs)

            connection.handle = handle
            connection.state = 'open'
        except pymssql.Error as e:
            logger.debug("Got an error when attempting to open a sql server "
                         "connection: '{}'"
                         .format(e))

            connection.handle = None
            connection.state = 'fail'

            raise dbt.exceptions.FailedToConnectException(str(e))

        return connection

    def cancel(self, connection):
        logger.debug("Cancel query")
        pass

    def add_begin_query(self):
        return self.add_query('BEGIN TRANSACTION', auto_begin=False)

    def add_commit_query(self):
        return self.add_query('COMMIT TRANSACTION', auto_begin=False)

    @classmethod
    def get_credentials(cls, credentials):
        return credentials

    @classmethod
    def get_status(cls, cursor):
        if cursor.rowcount == -1:
            status = 'OK'
        else:
            status = str(cursor.rowcount)
        return status
