from contextlib import contextmanager

import pymssql

import dbt.exceptions
from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager
from dbt.logger import GLOBAL_LOGGER as logger

import time


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
    ALIASES = {
        'host': 'server',
        'pass': 'password'
    }
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
        # return self.add_query('BEGIN TRANSACTION', auto_begin=False)
        pass

    def add_commit_query(self):
        # return self.add_query('COMMIT TRANSACTION', auto_begin=False)
        pass

    def add_query(self, sql, auto_begin=True, bindings=None,
                  abridge_sql_log=False):
        connection = self.get_thread_connection()

        if bindings:
            # The sqlserver connector is more strict than, eg., psycopg2 -
            # which allows any iterable thing to be passed as a binding.
            bindings = tuple(bindings)

        if auto_begin and connection.transaction_open is False:
            self.begin()

        logger.debug('Using {} connection "{}".'
                     .format(self.TYPE, connection.name))

        with self.exception_handler(sql):
            if abridge_sql_log:
                logger.debug('On %s: %s....', connection.name, sql[0:512])
            else:
                logger.debug('On %s: %s', connection.name, sql)
            pre = time.time()

            cursor = connection.handle.cursor()
            cursor.execute(sql, bindings)

            logger.debug("SQL status: %s in %0.2f seconds",
                         self.get_status(cursor), (time.time() - pre))

            return connection, cursor

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

    def execute(self, sql, auto_begin=True, fetch=False):
        _, cursor = self.add_query(sql, auto_begin)
        status = self.get_status(cursor)
        if fetch:
            table = self.get_result_from_cursor(cursor)
        else:
            table = dbt.clients.agate_helper.empty_table()
        return status, table
