from contextlib import contextmanager

import pyodbc
import time

import dbt.exceptions
from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager

from dbt.logger import GLOBAL_LOGGER as logger

from dataclasses import dataclass
from typing import Optional


@dataclass
class SQLServerCredentials(Credentials):
    driver: str
    host: str
    database: str
    schema: str
    port: Optional[int] = 1433
    UID: Optional[str] = None
    PWD: Optional[str] = None
    # "sql", "ActiveDirectoryPassword" or "ActiveDirectoryInteractive"
    authentication: Optional[str] = "sql"
    encrypt: Optional[str] = "yes"

    _ALIASES = {
        'user': 'UID'
        , 'username': 'UID'
        , 'pass': 'PWD'
        , 'password': 'PWD'
        , 'server': 'host'
        , 'auth': 'authentication'
    }

    @property
    def type(self):
        return 'sqlserver'

    def _connection_keys(self):
        # return an iterator of keys to pretty-print in 'dbt debug'
        # raise NotImplementedError
        return 'server', 'database', 'schema', \
               'port', 'UID', 'authentication', 'encrypt'


class SQLServerConnectionManager(SQLConnectionManager):
    TYPE = 'sqlserver'

    @contextmanager
    def exception_handler(self, sql):
        try:
            yield

        except pyodbc.DatabaseError as e:
            logger.debug('Database error: {}'.format(str(e)))

            try:
                # attempt to release the connection
                self.release()
            except pyodbc.Error:
                logger.debug("Failed to release connection!")
                pass

            raise dbt.exceptions.DatabaseException(str(e).strip()) from e

        except Exception as e:
            logger.debug(f"Error running SQL: {sql}")
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

        credentials = connection.credentials

        try:
            con_str = []
            con_str.append(f"DRIVER={{{credentials.driver}}}")
            con_str.append(f"SERVER={credentials.host}")
            con_str.append(f"Database={credentials.database}")

            type_auth = getattr(credentials, 'authentication', 'sql')

            if 'ActiveDirectory' in type_auth:
                con_str.append(f"Authentication={credentials.authentication}")

                if type_auth == "ActiveDirectoryPassword":
                    con_str.append(f"UID={{{credentials.UID}}}")
                    con_str.append(f"PWD={{{credentials.PWD}}}")
                elif type_auth == "ActiveDirectoryInteractive":
                    con_str.append(f"UID={{{credentials.UID}}}")
                elif type_auth == "ActiveDirectoryIntegrated":
                    # why is this necessary???
                    con_str.remove("UID={None}")
                elif type_auth == "ActiveDirectoryMsi":
                    raise ValueError("ActiveDirectoryMsi is not supported yet")

            elif type_auth == 'sql':
                con_str.append("Authentication=SqlPassword")
                con_str.append(f"UID={{{credentials.UID}}}")
                con_str.append(f"PWD={{{credentials.PWD}}}")

            if not getattr(credentials, 'encrypt', False):
                con_str.append(f"Encrypt={credentials.encrypt}")

            con_str_concat = ';'.join(con_str)
            logger.debug(f'Using connection string: {con_str_concat}')
            handle = pyodbc.connect(con_str_concat, autocommit=True)

            connection.state = 'open'
            connection.handle = handle
            logger.debug(f'Connected to db: {credentials.database}')

        except pyodbc.Error as e:
            logger.debug(f"Could not connect to db: {e}")

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

        if auto_begin and connection.transaction_open is False:
            self.begin()

        logger.debug('Using {} connection "{}".'
                     .format(self.TYPE, connection.name))

        with self.exception_handler(sql):
            if abridge_sql_log:
                logger.debug('On {}:\n{}....'.format(
                    connection.name, sql[0:512]))
            else:
                logger.debug('On {}:\n{}'.format(connection.name, sql))
            pre = time.time()

            cursor = connection.handle.cursor()

            # pyodbc does not handle a None type binding!
            if bindings is None:
                cursor.execute(sql)
            else:
                cursor.execute(sql, bindings)

            logger.debug("SQL status: {} in {:0.2f} seconds".format(
                         self.get_status(cursor), (time.time() - pre)))

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
