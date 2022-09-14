#!/usr/bin/env python3

import os
import time

import pyodbc


def resume_azsql():
    sql_server_name = os.getenv("DBT_AZURESQL_SERVER")
    sql_server_port = 1433
    database_name = os.getenv("DBT_AZURESQL_DB")
    username = os.getenv("DBT_AZURESQL_UID")
    password = os.getenv("DBT_AZURESQL_PWD")
    driver = f"ODBC Driver {os.getenv('MSODBC_VERSION')} for SQL Server"

    con_str = [
        f"DRIVER={{{driver}}}",
        f"SERVER={sql_server_name},{sql_server_port}",
        f"Database={database_name}",
        "Encrypt=Yes",
        f"UID={{{username}}}",
        f"PWD={{{password}}}",
    ]

    con_str_concat = ";".join(con_str)
    print("Connecting with the following connection string:")
    print(con_str_concat.replace(password, "***"))

    connected = False
    attempts = 0
    while not connected and attempts < 20:
        try:
            attempts += 1
            handle = pyodbc.connect(con_str_concat, autocommit=True)
            cursor = handle.cursor()
            cursor.execute("SELECT 1")
            connected = True
        except pyodbc.Error as e:
            print("Failed to connect to SQL Server. Retrying...")
            print(e)
            time.sleep(10)


def main():
    resume_azsql()


if __name__ == "__main__":
    main()
