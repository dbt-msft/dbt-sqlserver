#!/usr/bin/env python3

import os

import pyodbc


def resume_azsql():
    sql_server_name = os.getenv("DBT_AZURESQL_SERVER")
    sql_server_port = 1433
    database_name = os.getenv("DBT_AZURESQL_DB")
    username = os.getenv("DBT_AZURESQL_UID")
    password = os.getenv("DBT_AZURESQL_PASSWORD")
    driver = "ODBC Driver 17 for SQL Server"

    con_str = [
        f"DRIVER={{{driver}}}",
        f"SERVER={sql_server_name},{sql_server_port}",
        f"Database={database_name}",
        "Encrypt=Yes",
        f"UID={{{username}}}",
        f"PWD={{{password}}}",
    ]

    con_str_concat = ";".join(con_str)
    pyodbc.connect(con_str_concat, autocommit=True)


def main():
    resume_azsql()


if __name__ == "__main__":
    main()
