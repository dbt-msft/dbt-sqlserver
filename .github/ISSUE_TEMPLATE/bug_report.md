---
name: Bug report
about: Something in dbt-sqlserver doesn't work as documented.
labels: bug
---

<!-- What you ran, what you expected, and what happened instead. -->

## Steps to reproduce
<!-- The smallest model, config and command that show the problem. Leave out credentials. -->

## Environment
<!-- Authentication, ODBC driver and OS matter for connection problems. Collation: SELECT DATABASEPROPERTYEX(DB_NAME(), 'Collation') -->
- Database: <!-- SQL Server 2017 / 2019 / 2022 / 2025, Azure SQL Database, Azure SQL Managed Instance -->
- Backend: <!-- pyodbc (default), mssql-python, adbc -->
- Authentication, ODBC driver, OS:
- Database collation:
- Other dbt packages: <!-- e.g. tsql-utils, if the failing macro could come from one -->

`dbt --version`:
```text

```

## Log excerpt
<!-- The lines around the error from logs/dbt.log in your project; it includes the SQL dbt ran. Remove secrets. -->
```text

```
