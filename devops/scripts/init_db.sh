#!/usr/bin/env bash

if [ -d "/opt/mssql-tools18" ]; then
  cp -r /opt/mssql-tools18 /opt/mssql-tools
fi

for i in {1..50};
do
    /opt/mssql-tools/bin/sqlcmd -C -S localhost -U sa -P "${SA_PASSWORD}" -d master -I -Q "CREATE DATABASE TestDB COLLATE ${COLLATION}"
    if [ $? -eq 0 ]
    then
        echo "database creation completed"
        break
    else
        echo "creating database..."
        sleep 1
    fi
done

for i in {1..50};
do
    /opt/mssql-tools/bin/sqlcmd -C -S localhost -U sa -P "${SA_PASSWORD}" -d TestDB -I -i init.sql
    if [ $? -eq 0 ]
    then
        echo "user creation completed"
        break
    else
        echo "configuring users..."
        sleep 1
    fi
done
