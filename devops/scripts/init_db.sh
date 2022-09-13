#!/usr/bin/env bash

for i in {1..50};
do
    /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P "${SA_PASSWORD}" -d msdb -I -i create_sql_users.sql
    if [ $? -eq 0 ]
    then
        echo "create_sql_users.sql completed"
        break
    else
        echo "not ready yet..."
        sleep 1
    fi
done
