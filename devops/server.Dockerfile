ARG MSSQL_VERSION="2022"
FROM mcr.microsoft.com/mssql/server:${MSSQL_VERSION}-latest

USER root

ENV COLLATION="SQL_Latin1_General_CP1_CI_AS"

RUN apt update && apt install -y unixodbc

RUN mkdir -p /opt/init_scripts
WORKDIR /opt/init_scripts
COPY scripts/* /opt/init_scripts/

USER mssql

ENTRYPOINT /bin/bash ./entrypoint.sh
