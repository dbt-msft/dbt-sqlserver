ARG MSSQL_VERSION="2022"
FROM mcr.microsoft.com/mssql/server:${MSSQL_VERSION}-latest

USER root
RUN mkdir -p /opt/init_scripts
WORKDIR /opt/init_scripts
COPY scripts/* /opt/init_scripts/

USER mssql
ENTRYPOINT /bin/bash ./entrypoint.sh
