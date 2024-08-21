ARG SQLServer_VERSION="2022"
FROM mcr.microsoft.com/mssql/server:${SQLServer_VERSION}-latest

ENV COLLATION="SQL_Latin1_General_CP1_CI_AS"

USER root

RUN mkdir -p /opt/init_scripts
WORKDIR /opt/init_scripts
COPY scripts/* /opt/init_scripts/

RUN chmod +x /opt/init_scripts/*.sh

ENTRYPOINT /bin/bash ./entrypoint.sh
