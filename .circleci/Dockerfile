
FROM cimg/python:3.7.10 AS base
USER root

# Setup dependencies for pyodbc
RUN \
    apt-get update && \
    apt-get install -y unixodbc-dev unixodbc g++ apt-transport-https && \
    gpg --keyserver hkp://keys.gnupg.net:80 --recv-keys 5072E1F5

RUN \
  export ACCEPT_EULA='Y' && \
  # Install pyodbc db drivers for MSSQL
  curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
  curl https://packages.microsoft.com/config/debian/9/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
  apt-get update && \
  apt-get install -y msodbcsql17 odbc-postgresql mssql-tools

# add sqlcmd to the path
ENV PATH="$PATH:/opt/mssql-tools/bin"

# Update odbcinst.ini to make sure full path to driver is listed
RUN \
  sed 's/Driver=psql/Driver=\/usr\/lib\/x86_64-linux-gnu\/odbc\/psql/' /etc/odbcinst.ini > /tmp/temp.ini && \
  mv -f /tmp/temp.ini /etc/odbcinst.ini

RUN \
  # Cleanup build dependencies
  apt-get remove -y curl apt-transport-https debconf-utils rsync build-essential gnupg2 && \
  apt-get autoremove -y && apt-get autoclean -y