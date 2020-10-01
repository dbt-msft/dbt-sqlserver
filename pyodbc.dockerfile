FROM python:3.7-slim AS base

# Setup dependencies for pyodbc
RUN \
  export ACCEPT_EULA='Y' && \
  apt-get update && \
  apt-get install -y curl build-essential unixodbc-dev g++ apt-transport-https && \
  gpg --keyserver hkp://keys.gnupg.net --recv-keys 5072E1F5 && \
  #
  # Install pyodbc db drivers for MSSQL
  curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
  curl https://packages.microsoft.com/config/debian/9/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
  apt-get update && \
  apt-get install -y msodbcsql17 odbc-postgresql && \
  #
  # Update odbcinst.ini to make sure full path to driver is listed
  sed 's/Driver=psql/Driver=\/usr\/lib\/x86_64-linux-gnu\/odbc\/psql/' /etc/odbcinst.ini > /tmp/temp.ini && \
  mv -f /tmp/temp.ini /etc/odbcinst.ini && \
  # Install pip
  pip install --upgrade pip

RUN \
  #
  # allow weak certificates (certificate signed with SHA1)
  # by downgrading OpenSSL security level from 2 to 1
  sed -i 's/SECLEVEL=2/SECLEVEL=1/g' /etc/ssl/openssl.cnf

RUN \
  # Cleanup build dependencies
  apt-get remove -y curl apt-transport-https debconf-utils g++ gcc rsync build-essential gnupg2 && \
  apt-get autoremove -y && apt-get autoclean -y
