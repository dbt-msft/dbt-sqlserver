
FROM python:3.7-slim AS base

ADD requirements.txt ./

# Setup dependencies for pyodbc
RUN \
    apt-get update && \
    apt-get install -y curl build-essential unixodbc-dev g++ apt-transport-https && \
    gpg --keyserver hkp://keys.gnupg.net --recv-keys 5072E1F5

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
# Install pip
RUN \
  pip install --upgrade pip && \
  pip install -r requirements.txt && \
  rm requirements.txt
# permission management
RUN \
  chmod +rwx /etc/ssl/openssl.cnf && \
  # change TLS back to version 1
  sed -i 's/TLSv1.2/TLSv1/g' /etc/ssl/openssl.cnf && \
  # allow weak certificates (certificate signed with SHA1)
  # by downgrading OpenSSL security level from 2 to 1
  sed -i 's/SECLEVEL=2/SECLEVEL=1/g' /etc/ssl/openssl.cnf 

RUN \
  # Cleanup build dependencies
  apt-get remove -y curl apt-transport-https debconf-utils g++ gcc rsync build-essential gnupg2 && \
  apt-get autoremove -y && apt-get autoclean -y