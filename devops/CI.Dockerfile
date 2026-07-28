ARG PYTHON_VERSION="3.11"
FROM python:${PYTHON_VERSION}-bookworm AS base

# Shared CI tooling used by both backends.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      apt-transport-https \
      curl  \
      gnupg2 \
      lsb-release && \
    apt-get autoremove -yqq --purge && \
    apt-get clean &&  \
    rm -rf /var/lib/apt/lists/*

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# Download and dearmor Microsoft's GPG key
RUN curl -sL https://packages.microsoft.com/keys/microsoft.asc \
    | gpg --dearmor \
    | tee /usr/share/keyrings/microsoft-prod.gpg >/dev/null

# Enable Microsoft package repo with signed-by key
RUN curl -sL https://packages.microsoft.com/config/debian/$(lsb_release -sr 2>/dev/null)/prod.list \
    | sed -e 's#deb \[arch=#deb [signed-by=/usr/share/keyrings/microsoft-prod.gpg arch=#' \
    | tee /etc/apt/sources.list.d/mssql-release.list

# Enable Azure CLI package repo
RUN echo "deb [signed-by=/usr/share/keyrings/microsoft-prod.gpg arch=amd64] https://packages.microsoft.com/repos/azure-cli/ $(lsb_release -cs) main" \
    | tee /etc/apt/sources.list.d/azure-cli.list

# install Azure CLI
ENV ACCEPT_EULA=Y
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      azure-cli && \
    apt-get autoremove -yqq --purge && \
    apt-get clean &&  \
    rm -rf /var/lib/apt/lists/*

FROM base AS mssql

# System libraries required by the mssql-python backend.
ENV ACCEPT_EULA=Y
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      libgssapi-krb5-2 \
      libkrb5-3 \
      libltdl7 && \
    apt-get autoremove -yqq --purge && \
    apt-get clean &&  \
    rm -rf /var/lib/apt/lists/*

FROM base AS adbc

# `dbc` is the driver-manager CLI for ADBC; it fetches the go-mssqldb-based
# `adbc-driver-mssql` binary (not on PyPI) that the adbc-driver-manager
# Python package talks to. See docs/adbc_backend.md.
ENV ACCEPT_EULA=Y
RUN pip install --no-cache-dir dbc && \
    dbc install mssql

ENV ADBC_DRIVER_PATH="/root/.config/adbc/drivers"

FROM base AS msodbc17

# install ODBC driver 17
ENV ACCEPT_EULA=Y
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      unixodbc-dev \
      msodbcsql17 \
      mssql-tools && \
    apt-get autoremove -yqq --purge && \
    apt-get clean &&  \
    rm -rf /var/lib/apt/lists/*

# add sqlcmd to the path
ENV PATH="$PATH:/opt/mssql-tools/bin"

FROM base AS msodbc18

# install ODBC driver 18
ENV ACCEPT_EULA=Y
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      unixodbc-dev \
      msodbcsql18 \
      mssql-tools18 && \
    apt-get autoremove -yqq --purge && \
    apt-get clean &&  \
    rm -rf /var/lib/apt/lists/*

# add sqlcmd to the path
ENV PATH="$PATH:/opt/mssql-tools18/bin"
