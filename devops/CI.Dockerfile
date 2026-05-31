ARG PYTHON_VERSION="3.10"
FROM python:${PYTHON_VERSION}-bullseye as base

# Setup dependencies for pyodbc
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      apt-transport-https \
      curl  \
      gnupg2 \
      unixodbc-dev \
      lsb-release && \
    apt-get autoremove -yqq --purge && \
    apt-get clean &&  \
    rm -rf /var/lib/apt/lists/*

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# Download and dearmor Microsoft's GPG key
RUN mkdir -p /etc/apt/keyrings/ \
    && curl -s https://packages.microsoft.com/keys/microsoft.asc \
    | gpg --dearmor > /etc/apt/keyrings/packages.microsoft.com.gpg

# Download and add key to Microsoft apt source
RUN curl -s https://packages.microsoft.com/config/debian/$(lsb_release -sr 2>/dev/null)/prod.list \
    | sed -e 's#\[#[signed-by=/etc/apt/keyrings/packages.microsoft.com.gpg #' \
    | tee /etc/apt/sources.list.d/microsoft-prod.list

# enable Azure CLI package repo
RUN echo "deb [signed-by=/etc/apt/keyrings/packages.microsoft.com.gpg arch=amd64] https://packages.microsoft.com/repos/azure-cli/ $(lsb_release -cs) main" | tee /etc/apt/sources.list.d/azure-cli.list

# install Azure CLI
ENV ACCEPT_EULA=Y
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      azure-cli && \
    apt-get autoremove -yqq --purge && \
    apt-get clean &&  \
    rm -rf /var/lib/apt/lists/*

FROM base as msodbc17

# install ODBC driver 17
ENV ACCEPT_EULA=Y
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      msodbcsql17 \
      mssql-tools && \
    apt-get autoremove -yqq --purge && \
    apt-get clean &&  \
    rm -rf /var/lib/apt/lists/*

# add sqlcmd to the path
ENV PATH="$PATH:/opt/mssql-tools/bin"

FROM base as msodbc18

# install ODBC driver 18
ENV ACCEPT_EULA=Y
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      msodbcsql18 \
      mssql-tools18 && \
    apt-get autoremove -yqq --purge && \
    apt-get clean &&  \
    rm -rf /var/lib/apt/lists/*

# add sqlcmd to the path
ENV PATH="$PATH:/opt/mssql-tools18/bin"
