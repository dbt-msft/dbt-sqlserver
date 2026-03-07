ARG PYTHON_VERSION="3.10"
FROM python:${PYTHON_VERSION}-bullseye as base

# Setup dependencies for mssql-python
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      apt-transport-https \
      curl  \
      gnupg2 \
      lsb-release && \
    apt-get autoremove -yqq --purge && \
    apt-get clean &&  \
    rm -rf /var/lib/apt/lists/*

# enable Microsoft package repo
RUN curl -sL https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl -sL https://packages.microsoft.com/config/debian/$(lsb_release -sr)/prod.list | tee /etc/apt/sources.list.d/msprod.list
# enable Azure CLI package repo
RUN echo "deb [arch=amd64] https://packages.microsoft.com/repos/azure-cli/ $(lsb_release -cs) main" | tee /etc/apt/sources.list.d/azure-cli.list

# install Azure CLI
ENV ACCEPT_EULA=Y
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      azure-cli && \
    apt-get autoremove -yqq --purge && \
    apt-get clean &&  \
    rm -rf /var/lib/apt/lists/*

FROM base as mssql

# install sqlcmd for testing
ENV ACCEPT_EULA=Y
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      mssql-tools18 && \
    apt-get autoremove -yqq --purge && \
    apt-get clean &&  \
    rm -rf /var/lib/apt/lists/*

# add sqlcmd to the path
ENV PATH="$PATH:/opt/mssql-tools18/bin"
