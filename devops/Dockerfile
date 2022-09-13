ARG PYTHON_VERSION="3.10"
FROM python:${PYTHON_VERSION}-bullseye as base

# Setup dependencies for pyodbc
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      apt-transport-https \
      curl  \
      lsb-release && \
    apt-get autoremove -yqq --purge && \
    apt-get clean &&  \
    rm -rf /var/lib/apt/lists/*

# enable Microsoft package repo
RUN curl -sL -o mspkgs.deb https://packages.microsoft.com/config/debian/10/packages-microsoft-prod.deb && \
   dpkg -i mspkgs.deb && \
   rm -rf mspkgs.deb

# enable Azure CLI package repo
RUN echo "deb [arch=amd64] https://packages.microsoft.com/repos/azure-cli/ $(lsb_release -cs) main" | tee /etc/apt/sources.list.d/azure-cli.list

# install Azure CLI
ENV ACCEPT_EULA=Y
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      azure-cli \
    apt-get autoremove -yqq --purge && \
    apt-get clean &&  \
    rm -rf /var/lib/apt/lists/*

FROM base as msodbc17

# install ODBC driver 17
ENV ACCEPT_EULA=Y
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      unixodbc-dev \
      azure-cli \
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
      unixodbc-dev \
      azure-cli \
      msodbcsql18 \
      mssql-tools18 && \
    apt-get autoremove -yqq --purge && \
    apt-get clean &&  \
    rm -rf /var/lib/apt/lists/*

# add sqlcmd to the path
ENV PATH="$PATH:/opt/mssql-tools18/bin"
