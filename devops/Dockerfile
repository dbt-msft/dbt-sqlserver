ARG PYTHON_VERSION="3.10"
FROM python:${PYTHON_VERSION}-bullseye

# Setup dependencies for pyodbc
RUN apt-get update && \
    apt-get install -y unixodbc-dev unixodbc apt-transport-https curl lsb-release && \
    rm -rf /var/lib/apt/lists/*

# enable Microsoft package repo
RUN curl -sL -o mspkgs.deb https://packages.microsoft.com/config/debian/10/packages-microsoft-prod.deb && \
   dpkg -i mspkgs.deb && \
   rm -rf mspkgs.deb

# enable Azure CLI package repo
RUN echo "deb [arch=amd64] https://packages.microsoft.com/repos/azure-cli/ $(lsb_release -cs) main" | tee /etc/apt/sources.list.d/azure-cli.list

# install Microsoft packages
ENV ACCEPT_EULA=Y
RUN apt-get update && \
   apt-get install -y --no-install-recommends \
   azure-cli \
   msodbcsql17 \
   mssql-tools && \
   rm -rf /var/lib/apt/lists/*

# add sqlcmd to the path
ENV PATH="$PATH:/opt/mssql-tools/bin"
