FROM ubuntu:18.04

RUN apt update -y  &&  apt upgrade -y && apt-get update && apt-get upgrade -y
RUN apt-get install -y git 
RUN apt install -y curl python3.7 git python3-pip openjdk-8-jdk unixodbc-dev
RUN pip3 install --upgrade pip
# Add SQL Server ODBC Driver 17 for Ubuntu 18.04
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/ubuntu/18.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
RUN apt-get update

RUN ACCEPT_EULA=Y apt-get install -y --allow-unauthenticated msodbcsql17
RUN ACCEPT_EULA=Y apt-get install -y --allow-unauthenticated mssql-tools
RUN echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bash_profile
RUN echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc

RUN chmod +rwx /etc/ssl/openssl.cnf
RUN sed -i 's/TLSv1.2/TLSv1/g' /etc/ssl/openssl.cnf
# allow weak certificates (certificate signed with SHA1)
# by downgrading OpenSSL security level from 2 to 1
RUN sed -i 's/SECLEVEL=2/SECLEVEL=1/g' /etc/ssl/openssl.cnf 

ENTRYPOINT ["sh"]