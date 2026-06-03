package_installed() {
  dpkg-query -W -f='${db:Status-Abbrev}' "$1" 2>/dev/null | grep -q '^ii '
}

if ! package_installed msodbcsql18; then
  curl https://packages.microsoft.com/keys/microsoft.asc \
    | gpg --dearmor \
    | sudo tee /usr/share/keyrings/microsoft-prod.gpg >/dev/null

  # Download appropriate package for the OS version.
  # Choose only ONE of the following, corresponding to your OS version.

  # Debian 12
  curl https://packages.microsoft.com/config/debian/12/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list

  sudo apt-get update
  sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18
fi

# optional: for bcp and sqlcmd
if ! package_installed mssql-tools18; then
  sudo ACCEPT_EULA=Y apt-get install -y mssql-tools18
fi

if ! grep -qF '/opt/mssql-tools18/bin' "$HOME/.bashrc"; then
  echo 'export PATH="$PATH:/opt/mssql-tools18/bin"' >> ~/.bashrc
fi
source ~/.bashrc

# optional: for unixODBC development headers
if ! package_installed unixodbc-dev; then
  sudo apt-get install -y unixodbc-dev
fi

# optional: kerberos library for debian-slim distributions
if ! package_installed libgssapi-krb5-2; then
  sudo apt-get install -y libgssapi-krb5-2
fi
