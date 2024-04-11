cp test.env.sample test.env

pyenv install 3.10.7
pyenv virtualenv 3.10.7 dbt-sqlserver
pyenv activate dbt-sqlserver

make dev
make server
