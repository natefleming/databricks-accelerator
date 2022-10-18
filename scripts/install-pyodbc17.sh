#!/bin/bash

curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
curl https://packages.microsoft.com/config/ubuntu/16.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
sudo apt-get update
sudo ACCEPT_EULA=Y apt-get -q -y install msodbcsql17

sudo apt-get update
sudo apt-get -q -y install unixodbc unixodbc-dev
sudo apt-get -q -y install python3-dev
python3 -m pip install --upgrade pip

