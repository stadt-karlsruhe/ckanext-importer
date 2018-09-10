#!/bin/bash

set -e

echo "This is travis-build.bash..."


#
# CKAN
#

echo "Installing the packages that CKAN requires..."
sudo apt-get update -qq
sudo apt-get install postgresql-$PGVERSION solr-tomcat

echo "Installing CKAN and its Python dependencies..."
git clone https://github.com/ckan/ckan
pushd ckan
if [ $CKANVERSION == 'master' ]
then
    echo "CKAN version: master"
else
    CKAN_TAG=$(git tag | grep ^ckan-$CKANVERSION | sort --version-sort | tail -n 1)
    git checkout $CKAN_TAG
    echo "CKAN version: ${CKAN_TAG#ckan-}"
fi
# Unpin CKAN's psycopg2 dependency get an important bugfix
# https://stackoverflow.com/questions/47044854/error-installing-psycopg2-2-6-2
sed -i '/psycopg2/c\psycopg2' requirements.txt
python setup.py develop
pip install -r requirements.txt
pip install -r dev-requirements.txt
popd


#
# TRAVIS-SPECIFIC
#

pip install coveralls

cp travis/test-local.ini .


#
# CKANEXT-IMPORTER
#

echo "Installing ckanext-importer and its requirements..."
python setup.py develop
pip install -r requirements.txt
pip install -r dev-requirements.txt


#
# SOLR
#

sudo cp ckan/ckan/config/solr/schema.xml /etc/solr/conf/schema.xml
# The name of the Tomcat service depends on the currently installed version
TOMCAT_SERVICE=$(sudo service --status-all 2>&1 | awk '/tomcat/ { print $4 }')
echo "Tomcat's service is $TOMCAT_SERVICE"
sudo service "$TOMCAT_SERVICE" restart


#
# POSTGRESQL
#

echo "Creating the PostgreSQL user and database..."
sudo -u postgres psql -c "CREATE USER ckan_default WITH PASSWORD 'pass';"
sudo -u postgres psql -c 'CREATE DATABASE ckan_test WITH OWNER ckan_default;'

echo "Initialising CKAN database..."
paster --plugin=ckan db init -c test.ini


#
# DOCS
#

echo "Building the docs..."
./make_docs.sh


echo "travis-build.bash is done."

