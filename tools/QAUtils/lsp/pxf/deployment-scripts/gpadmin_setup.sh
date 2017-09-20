#!/bin/bash
set -e

echo "source /usr/local/hawq/greenplum_path.sh" >> ~/.bashrc
source /usr/local/hawq/greenplum_path.sh
psql -c "create database gpadmin" template1
psql -c "alter user gpadmin password 'changeme'"
echo "host all all 0.0.0.0/0 md5" >> /data/hawq/master/pg_hba.conf 
hawq stop cluster -u -a
