#!/bin/bash
set -e

wget -nv http://public-repo-1.hortonworks.com/ambari/centos6/2.x/updates/2.2.2.0/ambari.repo -O /etc/yum.repos.d/ambari.repo
yum install ambari-server -y
ambari-server setup -s

echo "Re-configure Ambari to use port 10432"
/etc/init.d/postgresql stop
sed -i 's/PGPORT=5432/PGPORT=10432/g' /etc/init.d/postgresql
su -c "echo \"PGPORT=10432\" >> ~postgres/.bash_profile" postgres
su -c "echo \"export PGPORT\" >> ~postgres/.bash_profile" postgres
su -c "echo \"PGDATABASE=ambari\" >> ~postgres/.bash_profile" postgres
su -c "echo \"export PGDATABASE\" >> ~postgres/.bash_profile" postgres
/etc/init.d/postgresql start
ambari-server setup --database=postgres --databasehost=localhost --databaseport=10432 --databasename=ambari --databaseusername=ambari --databasepassword=bigdata -s
ln -s /etc/init.d/postgresql /etc/rc3.d/S36postgresql

ambari-server start
