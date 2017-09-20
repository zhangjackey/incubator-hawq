#!/bin/bash
set -e

PWD=$( cd $( dirname ${BASH_SOURCE[0]} ) && pwd )

if [ ! -f $PWD/ambari_host.txt ]; then
	echo "ambari_host.txt not found!"
	exit 1
fi

if [ ! -f $PWD/ambari_install.sh ]; then
	echo "ambari_install.sh not found!"
	exit 1
fi

if [ -z "${HDB_ARTIFACTS}"]; then
	HDB_ARTIFACTS=~/workspace/artifacts/HDB
fi


echo "############################################################"
echo "# Make sure these variables are correct before continuing. #"
echo "############################################################"
echo "ambari_host.txt:"
cat $PWD/ambari_host.txt
echo "############################################################"
read -p "Hit enter to continue..."

for i in $(cat $PWD/ambari_host.txt); do
	ssh root@$i "mkdir /staging"
	ssh root@$i "chmod 755 /staging"
	scp "${HDB_ARTIFACTS}"/hdb* root@$i:/staging/
	scp ambari_install.sh root@$i:/root/
	ssh root@$i "cd /root; ./ambari_install.sh"
	ssh root@$i "tar -xvzf /staging/hdb-2.0.1.0-*.tar.gz -C /staging/"
	ssh root@$i "tar -xvzf /staging/hdb-ambari-plugin*.tar.gz -C /staging/"
	ssh root@$i "cd /staging/hdb-2*; ./setup_repo.sh"
	ssh root@$i "cd /staging/hdb-ambari-plugin*; ./setup_repo.sh"
	ssh root@$i "yum install hdb-ambari-plugin -y"
	ssh root@$i "ambari-server restart"
	echo "Make sure port 8080 is open on the Ambari host $i"
	echo "Private key needed for Ambari installation:"
	ssh root@$i "cat /root/.ssh/id_rsa"
done

