#!/bin/bash

set -e
PWD=$( cd $( dirname ${BASH_SOURCE[0]} ) && pwd )
source $PWD/aws_config.sh

if [ ! -f $PWD/all_hosts.txt ]; then
	echo "all_hosts.txt not found!"
	exit 1
fi

if [ ! -f $KEY_FILE ]; then
	echo "$KEY_FILE not found!"
	exit 1
fi

for i in $(cat all_hosts.txt); do
	echo "scp $PWD/jce_policy-8.zip root@$i:/home/centos"
	scp -i $KEY_FILE $PWD/jce_policy-8.zip $AWS_USER@$i:/home/centos

	echo "ssh -t -i $KEY_FILE $AWS_USER@$i \"sudo unzip -o -j -q /home/centos/jce_policy-8.zip -d /usr/jdk64/jdk1.8.0_60/jre/lib/security/\""
	ssh -t -i $KEY_FILE $AWS_USER@$i "sudo unzip -o -j -q /home/centos/jce_policy-8.zip -d /usr/jdk64/jdk1.8.0_60/jre/lib/security/"
done
