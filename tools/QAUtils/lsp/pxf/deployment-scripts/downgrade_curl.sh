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
	ssh -t -i $KEY_FILE $AWS_USER@$i "sudo yum downgrade curl && sudo yum -y install curl-devel"
done

