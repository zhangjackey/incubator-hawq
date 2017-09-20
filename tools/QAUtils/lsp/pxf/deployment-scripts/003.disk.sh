#!/bin/bash
set -e

PWD=$( cd $( dirname ${BASH_SOURCE[0]} ) && pwd )
source $PWD/aws_config.sh

if [ ! -f $PWD/all_hosts.txt ]; then
	echo "all_hosts.txt not found!"
	exit 1
fi
if [ ! -f $PWD/disk.sh ]; then
	echo "disk.sh not found!"
	exit 1
fi

echo "############################################################"
echo "# Make sure these variables are correct before continuing. #"
echo "############################################################"
echo ""
echo "all_hosts.txt:"
cat $PWD/all_hosts.txt
echo "############################################################"
read -p "Hit enter to continue..."

get_disk_count()
{
	count="0"
	for i in $(cat $PWD/all_hosts.txt); do
		next_count=$(ssh root@$i "ps -ef | grep disk.sh | grep -v grep | wc -l")
		count=$(($count + $next_count))
	done
}

for i in $(cat all_hosts.txt); do
	echo "scp $PWD/disk.sh root@$i:/root"
	scp $PWD/disk.sh root@$i:/root
	ssh root@$i "/root/disk.sh > /root/disk.log 2>&1 < /root/disk.log &"
done

get_disk_count

echo "Now configuring disks.  This may take a while."
echo -ne "Configuring hosts"
while [ "$count" -gt "0" ]; do
	echo -ne "."
	sleep 5
	get_disk_count
done

echo "Done configuring disks."
echo ""

#print the disk configuration
for i in $(cat all_hosts.txt); do
	echo "host disk: $i"
	ssh root@$i "df -h"
	echo "host swap: $i"
	ssh root@$i "swapon -s"
	echo ""
done
