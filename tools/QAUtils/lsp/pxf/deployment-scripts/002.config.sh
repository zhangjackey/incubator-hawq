#!/bin/bash
set -e

PWD=$( cd $( dirname ${BASH_SOURCE[0]} ) && pwd )

if [ ! -f $PWD/all_hosts.txt ]; then
	echo "all_hosts.txt not found!"
	exit 1
fi

echo "############################################################"
echo "# Make sure these variables are correct before continuing. #"
echo "############################################################"
echo "Hosts:"
cat $PWD/all_hosts.txt
echo "############################################################"
read -p "Hit enter to continue..."

get_config_count()
{
	count="0"
	for i in $(cat $PWD/all_hosts.txt); do
		next_count=$(ssh root@$i "ps -ef | grep config.sh | grep -v grep | wc -l")
		count=$(($count + $next_count))
	done
}

rm -f *.log

for i in $(cat all_hosts.txt); do
	echo "scp $PWD/config.sh root@$i:/root"
	scp $PWD/config.sh root@$i:/root
	echo "ssh root@$i \"/root/config.sh $i > /root/config.log 2>&1 < /root/config.log &\""
	ssh root@$i "/root/config.sh $i > /root/config.log 2>&1 < /root/config.log &"
done

get_config_count

echo "Now configuring hosts.  This may take a while."
echo -ne "Configuring hosts"
while [ "$count" -gt "0" ]; do
	echo -ne "."
	sleep 5
	get_config_count
done

echo "Done configuring hosts."
echo ""
