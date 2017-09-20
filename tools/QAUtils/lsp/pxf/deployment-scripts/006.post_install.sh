#!/bin/bash
set -e
PWD=$( cd $( dirname ${BASH_SOURCE[0]} ) && pwd )

if [ ! -f $PWD/hawq_master.txt ]; then
	echo "hawq_master.txt not found!"
	exit 1
fi

if [ ! -f $PWD/gpadmin_setup.sh ]; then
	echo "gpadmin_setup.sh not found!"
	exit 1
fi

echo "############################################################"
echo "# Make sure these variables are correct before continuing. #"
echo "############################################################"
echo "hawq_master.txt:"
cat $PWD/hawq_master.txt
echo "############################################################"
read -p "Hit enter to continue..."

for i in $(cat $PWD/hawq_master.txt); do
	scp $PWD/gpadmin_setup.sh root@$i:/home/gpadmin/
	ssh root@$i "chown gpadmin:gpadmin /home/gpadmin/gpadmin_setup.sh"
	ssh root@$i "su -c '/home/gpadmin/gpadmin_setup.sh'" gpadmin
done

