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

echo "############################################################"
echo "# Make sure these variables are correct before continuing. #"
echo "############################################################"
echo "aws_config.sh:"
echo "        AWS_USER: $AWS_USER"
echo "        KEY_FILE: $KEY_FILE"
echo ""
echo "all_hosts.txt:"
cat $PWD/all_hosts.txt
echo "############################################################"
read -p "Hit enter to continue..."

for i in $(cat all_hosts.txt); do
	echo "ssh -o StrictHostKeyChecking=no -t -i $KEY_FILE $AWS_USER@$i \"sudo sed -i 's/PasswordAuthentication no/PasswordAuthentication yes/g' /etc/ssh/sshd_config\""
	ssh -o StrictHostKeyChecking=no -t -i $KEY_FILE $AWS_USER@$i "sudo sed -i 's/PasswordAuthentication no/PasswordAuthentication yes/g' /etc/ssh/sshd_config"

	echo "ssh -t -i $KEY_FILE $AWS_USER@$i \"sudo /etc/init.d/sshd restart; sleep 5\""
	ssh -t -i $KEY_FILE $AWS_USER@$i "sudo /etc/init.d/sshd restart; sleep 5"

	echo "ssh -t -i $KEY_FILE $AWS_USER@$i \"sudo ssh-keygen -t rsa -N '' -f /root/.ssh/id_rsa\""
	ssh -t -i $KEY_FILE $AWS_USER@$i "sudo ssh-keygen -t rsa -N '' -f /root/.ssh/id_rsa"

	echo "ssh -t -i $KEY_FILE $AWS_USER@$i \"sudo cp /root/.ssh/id_rsa.pub /home/$AWS_USER/$i.pub\""
	ssh -t -i $KEY_FILE $AWS_USER@$i "sudo cp /root/.ssh/id_rsa.pub /home/$AWS_USER/$i.pub"

	echo "scp -i $KEY_FILE $AWS_USER@$i:/home/$AWS_USER/$i.pub ."
	scp -i $KEY_FILE $AWS_USER@$i:/home/$AWS_USER/$i.pub .
done

echo "make authorized_keys file"
cat ~/.ssh/id_rsa.pub > authorized_keys
for i in $(cat all_hosts.txt); do
	echo "cat $PWD/$i.pub >> authorized_keys"
	cat $PWD/$i.pub >> authorized_keys
done

rm -f $PWD/*.pub

for i in $(cat all_hosts.txt); do
	echo "scp -i $KEY_FILE authorized_keys $AWS_USER@$i:/home/$AWS_USER/"
	scp -i $KEY_FILE authorized_keys $AWS_USER@$i:/home/$AWS_USER/

	echo "ssh -t -i $KEY_FILE $AWS_USER@$i \"sudo cp /home/$AWS_USER/authorized_keys /root/.ssh/\""
	ssh -t -i $KEY_FILE $AWS_USER@$i "sudo cp /home/$AWS_USER/authorized_keys /root/.ssh/"
done

rm -f $PWD/authorized_keys
