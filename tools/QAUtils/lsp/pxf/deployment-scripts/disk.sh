#!/bin/bash
set -e
PWD=$( cd $( dirname ${BASH_SOURCE[0]} ) && pwd )

d="0"
# remove unneccessary mount
sed -i '/mnt/d' /etc/fstab

counter="0"
data_disks=$(fdisk -l | grep Disk | grep -v identifier | awk -F ' ' '{print $2}' | awk -F ':' '{print $1}' | wc -l)
#subtract the root and swap partition
data_disks=$((data_disks - 2))

for i in $(fdisk -l | grep Disk | grep -v identifier | awk -F ' ' '{print $2}' | awk -F ':' '{print $1}' | sort); do

	# first disk is the root disk so skip it
	if [ "$counter" -eq "0" ]; then
		echo "skipping root partition"
	else 
		if [ "$counter" -le "$data_disks" ]; then
			mount_counter=$(df | grep $i | wc -l)
			if [ "$mount_counter" -eq "0" ]; then
				echo "mkfs -t ext4 $i"
				mkfs -t ext4 $i
				echo "mkdir /data$counter"
				mkdir /data$counter
				echo "mount $i /data$counter"
				mount $i /data$counter
				echo "echo '$i        /data$counter                  ext4    defaults,nofail 0 2' >> /etc/fstab"
				echo "$i               /data$counter	          ext4    defaults,nofail 0 2" >> /etc/fstab
			else
				echo "$i already mounted"
			fi
		else
			# exceeded the number of data disks so make the rest of the disks swap
			swap_counter=$(swapon -s | grep $i | wc -l)
			if [ "$swap_counter" -eq "0" ]; then
				echo "mkswap $i"
				mkswap $i
				echo "swapon $i"
				swapon $i
				echo "echo '$i               swap                    swap    defaults        0 0' >> /etc/fstab"
				echo "$i               swap                    swap    defaults        0 0" >> /etc/fstab
				echo "swapon -s"
				swapon -s
				echo "df -h"
				df -h
			else
				echo "$i alread mounted as swap"
			fi
		fi
		
	fi
	counter=$((counter + 1))
done
