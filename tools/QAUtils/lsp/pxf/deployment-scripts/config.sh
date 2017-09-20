#!/bin/bash
echo "config begin"
chkconfig iptables off
service iptables stop
echo 0 >/selinux/enforce
sed -i 's/SELINUX=enforcing/SELINUX=disabled/g' /etc/selinux/config
setenforce 0
sed -i '/\t\bkernel\b/ s/$/ transparent_hugepage=never/' /boot/grub/grub.conf
echo never > /sys/kernel/mm/transparent_hugepage/enabled
yum update openssl -y
yum install ntp -y
/etc/init.d/ntpd start
yum install httpd -y
/etc/init.d/httpd start
yum install wget -y
ln -s /etc/init.d/httpd /etc/rc3.d/S15httpd
ln -s /etc/init.d/ntpd /etc/rc3.d/S74ntpd
echo "config end"
