#!/bin/bash


usage()
{
  echo "Usage"
  echo "  setup_simple_kdc.sh HOST"
  echo "  Where KDC will be installed and setup"
  echo "Precondition"
  echo "  Make sure you setup passwordless ssh with HOST"
  echo "Example"
  echo "  setup_simple_kdc.sh ip-10-32-36-122.ore1.vpc.pivotal.io"
}


if [ -z "$1" ]; then
    usage
	exit
fi

set -e -x -u


export TARGET=$1

echo $TARGET

# install kerberos
ssh root@${TARGET} sudo yum install krb5-server krb5-workstation -y


# Modify krb5.conf
ssh root@${TARGET} 'sudo sed -i "s/default_realm = EXAMPLE.COM/default_realm = PIVOTAL.IO/" /etc/krb5.conf'
ssh root@${TARGET} 'sudo sed -i "s/EXAMPLE.COM = {/PIVOTAL.IO = {/" /etc/krb5.conf'
ssh root@${TARGET} 'sudo sed -i "s/kdc = kerberos.example.com/kdc = $(hostname -f)/" /etc/krb5.conf'
ssh root@${TARGET} 'sudo sed -i "s/admin_server = kerberos.example.com/admin_server = $(hostname -f)/" /etc/krb5.conf'
ssh root@${TARGET} 'sudo sed -i "/\.example.com = EXAMPLE.COM/d" /etc/krb5.conf'
ssh root@${TARGET} 'sudo sed -i "s/example.com = EXAMPLE.COM/$(hostname -f) = PIVOTAL.IO/" /etc/krb5.conf'


ssh root@${TARGET} 'sudo kdb5_util create -s -P admin'

# Modify kadm5.acl
ssh root@${TARGET} 'sudo sed -i "s/EXAMPLE.COM/PIVOTAL.IO/" /var/kerberos/krb5kdc/kadm5.acl'

# Start services
ssh root@${TARGET} sudo service kadmin start
ssh root@${TARGET} sudo service krb5kdc start

# firewall (don't know why this is required)
ssh root@${TARGET} sudo iptables -I INPUT -p udp --dport 88 -j ACCEPT
ssh root@${TARGET} sudo iptables -I INPUT -p tcp --dport 749 -j ACCEPT
ssh root@${TARGET} sudo iptables -I INPUT -p udp --dport 464 -j ACCEPT
ssh root@${TARGET} sudo service iptables save

# Set keyworkd for the principal. it is set to 'admin' and is required on UI during the wizard
ssh root@${TARGET} 'sudo kadmin.local -q "change_password -pw admin kadmin/admin@PIVOTAL.IO"'

# Don't know why this is required.
ssh root@${TARGET} sudo chmod og+w /var/log/kadmind.log

echo "KDC and Kadmin host: ${TARGET}"
echo "Realm name: PIVOTAL.IO"
echo "Principal: kadmin/admin@PIVOTAL.IO"
echo "Password: admin"


