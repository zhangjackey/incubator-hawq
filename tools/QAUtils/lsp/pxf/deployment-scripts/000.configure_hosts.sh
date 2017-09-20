#!/bin/bash

./get_instance_ips.sh > all_hosts.txt
./instance_info.sh | awk '/Ambari-Master/{print $NF}' > ./ambari_host.txt
./instance_info.sh | awk '/Namenode/{print $NF}' > ./hawq_master.txt
