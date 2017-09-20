#!/bin/bash
set -e

if [ ! -f $PWD/ambari_host.txt ]; then
        echo "ambari_host.txt not found!"
        exit 1
fi

echo "############################################################"
echo "# Configure HDP and HDB with Ambari.                       #"
echo "############################################################"
h=$(cat $PWD/ambari_host.txt)
echo "http://$h:8080"
