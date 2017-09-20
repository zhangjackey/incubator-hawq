# Vagrant AWS Provisioning

NOTE: Make sure you have your aws credentials exported to your shell before running any vagrant commands

## Creating the default 8-node cluster

1. Install the Vagrant AWS plugin
2. From this directory invoke `vagrant up`

## Destroying the 8-node cluster

1. From this directory `vagrant destroy`

## Alternative Cluster Formations

To create a different configuration of clusters just create another .json file and change the line `aws_config = (JSON.parse(File.read("9-node.json")))` to refer to your new file
