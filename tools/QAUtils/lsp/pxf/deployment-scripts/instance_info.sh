#! /bin/bash

if [ ! -f $USER]; then
	echo "$USER not found"
	echo "Make sure this is set"
	exit 1
fi
aws ec2 describe-instances --output text --query 'Reservations[*].Instances[*].[Tags[?Key==`Name`].Value | [0], PrivateDnsName, PublicIpAddress]' --filter "Name=tag:Owner,Values="$USER "Name=instance-state-name,Values=running" "Name=key-name,Values=hawq-ud"
