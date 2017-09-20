#!/bin/bash
set -e
PWD=$( cd $( dirname ${BASH_SOURCE[0]} ) && pwd )
AWS_USER="centos"
KEY_FILE=~/.aws/hawq-ud.pem
