#!/bin/sh

source ~/.bashrc
source ~/qa.sh

python -u lsp.py -s performance_quick_test -a  > ./performance_quick_test.log 2>&1
