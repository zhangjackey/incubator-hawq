#!/bin/sh

source ~/.bashrc
source ~/qa.sh

python -u lsp.py -s performance_orange_10g > ./performance_orange_10g.log 2>&1