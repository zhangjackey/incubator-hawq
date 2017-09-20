#!/bin/sh

source ~/.bashrc
source ~/qa.sh



python -u lsp.py -s performance_tpch_100g_pxf -a  > ./performance_tpch_100g.log 2>&1
