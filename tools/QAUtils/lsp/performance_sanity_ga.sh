#!/bin/sh

source ~/.bashrc
source ~/qa.sh

# Workaround until we update AMI with ulimited parameter
localhdfs stop
sleep 3
localhdfs start
sleep 3
hawq stop cluster -a
sleep 3
hawq start cluster
sleep 3

ulimit -a


python -u lsp.py -s performance_tpch_10g_ga -a  > ./performance_tpch_10g_ga.log 2>&1
sleep 10
python -u lsp.py -s performance_tpch_100g_ga -a  > ./performance_tpch_100g_ga.log 2>&1
#sleep 10
#python -u lsp.py -s performance_tpch_concurrent > ./performance_tpch_concurrent.log 2>&1
#sleep 10
#4. 2. 1 ratio concurrent to run tpch 10G
#python -u lsp.py -s resourcequene_tpch_ratio_10g -m 30 -a -c   > ./resourcequene_tpch_ratio_10g 2>&1

#python -u lsp.py -s tpcds_sanity -m 10 -a -c -r 5 > ./tpcds 2>&1
#sleep 10
