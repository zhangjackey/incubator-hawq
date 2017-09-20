#!/bin/sh

source ~/.bashrc
source ~/qa.sh

ulimit -a

#Get default baseline values from pulse, else using default values. 
baseline_options=""
if [ -n "${BASELINE_HDFS_VERSION}" ] && [ -n "${BASELINE_HAWQ_VERSION}" ] ; then
    baseline_options="--baseline-hawq-version ${BASELINE_HAWQ_VERSION} --baseline-hdfs-version ${BASELINE_HDFS_VERSION}"
fi 

python -u lsp.py ${baseline_options} -s performance_hawq -a  > ./performance_hawq.log 2>&1
