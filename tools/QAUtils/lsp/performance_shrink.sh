#!/bin/sh

source ~/.bashrc
source ~/qa.sh

hawq stop cluster -a -M immediate
hawq config -c hawq_rm_rejectrequest_nseg_limit -v 8
hawq config -c hawq_rm_segment_heartbeat_timeout -v 120
hawq start cluster -a
localhdfs stop HA
localhdfs start HA

source ~/.bashrc
source ~/qa.sh

export HADOOP_PATH_VAR=/usr/phd/current/
function nodeconfig () 
{
   if [ $1 != 'stop' ] && [ $1 != 'start' ] ; then
       echo "Please use options: start/stop."   
       exit 1
   fi
   hostlist=$3
   option=$1
   echo "hostlist is " $hostlist
   echo "option is " $option
   if [ "$2" = "HDFS" ]  || [ "$2" = "BOTH" ]; then
       for datanode in $hostlist; do
           echo "datanode is " $datanode
           ssh $datanode sudo $HADOOP_PATH_VAR/hadoop-client/sbin/hadoop-daemon.sh $option datanode
       done
   fi

   if [ "$2" = "HAWQ" ] || [ "$2" = "BOTH" ]; then
       for hawqnode in $hostlist; do
           ssh $hawqnode "source $GPHOME/greenplum_path.sh;hawq $option segment -a "
       done
   fi

   if [ "$2" = "NAMENODE" ]; then
       for namenode in $hostlist; do
           ssh $namenode sudo -u hdfs $HADOOP_PATH_VAR/hadoop-client/sbin/hadoop-daemon.sh  $option  namenode
       done
   fi
}

SUCCESS="Under replicated blocks: 0"
SIGN="CONTINUE"

### $1 log file to check, $2 log file to log
function check_success() {
    if grep -Fq "$SUCCESS" $1
    then
        echo "HDFS have completed copying missing blocks successful :  " >> $2 2>&1
        SIGN="SUCCESS"
    else
        echo "HDFS haven\`t completed copying missing blocks yet." >> $2 2>&1
        SIGN="CONTINUE"
        sleep 600
    fi
}

#### $1 log file to log.
function check_success_or_timeout(){
    TOTAL_CHECK=144
    COUNT=144
    while [ $SIGN = "CONTINUE" ]
    do
        /usr/phd/current/hadoop-client/bin/hdfs dfsadmin -report -live > HDFS_tmp_report.log 2>&1
        check_success ./HDFS_tmp_report.log $1
        COUNT=`expr $COUNT - 1`
        date >> $1  2>&1
        echo "This is the `expr $TOTAL_CHECK - $COUNT` times check." >> $1 2>&1
        if [ $COUNT = 0 ]
        then
            SIGN="TIMEOUT"
        fi
    done
}

################ 
# $1: log file to log.
# $2: schedule name
function check_replica(){
    check_success_or_timeout $1
    if [ $SIGN = "SUCCESS" ]
    then
        date >> $1 2>&1
        echo "HDFS recover 3 replica finished." >> $1 2>&1
        ### run workload
        echo "Start running workload..." >> $1 2>&1
        date                             >> $1 2>&1
        python -u lsp.py -s $2           >> $1 2>&1
        date                             >> $1 2>&1
        ### run workload finished
        echo "Ending run workloads"      >> $1 2>&1
    else
        if [ $SIGN = "TIMEOUT" ]
        then
            date >> $1 2>&1
            echo "There are always missing blocks. Cannot finish the recover work." >> $1 2>&1
            echo "Exit due to timeout." >> $1 2>&1
        else
            date >> $1 2>&1
            echo "There are unknown error." >> $1 2>&1
        fi
    fi
}
#################
# $1: Log File name
# $2: schedule name
# 
function runworkload () {
     sleep 240
     /usr/phd/current/hadoop-client/bin/hdfs dfsadmin -restoreFailedStorage true  >> $1 2>&1
     /usr/phd/current/hadoop-client/bin/hdfs dfsadmin -report -live               >> $1 2>&1                                      
     psql -d postgres -c "select * from gp_segment_configuration;"                >> $1 2>&1
     psql -d postgres -c "drop table if exists test;"                             >> $1 2>&1
     psql -d postgres -c "create table test(a int); insert into test values (1);" >> $1 2>&1
     echo $2
     check_replica $1 $2
}
#################
# $1: Log File name
#
function clear_cache () {
    psql -d postgres -c "select gp_metadata_cache_clear();"        >> $1 2>&1
}
#################
# $1: slaves_x
# $2: exclude file
# $3: log file name
#
function refresh_nodes () {
    echo "==============================refresh nodes============================" >> $3 2>&1
    gpscp -f ~/hostfile $1 =:$HADOOP_PATH_VAR/hadoop-client/etc/hadoop/slaves      >> $3 2>&1
    gpscp -f ~/hostfile $2 =:$HADOOP_PATH_VAR/hadoop-client/etc/hadoop/dfs.exclude >> $3 2>&1
    nodeconfig stop   NAMENODE "bcn-mst2 bcn-mst1"                                 >> $3 2>&1
    nodeconfig start  NAMENODE "bcn-mst2 bcn-mst1"                                 >> $3 2>&1
    sleep 120
    $HADOOP_PATH_VAR/hadoop-client/bin/hdfs dfsadmin -safemode get                 >> $3 2>&1
    $HADOOP_PATH_VAR/hadoop-client/bin/hdfs dfsadmin -safemode leave               >> $3 2>&1
    /usr/phd/current/hadoop-client/bin/hdfs dfsadmin -report -live                 >> $3 2>&1 
}
########## Current There are 16 Nodes. Load Data into them and run 16 HAWQ & 16 HDFS ######################################################
gpssh -f ~/hostfile -e "sudo chmod 666 /usr/phd/current/hadoop-client/etc/hadoop/slaves"
gpssh -f ~/hostfile -e "sudo chmod 666 /usr/phd/current/hadoop-client/etc/hadoop/dfs.exclude"

echo '16 Nodes with HAWQ and HDFS'    > ./performance_tpch_nodechange_16both.log
runworkload ./performance_tpch_nodechange_16both.log performance_tpch_nodechange

mv report report_16both

########## 16 HDFS node and change HAWQ node to 15, 14, 8 : to test HAWQ influence on performance. ########################################
echo '16 nodes with HDFS and 15 nodes with HAWQ'  > ./performance_tpch_nodechange_16hdfs_15hawq.log 2>&1
nodeconfig stop HAWQ "bcn-w16"                   >> ./performance_tpch_nodechange_16hdfs_15hawq.log 2>&1
sleep 150
runworkload ./performance_tpch_nodechange_16hdfs_15hawq.log performance_tpch_nodechange_noload 
nodeconfig start HAWQ "bcn-w16"                  >> ./performance_tpch_nodechange_16hdfs_15hawq.log 2>&1

echo '16 nodes with HDFS and 14 nodes with HAWQ'  > ./performance_tpch_nodechange_16hdfs_14hawq.log 2>&1
nodeconfig stop HAWQ "bcn-w16 bcn-w15"           >> ./performance_tpch_nodechange_16hdfs_14hawq.log 2>&1
sleep 150
runworkload ./performance_tpch_nodechange_16hdfs_14hawq.log performance_tpch_nodechange_noload 
nodeconfig start HAWQ "bcn-w16 bcn-w15"          >> ./performance_tpch_nodechange_16hdfs_14hawq.log 2>&1

echo '16 nodes with HDFS and 8 nodes with HAWQ'                                         > ./performance_tpch_nodechange_16hdfs_8hawq.log 2>&1
nodeconfig stop HAWQ "bcn-w16 bcn-w15 bcn-w14 bcn-w13 bcn-w12 bcn-w11 bcn-w10 bcn-w9"  >> ./performance_tpch_nodechange_16hdfs_8hawq.log 2>&1
sleep 150
runworkload ./performance_tpch_nodechange_16hdfs_8hawq.log performance_tpch_nodechange_noload
nodeconfig start HAWQ "bcn-w16 bcn-w15 bcn-w14 bcn-w13 bcn-w12 bcn-w11 bcn-w10 bcn-w9" >> ./performance_tpch_nodechange_16hdfs_8hawq.log 2>&1

mv report report_16hdfs_changehawq

########## 16 HAWQ node and change HDFS node to 15, 14 (8 hdfs nodes have missing blocks): to test HDFS influence on performance. ##########
echo '16 nodes with HAWQ and 15 nodes with HDFS with cache'  > ./performance_tpch_nodechange_16hawq_15hdfs_withcache.log 2>&1
nodeconfig stop HDFS "bcn-w16"                              >> ./performance_tpch_nodechange_16hawq_15hdfs_withcache.log 2>&1
refresh_nodes ./shrink/slaves_15 ./shrink/slaves_ex1 ./performance_tpch_nodechange_16hawq_15hdfs_withcache.log
runworkload   ./performance_tpch_nodechange_16hawq_15hdfs_withcache.log performance_tpch_nodechange_noload
nodeconfig start HDFS "bcn-w16"                             >> ./performance_tpch_nodechange_16hawq_15hdfs_withcache.log 2>&1
refresh_nodes ./shrink/slaves_16 ./shrink/slaves_ex0 ./performance_tpch_nodechange_16hawq_15hdfs_withcache.log

echo '16 nodes with HAWQ and 14 nodes with HDFS with cache'  > ./performance_tpch_nodechange_16hawq_14hdfs_withcache.log 2>&1
nodeconfig stop HDFS "bcn-w16 bcn-w15"                      >> ./performance_tpch_nodechange_16hawq_14hdfs_withcache.log 2>&1
refresh_nodes ./shrink/slaves_14 ./shrink/slaves_ex2 ./performance_tpch_nodechange_16hawq_14hdfs_withcache.log
runworkload   ./performance_tpch_nodechange_16hawq_14hdfs_withcache.log performance_tpch_nodechange_noload
nodeconfig start HDFS "bcn-w16 bcn-w15"                     >> ./performance_tpch_nodechange_16hawq_14hdfs_withcache.log 2>&1
refresh_nodes ./shrink/slaves_16 ./shrink/slaves_ex0 ./performance_tpch_nodechange_16hawq_14hdfs_withcache.log

mv report report_16hawq_changehdfs_cache

########## 16 HAWQ node and change HDFS node to 15, 14, 8 (clear cache): to test cache influence on performance when hdfs change. ##########
echo '16 nodes with HAWQ and 15 nodes with HDFS clear cache' > ./performance_tpch_nodechange_16hawq_15hdfs_clear_cache.log 2>&1
nodeconfig stop HDFS "bcn-w16"                              >> ./performance_tpch_nodechange_16hawq_15hdfs_clear_cache.log 2>&1
clear_cache   ./performance_tpch_nodechange_16hawq_15hdfs_clear_cache.log
refresh_nodes ./shrink/slaves_15 ./shrink/slaves_ex1 ./performance_tpch_nodechange_16hawq_15hdfs_clear_cache.log
runworkload   ./performance_tpch_nodechange_16hawq_15hdfs_clear_cache.log performance_tpch_nodechange_noload
nodeconfig start HDFS "bcn-w16"                             >> ./performance_tpch_nodechange_16hawq_15hdfs_clear_cache.log 2>&1
refresh_nodes ./shrink/slaves_16 ./shrink/slaves_ex0 ./performance_tpch_nodechange_16hawq_15hdfs_clear_cache.log

echo '16 nodes with HAWQ and 14 nodes with HDFS clear cache' > ./performance_tpch_nodechange_16hawq_14hdfs_clear_cache.log 2>&1
nodeconfig stop HDFS "bcn-w16 bcn-w15"                      >> ./performance_tpch_nodechange_16hawq_14hdfs_clear_cache.log 2>&1
clear_cache   ./performance_tpch_nodechange_16hawq_14hdfs_clear_cache.log
refresh_nodes ./shrink/slaves_14 ./shrink/slaves_ex2 ./performance_tpch_nodechange_16hawq_14hdfs_clear_cache.log
runworkload   ./performance_tpch_nodechange_16hawq_14hdfs_clear_cache.log performance_tpch_nodechange_noload
nodeconfig start HDFS "bcn-w16 bcn-w15"                     >> ./performance_tpch_nodechange_16hawq_14hdfs_clear_cache.log 2>&1
refresh_nodes ./shrink/slaves_16 ./shrink/slaves_ex0 ./performance_tpch_nodechange_16hawq_14hdfs_clear_cache.log

mv report report_16hawq_changehdfs_clearcache

########## Change both HAWQ and HDFS node to 15, 14, (8 nodes hdfs have missing blocks.) ################################################
echo '15 Nodes with HAWQ and 15 nodes with HDFS' >./performance_tpch_nodechange_15both.log 2>&1
nodeconfig stop BOTH "bcn-w16"                  >>./performance_tpch_nodechange_15both.log 2>&1
sleep 150
refresh_nodes ./shrink/slaves_15 ./shrink/slaves_ex1 ./performance_tpch_nodechange_15both.log
runworkload   ./performance_tpch_nodechange_15both.log performance_tpch_nodechange_noload
nodeconfig start BOTH "bcn-w16"                 >>./performance_tpch_nodechange_15both.log 2>&1
refresh_nodes ./shrink/slaves_16 ./shrink/slaves_ex0 ./performance_tpch_nodechange_15both.log

echo '14 Nodes with HAWQ and 14 nodes with HDFS' >./performance_tpch_nodechange_14both.log 2>&1
nodeconfig stop BOTH "bcn-w16 bcn-w15"          >>./performance_tpch_nodechange_14both.log 2>&1
sleep 150
refresh_nodes ./shrink/slaves_14 ./shrink/slaves_ex2 ./performance_tpch_nodechange_14both.log
runworkload   ./performance_tpch_nodechange_14both.log performance_tpch_nodechange_noload
nodeconfig start BOTH "bcn-w16 bcn-w15"         >>./performance_tpch_nodechange_14both.log 2>&1
refresh_nodes ./shrink/slaves_16 ./shrink/slaves_ex0 ./performance_tpch_nodechange_14both.log

mv report report_changeboth

########## Change both HAWQ and HDFS node clear cache ######################################################################################
echo '15 Nodes with HAWQ and 15 nodes with HDFS clear cache' >./performance_tpch_nodechange_15both_clearcache.log 2>&1
nodeconfig    stop BOTH "bcn-w16"                           >>./performance_tpch_nodechange_15both_clearcache.log 2>&1
sleep 150
clear_cache   ./performance_tpch_nodechange_15both_clearcache.log
refresh_nodes ./shrink/slaves_15 ./shrink/slaves_ex1 ./performance_tpch_nodechange_15both_clearcache.log
runworkload   ./performance_tpch_nodechange_15both_clearcache.log performance_tpch_nodechange_noload
nodeconfig start BOTH "bcn-w16"                             >>./performance_tpch_nodechange_15both_clearcache.log 2>&1
refresh_nodes ./shrink/slaves_16 ./shrink/slaves_ex0 ./performance_tpch_nodechange_15both_clearcache.log

echo '14 Nodes with HAWQ and 14 nodes with HDFS clear cache' >./performance_tpch_nodechange_14both_clearcache.log 2>&1
nodeconfig stop BOTH "bcn-w16 bcn-w15"                      >>./performance_tpch_nodechange_14both_clearcache.log 2>&1
sleep 150
clear_cache   ./performance_tpch_nodechange_14both_clearcache.log
refresh_nodes ./shrink/slaves_14 ./shrink/slaves_ex2 ./performance_tpch_nodechange_14both_clearcache.log
runworkload   ./performance_tpch_nodechange_14both_clearcache.log performance_tpch_nodechange_noload
nodeconfig start BOTH "bcn-w16 bcn-w15"                     >>./performance_tpch_nodechange_14both_clearcache.log 2>&1
refresh_nodes ./shrink/slaves_16 ./shrink/slaves_ex0 ./performance_tpch_nodechange_14both_clearcache.log

mv report report_changeboth_withoutcache
