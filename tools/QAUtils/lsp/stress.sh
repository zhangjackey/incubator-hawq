#!/bin/sh

source ~/.bashrc
source ~/qa.sh

psql -d postgres -c "drop role role_2;"
psql -d postgres -c "drop role role_1;"
psql -d postgres -c "drop role role_default;"
psql -d postgres -c "drop resource queue queue1;"
psql -d postgres -c "drop resource queue queue2;"
psql -d postgres -c "alter resource queue pg_default with (ACTIVE_STATEMENTS=100, MEMORY_LIMIT_CLUSTER=85% , CORE_LIMIT_CLUSTER=85%,RESOURCE_OVERCOMMIT_FACTOR=2, VSEG_RESOURCE_QUOTA='mem:256mb');"
psql -d postgres -c "create resource queue copy_queue with (parent = 'pg_root',ACTIVE_STATEMENTS=20, MEMORY_LIMIT_CLUSTER=10% , CORE_LIMIT_CLUSTER=10%,RESOURCE_OVERCOMMIT_FACTOR=2, VSEG_RESOURCE_QUOTA='mem:256mb');"
psql -d postgres -c "create resource queue insert_queue with (parent = 'pg_root',ACTIVE_STATEMENTS=20, MEMORY_LIMIT_CLUSTER=5% , CORE_LIMIT_CLUSTER=5%,RESOURCE_OVERCOMMIT_FACTOR=2, VSEG_RESOURCE_QUOTA='mem:256mb');"
psql -d postgres -c "CREATE USER copyu WITH superuser RESOURCE QUEUE copy_queue;"
psql -d postgres -c "CREATE USER insertu WITH RESOURCE QUEUE insert_queue;"

#hawq config -c max_appendonly_tables -v 30000
#hawq config -c max_appendonly_segfiles -v 522144
hawq config -c hawq_rm_rejectrequest_nseg_limit -v 0.25
hawq config -c hawq_rm_tolerate_nseg_limit -v 0.25
hawq config -c default_hash_table_bucket_number -v 96
hawq config -c hawq_rm_nvseg_perquery_perseg_limit -v 6

hawq restart cluster -a -M fast
### Daily stress test 
# python -u lsp.py -s system_load -b > ./system_load.log 2>&1 
# sleep 10
# nohup python -u lsp.py -s system_run_random_2_iteration_2_concurrency -b -c > ./system_run_random_2_iteration_2_concurrency.log 2>&1 &


### QUAYL stress test
python -u lsp.py -s stress_load > ./stress_load.log 2>&1 
sleep 10
nohup python -u lsp.py -s stress_run > ./stress_run.log 2>&1 &
