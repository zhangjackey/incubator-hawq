# lsp ( Longevity Stress Performance )
---
## Overview
LSP is a system test framework for HAWQ performance, stress, longevity tests.

## Target & Design

* Flexibility to add & import third-part workloads
* Easy to blend & concurrently execute hybrid workloads
* Capability to capture and analyze KPIs
 + Correctness, consistency, response time, throughput
 + CPU usage, memory consumption, disk IO, network IO
* Easy to customize environment
 + Health check, OS hacking, fault injection

## User Guide

### Get code
Git clone this repo. $LSP_HOME is the path of your code.

    git clone https://github.com/Pivotal-DataFabric/lsp.git
    cd $LSP_HOME

### Workloads
Workloads contains data generate scripts for different workloads.Currently there are some workloads, they are listed as below.

 - TPC-H
 - TPC-DS
 - SRI (single row insert)
 - COPY
 - GDFDIST
 - RETAILDW
 - RQTPCH
 - XMARQ

### Schedule
Schedules contains different yml file that defines a list of workloads. A typical workload definition looks like this: 

 - workloads_list: list of workload name
 - workloads_mode: sequential / concurrent 
 - workloads_content: defines a list of workloads.
     + workload_name: name
     + database_name: load the data to which database
     + user: gpadmin
     + table_setting:
         + data_volume_type: PER_NODE
         + data_volume_size: 10
         + append_only: true
         + orientation: ROW
         + compression_type: SNAPPY 
         + partitions: 0
         + distributed_randomly: false for random and true for hash
         + seg_num: 6
     + load_data_flag: true means it will load data every time run this workloads, false means it won't load data.
     + run\_workload\_flag: true means it will run the queries of this workloads, false means it won't run the queries.
     + run\_workload\_mode: SEQUENTIAL(run the queries of this workload one by one in sorted order)/RANDOM(run queries with random order)/FIX_RANDOM(run the queris in a order for different iteration)
     + num\_concurrency: num (when num\_concurrency is greater than 1 then it will run in concurrent mode, and the num defines the concurrent num of queries)
     + num\_iteration: num (run the query num times)

You can define your own workloads according to the workloads folder and schedules folder.

### How to run

    python -u lsp.py -s schedule_name --options > log 2>&1
    e.g. python -u lsp.py -s performance\_tpch\_10g > log 2>&1

For lsp.py, there are several options when run a schedule.
 
- -s : (--schedule), schedule for test execution
- -a : (--add), not supported now.(add result to backend database) 
- -c : (--check), check query result
- -f : (--suffix), add table suffix
- -m : (--monitor),monitor interval
- -r : (--report), generate monirot report num
- -p : (--parameter), assign resource queue parameter name and value
- -d : (--delete), deleta table parameters

### How to check

After run the schedules, there is a `report` folder under `$LSP_HOME`. This folder stores all the information about the results of each schedule. The folder of each time will be named by the time. In each time-name folder, the result of each schedule has a same name folder and only a report.sql file. Each workload has a folder contains a output.csv for all output, queries_result for execute result and tmp for generated queries.

The report.sql contains the execute time result of all the workloads in a schedule by the order in schedule file. A typical TPC-H workload has 32 line results(8 tables create, 1 view create, 1 vacuum analyze and 22 tpc-h queries). Other workload with different type may have different num of lines result. You could check the query num under workload folder(E.g $LSP_HOME/workloads/TPCDS/queries/).

A query execution time is the 11th column(cut by ',') in the report.sql file. You could get all the execution times and compared them with your baseline to check whether there is an downgrade of performance. 
