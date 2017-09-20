import os
import sys
import commands
import time
from datetime import datetime
from multiprocessing import Process

try:
    import yaml
except ImportError:
    sys.stderr.write('LSP needs pyyaml. You can get it from http://pyyaml.org.\n') 
    sys.exit(2)

try:
    from optparse import OptionParser
except ImportError:
    sys.stderr.write('LSP needs optparse.\n') 
    sys.exit(2)

LSP_HOME = os.path.abspath(os.path.dirname(__file__))
os.environ['LSP_HOME'] = LSP_HOME

if LSP_HOME not in sys.path:
    sys.path.append(LSP_HOME)

EXECUTOR_DIR = LSP_HOME + os.sep + 'executors'
if EXECUTOR_DIR not in sys.path:
    sys.path.append(EXECUTOR_DIR)

WORKLOAD_DIR = LSP_HOME + os.sep + 'workloads'
if WORKLOAD_DIR not in sys.path:
    sys.path.append(WORKLOAD_DIR)

LIB_DIR = LSP_HOME + os.sep + 'lib'
if LIB_DIR not in sys.path:
    sys.path.append(LIB_DIR)

MONI_DIR = LSP_HOME + os.sep + 'monitor'
if MONI_DIR not in sys.path:
    sys.path.append(MONI_DIR)

#try:
#    import pexpect
#except ImportError:
#    PEXPECT_DIR = LIB_DIR + os.sep + 'pexpect.tar.gz'
#    os.system('cd %s && tar -zxvf %s' % (LIB_DIR, PEXPECT_DIR))
#    os.system('cd %s/pexpect && python ./setup.py install' % (LIB_DIR))

try:
    from executors.SequentialExecutor import SequentialExecutor
except ImportError:
    sys.stderr.write('LSP needs SequentialExecutor in executors/SequentialExecutor.py.\n')
    sys.exit(2)

try:
    from executors.ConcurrentExecutor import ConcurrentExecutor
except ImportError:
    sys.stderr.write('LSP needs ConcurrentExecutor in executors/ConcurrentExecutor.py.\n')
    sys.exit(2)

try:
    from executors.DynamicExecutor import DynamicExecutor
except ImportError:
    sys.stderr.write('LSP needs DynamicExecutor in executors/DynamicExecutor.py.\n')
    sys.exit(2)

try:
    from lib.PSQL import psql
except ImportError:
    sys.stderr.write('LSP needs psql in lib/PSQL.py in Workload.py.\n')
    sys.exit(2)

try:
    from lib.utils.Check import check
except ImportError:
    sys.stderr.write('LSP needs check in lib/utils/Check.py.\n')
    sys.exit(2)

try:
    from lib.utils.Report import Report
except ImportError:
    sys.stderr.write('LSP needs Report in lib/utils/Report.py.\n')
    sys.exit(2)

try:
    from lib.RemoteCommand import remotecmd
except ImportError:
    sys.stderr.write('LSP needs remotecmd in lib/RemoteCommand.py.\n')
    sys.exit(2)

import gl

from MonitorControl import Monitor_control

def generateReport(sql_stmt, result_file):
     result = check.get_result_by_sql(sql = sql_stmt)
     result = str(result).strip().split('\r\n')
     for one_tuple in result:
         msg = str(one_tuple).strip()
         Report(result_file , msg)

###########################################################################
#  Try to run if user launches this script directly
if __name__ == '__main__':
    # parse user options
    parser = OptionParser()
    parser.add_option('-s', '--schedule', dest='schedule', action='store', help='Schedule for test execution')
    parser.add_option('-a', '--add', dest='add_option', action='store_true', default=False, help='Add result in backend database')
    parser.add_option('-c', '--check', dest='check', action='store_true', default=False, help='Check query result')
    parser.add_option('-f', '--suffix', dest='suffix', action='store_true', default=False, help='Add table suffix')
    parser.add_option('-m', '--monitor', dest='monitor', action='store', default=0, help='Monitor interval')
    parser.add_option('-r', '--report', dest='report', action='store_true', default=False, help='Generate monitor report num')
    parser.add_option('-p', '--parameter', dest='param', action='store', help='Assign resource queue parameter name and value')
    parser.add_option('-d', '--delete', dest='del_flag', action='store_true', default=False, help='Delete table parameters')
    parser.add_option('--baseline-hawq-version', dest='hawq_base_version', action='store', default=None, help='Baseline version of HAWQ2.X.')
    parser.add_option('--baseline-hdfs-version', dest='hdfs_base_version', action='store', default=None, help='Baseline version of HDFS')
    (options, args) = parser.parse_args()
    schedules = options.schedule
    add_database = options.add_option
    gl.check_result = options.check
    gl.suffix = options.suffix
    monitor_interval = options.monitor
    isreport = options.report
    # This stand for hawq/hdfs version for HAWQ 2.x
    # We have another parameters to set versions for HAWQ 1.x
    hawq2_version = options.hawq_base_version
    phd2_version = options.hdfs_base_version
    
    if options.param is None:
        rq_param = ''
    elif options.param.find(':') != -1 and options.param[-1] != ':':
        rq_param = options.param
    else:
        print 'formart error: For example, -p RESOURCE_UPPER_FACTOR:2, -p MEMORY_LIMIT_CLUSTER:20'
        sys.exit(2)

    cs_id = 0
    if schedules is None:
        sys.stderr.write('Usage: python -u lsp.py -s schedule_file1[,schedule_file2] [-a] [-c] [-f] [-p] [-r]\nPlease use python -u lsp.py -h for more info\n')
        sys.exit(2)

    schedule_list = schedules.split(',')
    beg_time = datetime.now()
    runidfile =  LSP_HOME + os.sep + os.sep + 'runid'
    runid_ptr = open(runidfile,'a+')
    start_flag = False
    cluster_name = os.getenv("CLUSTER_NAME")
    if not cluster_name:
        # Set default cluster name if it's not provided
        cluster_name='HAWQ performance on AWS'
    generate_baseline = os.getenv("GENERATE_TEST_BASELINE")

    print "Start to run lsp test..."
    print "Parsing schedule file..."
    # parse schedule file
    for schedule_name in schedule_list:
        schedule_file = LSP_HOME + os.sep + 'schedules' + os.sep + schedule_name + '.yml'
        with open(schedule_file, 'r') as fschedule:
            schedule_parser = yaml.load(fschedule)

        # parse list of the workloads for execution
        if 'workloads_list' not in schedule_parser.keys() or schedule_parser['workloads_list'] is None :
            print 'No workload is specified in schedule file : %s' %(schedule_name + '.yml')
            continue

        (status, output) = commands.getstatusoutput('rpm -qa | grep hadoop | grep hdfs | grep -v node')
        hdfs_version = output
        if status != 0 or hdfs_version == '':
            hdfs_version = 'Local HDFS Deployment'

        (status, output) = commands.getstatusoutput('rpm -qa |grep hawq- |grep x86_64')
        hawq_version = output
        if status != 0 or hawq_version == '':
            hawq_version = 'Local HAWQ Deployment'
        print "Current running HAWQ version is: %s" % hawq_version
        print "Current running HDFS version is: %s" % hdfs_version

        # Set default HAWQ 2.X baseline versions.
        if not hawq2_version:
            hawq2_version = 'hawq-2.2.0.0-4141.el6.x86_64'

        if not phd2_version:
            phd2_version = 'hadoop_2_5_0_0_1245-hdfs-2.7.3.2.5.0.0-1245.el6.x86_64'
        print "HAWQ 2.x baseline version is set to: %s" % hawq_version
        print "HDFS baseline version for HAWQ 2.X is set to: %s" % hawq_version

        # HAWQ 1.X baseline versions are hard code here.
        hawq1_version = 'HAWQ 1.3.0.0 build 13048GVA HK'
        phd1_version = 'PHD 3.0'

        if cluster_name is None and cluster_name in schedule_parser.keys():
            cluster_name = schedule_parser['cluster_name']
        if cluster_name is None:
            sys.stderr.write('Invalid cluster name!')
            add_database = False
        else:
            if cluster_name == 'HAWQ main performance on BCN cluster':
                hawq2_version = 'HAWQ 2.0.0.0_beta build 21481 BCN HK'
        # check cluster information if lsp not run in standalone mode
        if add_database:
            # check if specified cluster exists 
            cs_id = check.check_id(result_id = 'cs_id', table_name = 'hst.cluster_settings', search_condition = "cs_name = '%s'" % (cluster_name))
            if cs_id is None:
                sys.stderr.write('Invalid cluster name %s!\n' % (cluster_name))
                continue

        if not start_flag:
            start_flag = True
            # add test run information in backend database if lsp not run in standalone mode,such as build_id, build_url, hawq_version, hdfs_version
            tr_id = -1
            if add_database:
                output = commands.getoutput('cat ~/qa.sh')
                try:
                    wd = output[output.index('wd='):].split('"')[1]
                    output = commands.getoutput('%s; cat build_info_file.txt' % (wd))
                    build_id = output[output.index('PULSE_ID_INFO'):].split('\n')[0].split('=')[1]
                    build_url = output[output.index('PULSE_PROJECT_INFO'):].split('\n')[0].split('=')[1]
                except Exception, e:
                    print('read build_info_file error: ' + str(e))
                    build_id = -1
                    build_url = 'Local'


                check.insert_new_record(table_name = 'hst.test_run', 
                    col_list = 'pulse_build_id, pulse_build_url, hdfs_version, hawq_version, start_time', 
                    values = "'%s', '%s', '%s', '%s', '%s'" % (build_id, build_url, hdfs_version, hawq_version, str(beg_time)))

                tr_id = check.check_id(result_id = 'tr_id', table_name = 'hst.test_run', search_condition = "start_time = '%s'" % ( str(beg_time) ))
            
            # prepare report directory with times and the report.sql file
            report_directory = LSP_HOME + os.sep + 'report' + os.sep + datetime.now().strftime('%Y%m%d-%H%M%S')
            os.system('mkdir -p %s' % (report_directory))
            #os.system('mkdir -p %s' % (report_directory + os.sep + 'tmp'))
            report_sql_file = os.path.join(report_directory, 'report.sql')

            if monitor_interval > 0:
                monitor_control = Monitor_control(mode = 'remote', interval = monitor_interval , run_id = tr_id)
                monitor_control.start(mode = 'sync')

        # select appropriate executor to run workloads
        workloads_executor = None 
        workloads_mode = schedule_parser['workloads_mode'].upper()
        if workloads_mode == 'SEQUENTIAL':
            workloads_executor = SequentialExecutor(schedule_parser, report_directory, schedule_name, report_sql_file, cs_id, tr_id, rq_param)
        elif workloads_mode == 'CONCURRENT':
            workloads_executor = ConcurrentExecutor(schedule_parser, report_directory, schedule_name, report_sql_file, cs_id, tr_id, rq_param)
        elif workloads_mode == 'DYNAMIC':
            workloads_executor = DynamicExecutor(schedule_parser, report_directory, schedule_name, report_sql_file, cs_id, tr_id, rq_param)
        else:
            print 'Invalid workloads mode ' + workloads_mode + ' specified in schedule file.'
            sys.exit(2)

        workloads_executor.execute()
    
    end_time = datetime.now()
    duration = end_time - beg_time
    duration = duration.days*24*3600*1000 + duration.seconds*1000 + duration.microseconds/1000

    if monitor_interval > 0 and start_flag:
        monitor_control.stop()

    # update backend database to log execution time
    if add_database and start_flag:
        summary_report_directory = LSP_HOME + os.sep + 'report' + os.sep 
        check.update_record(table_name = 'hst.test_run', set_content = "end_time = '%s', duration = %d" % (str(end_time), duration), search_condition = "start_time = '%s'" % (str(beg_time)))
           
        # add detailed execution information of test cases into backend database
        cmd = 'PGHOST=perfpipelinedb.csknn2zpm8tp.us-west-2.rds.amazonaws.com PGPORT=5432 PGUSER=postgres PGDATABASE=hawq PGPASSWORD=atomiccomingcoatheat psql -t -q -f "%s"' % (report_sql_file)
        (status, result) = commands.getstatusoutput(cmd)
        """
        remotecmd.scp_command(from_user = '', from_host = '', from_file = report_sql_file,
            to_user = 'gpadmin@', to_host = 'gpdb63.qa.dh.greenplum.com', to_file = ':/tmp/', password = 'changeme')
        cmd = 'source ~/psql.sh && psql -d hawq_cov -t -q -f /tmp/report.sql'
        remotecmd.ssh_command(user = 'gpadmin', host = 'gpdb63.qa.dh.greenplum.com', password = 'changeme', command = cmd)
        """

        tr_id = check.check_id(result_id = 'tr_id', table_name = 'hst.test_run', search_condition = "start_time = '%s'" % ( str(beg_time) )) 
        # retrieve test report from backend database for pulse report purpose
        detailprefix = "|| wl_name || '|Test Case Name|' || action_type ||'.' || action_target \
        || '|Test Detail|' \
        || 'Actural Run time is: ' || CASE WHEN actual_execution_time is NOT NULL THEN actual_execution_time::int::text ELSE 'N.A.' END || ' ms, ' \
        || 'Baseline time is: ' || CASE WHEN baseline_execution_time IS NOT NULL THEN baseline_execution_time::int::text ELSE 'N.A.' END || ' ms, ' \
        || 'Comparision is: ' || CASE WHEN deviation is NOT NULL THEN deviation::decimal(5,2)::text ELSE 'N.A.' END \
        || ' ('|| CASE WHEN actual_execution_time is NOT NULL THEN actual_execution_time::int::text ELSE '0' END || ' ms)' \
        || '|Test Status|' || test_result "

        result_file = os.path.join(report_directory, 'result.txt')
        sql_stmt = "select 'Test Suite Name|HAWQ13.'" +detailprefix + "from hst.f_generate_test_report_detail(%d, '%s', '%s') \
              where lower(wl_name) not like '%s' and test_result not like 'PASS AS NEW TEST CASE';" % (tr_id, phd1_version, hawq1_version, '%' + 'rwithd' + '%')
        generateReport(sql_stmt, result_file)   
        os.system('cat %s >> %sresult_v1.txt' % (result_file,summary_report_directory))

        # retrieve test report from backend database for pulse report purpose
        result_file = os.path.join(report_directory, 'result_v2.txt')
        sql_stmt = "select 'Test Suite Name|HAWQ20.'" +detailprefix + "from hst.f_generate_test_report_detail(%d, '%s', '%s') where lower(wl_name) not like '%s';" % (tr_id, phd2_version, hawq2_version, '%' + 'rwithd' + '%')
        generateReport(sql_stmt, result_file)  
        os.system('cat %s >> %sresult_v2.txt' % (result_file,summary_report_directory))

        # generate summary report

        summaryprefix = "|Test Case Name|' || wl_name || '.' || action_type \
        || '|Test Detail|' || 'Actural Run time is: ' || CASE WHEN actual_total_execution_time is NOT NULL THEN actual_total_execution_time::int::text ELSE 'N.A.' END || ' ms, ' \
        || 'Baseline time is: ' || CASE WHEN baseline_total_execution_time IS NOT NULL THEN baseline_total_execution_time::int::text ELSE 'N.A.' END || ' ms, ' \
        || 'Comparision is: ' || CASE WHEN deviation is NOT NULL THEN deviation::decimal(5,2)::text ELSE 'N.A.' END \
        || ' ' || test_statistic || ' ' \
        || ' ('|| CASE WHEN actual_total_execution_time is NOT NULL THEN actual_total_execution_time::int::text ELSE '0' END || ' ms)' \
        || '|Test Status|' || overral_test_result "
 
        result_file = os.path.join(report_directory, 'summary_report_v2.txt')
        sql_stmt = "select 'Test Suite Name|V2_SUMMARY" + summaryprefix + " from hst.f_generate_test_report_summary(%d, %d,'%s' , '%s' )\
                where lower(wl_name) not like '%s' order by action_type, tr_id, s_id;" % (tr_id, tr_id, phd2_version, hawq2_version, '%' + 'rwithd' + '%')
        generateReport(sql_stmt, result_file) 
        os.system('cat %s >> %ssummary_report_v2.txt' % (result_file,summary_report_directory))

        # Comment out since the function does not exist anymore.
        #result_file = os.path.join(report_directory, 'summary_report_v2_noq9.txt')
        #sql_stmt = "select 'Test Suite Name|V2_NoQ9_SUMMARY" + summaryprefix + " from hst.f_generate_test_report_summary_noq9(%d, %d, '%s' , '%s' )\
        #          where lower(wl_name) not like '%s' order by action_type, tr_id, s_id;" % (tr_id, tr_id, phd2_version, hawq2_version, '%' + 'rwithd' + '%') 
        #generateReport(sql_stmt, result_file) 
        #os.system('cat %s >> %ssummary_report_v2_noq9.txt' % (result_file,summary_report_directory))  

        result_file = os.path.join(report_directory, 'summary_report_v1.txt')
        sql_stmt = "SELECT 'Test Suite Name|V1_SUMMARY" + summaryprefix + " from hst.f_generate_test_report_summary(%d, %d, '%s', '%s') where lower(wl_name) not like '%s' and baseline_total_execution_time != 0 order by action_type, tr_id, s_id;" % (tr_id, tr_id, phd1_version, hawq1_version, '%' + 'rwithd' + '%')        
        generateReport(sql_stmt, result_file)    
        os.system('cat %s >> %ssummary_report_v1.txt' % (result_file,summary_report_directory))

        # Comment out since the function does not exist anymore.
        #result_file = os.path.join(report_directory, 'summary_report_v1_noq9.txt')
        #sql_stmt =  "select 'Test Suite Name|V1_NoQ9_SUMMARY" + summaryprefix  + " from hst.f_generate_test_report_summary_noq9(%d, %d, '%s', '%s')\
        #          where lower(wl_name) not like '%s' and baseline_total_execution_time != 0 order by action_type, tr_id, s_id;" % (tr_id, tr_id, phd1_version, hawq1_version, '%' + 'rwithd' + '%')     
        #generateReport(sql_stmt, result_file)
        #runid_ptr.write(str(tr_id))
        #runid_ptr.write('\n')
        #runid_ptr.close()
        #os.system('cat %s >> %ssummary_report_v1_noq9.txt' % (result_file,summary_report_directory))

        #generate base line 
        if generate_baseline == 'true':
                sql_baseline = "insert into hst.test_baseline(hdfs_version, hawq_version, s_id, action_type, action_target, iteration, stream, status, start_time, end_time, duration, output, plan, resource_usage, adj_s_id)\
                                select hst.test_run.hdfs_version, hst.test_run.hawq_version, hst.test_result.s_id, hst.test_result.action_type, hst.test_result.action_target, hst.test_result.iteration, hst.test_result.stream, hst.test_result.status, \
                                hst.test_result.start_time, hst.test_result.end_time, hst.test_result.duration, hst.test_result.output, hst.test_result.plan, hst.test_result.resource_usage, hst.test_result.adj_s_id from hst.test_result, hst.test_run where \
                                hst.test_result.tr_id = hst.test_run.tr_id and \
                                hst.test_result.tr_id in (select hst.test_run.tr_id from hst.test_run where hst.test_run.pulse_build_id = '%s' ) order by hst.test_result.start_time;" % (build_id)
                cmd = 'PGHOST=perfpipelinedb.csknn2zpm8tp.us-west-2.rds.amazonaws.com PGPORT=5432 PGUSER=postgres PGDATABASE=hawq PGPASSWORD=atomiccomingcoatheat psql -t -q -c "%s"' % (sql_baseline)
                (status, result) = commands.getstatusoutput(cmd)

        # add resource parameter and run info into backend database
        if options.param is not None:
            if options.del_flag:
                sql = "DELETE FROM hst.parameters WHERE param_name = '%s';" % (options.param.split(':')[0].strip().upper())
                print check.get_result_by_sql(sql = sql)
            sql = "INSERT INTO hst.parameters (run_id, param_name, param_value) VALUES (%d, '%s', '%s');" % (tr_id, options.param.split(':')[0].strip().upper(), options.param.split(':')[1].strip())
            print check.get_result_by_sql(sql = sql)

        # generate monitor report
        if monitor_interval > 0 and isreport is True:
            runid_ptr = open(runid_file, 'r')
            idlist = runid_ptr.read()
            print idlist
            idlist.rstrip().replace('\n',',')
            print idlist
            sql = "select hst.f_generate_monitor_report('%s', false);" % idlist
            print sql
            result = check.get_result_by_sql(sql = sql)
            print 'generate monitor report: ', result
            runid_ptr.close()
