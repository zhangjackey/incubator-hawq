import os
import sys
from datetime import datetime, date, timedelta
from math import ceil

try:
    from workloads.Workload import *
except ImportError:
    sys.stderr.write('Orange needs workloads/Workload.py\n')
    sys.exit(2)

try:
    from lib.PSQL import psql
except ImportError:
    sys.stderr.write('Orange needs psql in lib/PSQL.py\n')
    sys.exit(2)

try:
    import gl
except ImportError:
    sys.stderr.write('Orange needs gl.py in lsp_home\n')
    sys.exit(2)

try:
    from Shell import shell
except ImportError:
    sys.stderr.write('LSP needs shell in lib/Shell.py when using Orange workload\n')
    sys.exit(2)

class Orange(Workload):

    def __init__(self, workload_specification, workload_directory, report_directory, report_sql_file, cs_id, tr_id, user): 
        # init base common setting such as dbname, load_data, run_workload , niteration etc
        Workload.__init__(self, workload_specification, workload_directory, report_directory, report_sql_file, cs_id, tr_id, user)

    def load_data(self):
        self.output('-- Start loading data')

        hawq_tables = [ 'fai_bds_twitter_tweets', 'fai_bds_twitter_tweets_nopart']
        hive_tables = ['fai_bds_twitter_tweets', 'fai_bds_twitter_tweets_orc', 'fai_bds_twitter_tweets_nopart_orc' ]
        # create database
        super(Orange, self).load_data()
        self.run_queries_dir(hawq_tables, 'data-hawq', 'CREATE TABLE', 'hawq')
        self.run_queries_dir(hawq_tables, 'data-pxf', 'CREATE EXTERNAL TABLE', 'pxf')
        self.load_generate_series_data(hawq_tables)
        self.run_queries_dir(hawq_tables, 'data-hawq-dump-hdfs', 'CREATE EXTERNAL TABLE', 'hawq')
        self.run_hive_queries_dir(hive_tables, 'data-hive')
        self.output('-- Complete loading data')

    def vacuum_analyze(self):
        self.output('-- Start vacuum analyze')
        self.output('-- Complete vacuum analyze')

    def run_queries_dir(self, tables, data_directory_suffix, success_criteria_str, test_type):
        hawq_data_directory = self.workload_directory + os.sep + data_directory_suffix
        # run all sql in each loading data file
        for table_name in tables:
            con_id = -1
            if self.continue_flag:
                with open(os.path.join(hawq_data_directory, table_name + '.sql'), 'r') as f:
                    cmd = f.read()
                cmd = self.replace_sql(sql = cmd, table_name = table_name, query_type=test_type)

                with open(self.tmp_folder + os.sep + data_directory_suffix + '-' + table_name + '.sql', 'w') as f:
                    f.write(cmd)

                self.output(cmd)
                beg_time = datetime.now()
                (ok, result) = psql.runfile(ifile = self.tmp_folder + os.sep + data_directory_suffix + '-' + table_name + '.sql', dbname = self.database_name, username = 'gpadmin', flag = '-t -A')
                end_time = datetime.now()
                self.output(result[0].split('***')[0])

                if ok and str(result).find('ERROR') == -1 and str(result).find('FATAL') == -1 and str(result).find(success_criteria_str) != -1:
                    status = 'SUCCESS'
                    con_id = -1
                else:
                    status = 'ERROR'
                    beg_time = datetime.now()
                    end_time = beg_time
                    self.continue_flag = False
            else:
                status = 'ERROR'
                beg_time = datetime.now()
                end_time = beg_time

            duration = end_time - beg_time
            duration = duration.days*24*3600*1000 + duration.seconds*1000 + duration.microseconds /1000
            beg_time = str(beg_time).split('.')[0]
            end_time = str(end_time).split('.')[0]

            self.output('   Loading=%s   Iteration=%d   Stream=%d   Status=%s   Time=%d' % (table_name, 1, 1, status, duration))
            self.report_sql("INSERT INTO hst.test_result VALUES (%d, %d, %d, 'Loading', '%s', 1, 1, '%s', '%s', '%s', %d, NULL, NULL, NULL, %d);"
                % (self.tr_id, self.s_id, con_id, table_name, status, beg_time, end_time, duration, self.adj_s_id))

    def run_hive_queries_dir(self, tables, data_directory_suffix):
            hive_data_directory = self.workload_directory + os.sep + data_directory_suffix
            # run all sql in each loading data file
            for table_name in tables:
                con_id = -1
                if self.continue_flag:
                    with open(os.path.join(hive_data_directory, table_name + '.sql'), 'r') as f:
                        cmd = f.read()
                    cmd = self.replace_sql(sql = cmd, table_name = table_name, query_type='hive')
    
                    with open(self.tmp_folder + os.sep + data_directory_suffix + '-' + table_name + '.sql', 'w') as f:
                        f.write(cmd)
    
                    self.output(cmd)
                    beg_time = datetime.now()
                    (ok, result) = shell.run("hive -f %s" % self.tmp_folder + os.sep + data_directory_suffix + '-' + table_name + '.sql')
                    end_time = datetime.now()
    
                    if ok:
                        status = 'SUCCESS'
                        con_id = -1
                    else:
                        status = 'ERROR'
                        beg_time = datetime.now()
                        end_time = beg_time
                        self.continue_flag = False
                else:
                    status = 'ERROR'
                    beg_time = datetime.now()
                    end_time = beg_time
    
                duration = end_time - beg_time
                duration = duration.days*24*3600*1000 + duration.seconds*1000 + duration.microseconds /1000
                beg_time = str(beg_time).split('.')[0]
                end_time = str(end_time).split('.')[0]
    
                self.output('   Loading=%s   Iteration=%d   Stream=%d   Status=%s   Time=%d' % (table_name, 1, 1, status, duration))
                self.report_sql("INSERT INTO hst.test_result VALUES (%d, %d, %d, 'Loading', '%s', 1, 1, '%s', '%s', '%s', %d, NULL, NULL, NULL, %d);"
                    % (self.tr_id, self.s_id, con_id, table_name, status, beg_time, end_time, duration, self.adj_s_id))

    def load_generate_series_data(self, hawq_tables):
            hawq_data_directory = self.workload_directory + os.sep + 'data-gen-series-hawq'
            # run all sql in each loading data file
            for table_name in hawq_tables:
                con_id = -1

                with open(os.path.join(hawq_data_directory, table_name + '.sql'), 'r') as f:
                        cmd = f.read()
                cmd = self.replace_sql(sql = cmd, table_name = table_name, query_type='hawq')
                with open(self.tmp_folder + os.sep + table_name + '.sql', 'w') as f:
                    f.write(cmd)
                self.output(cmd)
                
                for batch_num in xrange(self.generate_data_batches_num):
                    if self.continue_flag:
                        batch_start_date = (datetime.strptime(str(self.generate_data_start_date), '%Y%m%d').date() + timedelta(days=batch_num * ceil(self.generate_data_batch_size / float(60 * 60 * 24)))).strftime("%Y%m%d")
                        flags = '-t -A -v v_date_YYYYMMDD="\'%s\'" -v v_nb_rows="%s" ' % (batch_start_date, self.generate_data_batch_size)
                        beg_time = datetime.now()
                        (ok, result) = psql.runfile(ifile = self.tmp_folder + os.sep + table_name + '.sql', dbname = self.database_name, username = 'gpadmin', flag = flags )
                        end_time = datetime.now()
                        self.output(result[0].split('***')[0])
                        if ok and str(result).find('ERROR') == -1 and str(result).find('FATAL') == -1 and str(result).find('INSERT 0') != -1:
                            status = 'SUCCESS'
                            con_id = -1
                        else:
                            status = 'ERROR'
                            beg_time = datetime.now()
                            end_time = beg_time
                            self.continue_flag = False
                    else:
                        status = 'ERROR'
                        beg_time = datetime.now()
                        end_time = beg_time

                    duration = end_time - beg_time
                    duration = duration.days*24*3600*1000 + duration.seconds*1000 + duration.microseconds /1000
                    beg_time = str(beg_time).split('.')[0]
                    end_time = str(end_time).split('.')[0]

                    self.output('   Loading=%s   Iteration=%d   Stream=%d   Status=%s   Time=%d' % (table_name, batch_num, 1, status, duration))
                    self.report_sql("INSERT INTO hst.test_result VALUES (%d, %d, %d, 'Loading', '%s', 1, 1, '%s', '%s', '%s', %d, NULL, NULL, NULL, %d);"
                        % (self.tr_id, self.s_id, con_id, table_name, status, beg_time, end_time, duration, self.adj_s_id))


    def run_one_query(self, iteration, stream, qf_name, query, query_type='hawq'):
        replaced_sql = self.replace_sql(query, qf_name, query_type)
        super(Orange, self).run_one_query(iteration, stream, qf_name, replaced_sql, query_type)

    def replace_sql(self, sql, table_name, query_type):
        if gl.suffix and query_type == 'hawq':
            sql = sql.replace('TABLESUFFIX', self.tbl_suffix)
        elif query_type == 'pxf':
            sql = sql.replace('TABLESUFFIX', self.pxf_profile)
        else:
            sql = sql.replace('_TABLESUFFIX', '')
        sql = sql.replace('PXF_NAMENODE', str(self.pxf_namenode))
        sql = sql.replace('PXF_OBJECT_PATH', str(self.pxf_object_path))
        sql = sql.replace('PXF_PROFILE', str(self.pxf_profile))
        sql = sql.replace('GENERATE_DATA_START_DATE', str(self.generate_data_start_date))

        return sql


    def execute(self):
        super(Orange, self).execute()
        hive_queries = ['count_full_orc', 'count_full_orc_no_partition', 'count_one_day_no_partition_orc', 'count_one_day_partition_orc']
        self.run_hive_queries_dir(hive_queries, 'hive-queries')
