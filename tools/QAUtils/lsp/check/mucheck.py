import os
import string
import subprocess
from optparse import OptionParser


def retrieve_databases(db_name='template1'):
    p = subprocess.Popen(['psql -t -d %s -f get_databases.sql' % (db_name)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    (out_db, err_db) = p.communicate()

    if p.returncode != 0:
       print "Failed to retrieve databases in catelog: errno=%d" % (p.returncode)
       print err_db
       exit(-1)

    out_db = out_db.splitlines()
    out_db = [x.strip() for x in out_db]
    out_db = filter(None, out_db)

    return out_db


def retrieve_userdata_from_db(db_name):
    if db_name is None or db_name.lower() in ['template0', 'template1', 'postgres', 'hdfs']:
        print 'Invalid database name is specified: %s.' % (db_name)
        print 'Hint: database name should NOT be in [None, \'template0\', \'template1\', \'postgres\', \'hdfs\']'
        exit(-1)

    p = subprocess.Popen(['psql -t -d %s -f get_userdata_tables.sql' % (db_name)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    (out_table, err_table) = p.communicate()

    if p.returncode != 0:
        print "Failed to retrieve table in database %s: errno=%d" % (db_name, p.returncode)
        print err_table
        exit(-1)

    out_table = out_table.splitlines()
    out_table = [x.strip() for x in out_table]
    out_table = filter(None, out_table)
    out_table.insert(0, '\c ' + db_name)

    return out_table


def retrieve_userdata():
    dbs = retrieve_databases()

    out_table = []
    for db_name in dbs:
        out_table.append('-- Retrieving USER DATA from DATABASE %s' % (db_name))
        out_table.extend(retrieve_userdata_from_db(db_name))

    return out_table


def retrieve_metadata_from_db(db_name='template1'):
    out_table_view = []

    p = subprocess.Popen(['psql -t -d %s -f get_metadata_tables.sql' % (db_name)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    (out_table, err_table) = p.communicate()

    if p.returncode != 0:
       print "Failed to retrieve tables in catelog: errno=%d" % (p.returncode)
       print err_table
       exit(-1)

    out_table = out_table.splitlines()
    out_table = [x.strip() for x in out_table]
    out_table = filter(None, out_table)
    out_table.insert(0, '\c ' + db_name)
    out_table_view.extend(out_table)

    p = subprocess.Popen(['psql -t -d %s -f get_metadata_views.sql' % (db_name)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    (out_view, err_view) = p.communicate()

    if p.returncode !=0:
        print "Failed to retrieve views in catelog: errno=%d" % (p.returncode)
        print err_view
        exit(-1)

    out_view = out_view.splitlines()
    out_view = [x.strip() for x in out_view]
    out_view = filter(None, out_view)
    out_table_view.extend(out_view)

    return out_table_view


def retrieve_metadata():
    dbs = retrieve_databases()

    out_table_view = []
    for db_name in dbs:
        out_table_view.append('-- Retrieving METADATA from DATABASE %s' % (db_name))
        out_table_view.extend(retrieve_metadata_from_db(db_name))

    return out_table_view


if __name__ == '__main__':
    # parse user options
    usage = "Usage: python %prog [options]"
    parser = OptionParser(usage=usage)
    parser.add_option("-m", "--metadata", dest="metadata", action="store_true", help="retrieve metadata from catalog")
    parser.add_option("-u", "--userdata", dest="userdata", action="store_true", help="retrieve user data from tables")
    parser.add_option("-r", "--runtest", dest="runtest", action="store_true", help="run test to retrieve user data and metadata")
    parser.add_option("-i", "--if", dest="infile", action="store", help="name of the input file to store queries to retrieve user data and metadata", default='metadata_userdata_check.sql')
    parser.add_option("-o", "--of", dest="outfile", action="store", help="name of the out file to store output of retrieved user data and metadata", default='metadata_userdata_check.out')
    (options, args) = parser.parse_args()

    with open(options.infile, 'w') as ifh:
        try:
            # retrieve metadata
            if options.metadata:
                metadata = retrieve_metadata()
                for m in metadata:
                    ifh.write(m+'\n')

            # retrieve userdata
            if options.userdata:
                userdata = retrieve_userdata()
                for u in userdata:
                    ifh.write(u+'\n')
        except IOError:
            print 'Could not open file for write: %s.' % (options.infile)
        finally:
            print 'Succeeded to generate %s which contains queries to retrieve metadata and user data.' % (options.infile)
            ifh.close()

    if options.runtest:
        os.system('nohup psql -a -d template1 -f %s > %s 2>&1 &' % (options.infile, options.outfile))
        print 'Succeeded to start to retrieve metadata and user data. Please check %s patiently as it may run for a while.' % (options.outfile)
