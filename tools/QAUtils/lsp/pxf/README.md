# PXF Performance Testing

## Step 1: Cluster Provisioning (Optional)

If you already have a cluster running with hawq/pxf then skip this step, otherwise cd into the `cluster-provision` dir and follow the README there

## Step 2: Deployment Scripts (Optional)

If you have a cluster without any artifacts installed then cd into `deployment-scripts` and follow the README there

## Step 3: Datagen:

### Compiling dbgen and generating data
  To generate the tpch data you first need to make the binary and then generate the data
  
  0. cd pxf-performance-testing/datagen/*
  0.make
  0. This will create dbgen executable. Run ./dbgen -? to see options
  0. ./dbgen -v -s 1024 (this will generate ~ 1TB of data). The default size is 1GB
  0. Data is put in *.tbl files which are | seperated table data for customers, orders, line items etc.  

#### Copying data to HDFS
  Once you have the files locally load them into HDFS via the following commands

  0. cp {repo_root}/pxf-performance-testing/datagen/tpch/tpch_prepare_data.sh 
  0. run ./tpch_prepare_data.sh (This will create the needed directories in HDFS and copy the tables)

## Step 4: Table generation

  Finally tables need to be created in HAWQ and Hive.

### Hive Tables

  The `hive.sql` file creates hive tables against the data we loaded into hdfs, then creates ORC tables, and then finally loads the data from the first set of tables to the second.

  0. `hive -f sql/hive.sql` 

### HAWQ PXF External Tables

  The `hawq.sql` file creates external tables using the HiveORC profile
  
  0. `psql -f sql/hawq.sql`
