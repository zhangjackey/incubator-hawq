hdfs dfs -mkdir /tpch/ 
echo "mkdir /tpch/"

hdfs dfs -mkdir /tpch/customer
echo "mkdir customer"

hdfs dfs -mkdir /tpch/lineitem
echo "mkdir lineitem"

hdfs dfs -mkdir /tpch/nation
echo "mkdir nation"

hdfs dfs -mkdir /tpch/orders
echo "mkdir orders"

hdfs dfs -mkdir /tpch/part
echo "mkdir part"

hdfs dfs -mkdir /tpch/partsupp
echo "mkdir partsupp"

hdfs dfs -mkdir /tpch/region
echo "mkdir region"

hdfs dfs -mkdir /tpch/supplier
echo "mkdir supplier"

hdfs dfs -rm /tpch/customer/customer.tbl
hdfs dfs -copyFromLocal $PWD/customer.tbl /tpch/customer/
echo "customer"

hdfs dfs -rm /tpch/lineitem/lineitem.tbl
hdfs dfs -copyFromLocal $PWD/lineitem.tbl /tpch/lineitem/
echo "lineitem"

hdfs dfs -rm /tpch/nation/nation.tbl
hdfs dfs -copyFromLocal $PWD/nation.tbl /tpch/nation/
echo "nation"

hdfs dfs -rm /tpch/orders/orders.tbl
hdfs dfs -copyFromLocal $PWD/orders.tbl /tpch/orders/
echo "orders"

hdfs dfs -rm /tpch/part/part.tbl
hdfs dfs -copyFromLocal $PWD/part.tbl /tpch/part/
echo "part"

hdfs dfs -rm /tpch/partsupp/partsupp.tbl
hdfs dfs -copyFromLocal $PWD/partsupp.tbl /tpch/partsupp/
echo "partsupp"

hdfs dfs -rm /tpch/region/region.tbl
hdfs dfs -copyFromLocal $PWD/region.tbl /tpch/region/
echo "region"

hdfs dfs -rm /tpch/supplier/supplier.tbl
hdfs dfs -copyFromLocal $PWD/supplier.tbl /tpch/supplier/
echo "supplier"
