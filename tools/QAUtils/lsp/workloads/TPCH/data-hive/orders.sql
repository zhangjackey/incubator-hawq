use ${DB};

drop table if exists orders_text;
create external table orders_text (
    O_ORDERKEY       BIGINT,
    O_CUSTKEY        INT,
    O_ORDERSTATUS    CHAR(1),
    O_TOTALPRICE     DECIMAL(15,2),
    O_ORDERDATE      DATE,
    O_ORDERPRIORITY  CHAR(15),
    O_CLERK          CHAR(15),
    O_SHIPPRIORITY   INT,
    O_COMMENT        VARCHAR(79) )
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE
LOCATION '${LOCATION}/orders_hdfstextsimple';

drop table if exists orders_${FILE};

create table orders_${FILE}
stored as ${FILE}
as select * from orders_text
cluster by o_orderdate;
