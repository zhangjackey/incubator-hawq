use ${DB};

drop table if exists customer_text;
create external table customer_text (C_CUSTKEY INT,
    C_NAME        VARCHAR(25),
    C_ADDRESS     VARCHAR(40),
    C_NATIONKEY   INT,
    C_PHONE       CHAR(15),
    C_ACCTBAL     DECIMAL(15,2),
    C_MKTSEGMENT  CHAR(10),
    C_COMMENT     VARCHAR(117) )
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE
LOCATION '${LOCATION}/customer_hdfstextsimple';

drop table if exists customer_${FILE};

create table customer_${FILE}
stored as ${FILE}
as select * from customer_text
cluster by C_MKTSEGMENT;
