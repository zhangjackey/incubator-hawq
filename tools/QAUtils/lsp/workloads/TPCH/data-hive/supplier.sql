use ${DB};

drop table if exists supplier_text;
create external table supplier_text (S_SUPPKEY INT,
    S_NAME        CHAR(25),
    S_ADDRESS     VARCHAR(40),
    S_NATIONKEY   INT,
    S_PHONE       CHAR(15),
    S_ACCTBAL     DECIMAL(15,2),
    S_COMMENT     VARCHAR(101) )
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE 
LOCATION '${LOCATION}/supplier_hdfstextsimple';

drop table if exists supplier_${FILE};

create table supplier_${FILE}
stored as ${FILE}
as select * from supplier_text
cluster by s_nationkey, s_suppkey;
