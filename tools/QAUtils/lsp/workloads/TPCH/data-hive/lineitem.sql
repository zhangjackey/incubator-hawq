use ${DB};

drop table if exists lineitem_text;
create external table lineitem_text 
(   L_ORDERKEY    BIGINT,
    L_PARTKEY     INT,
    L_SUPPKEY     INT,
    L_LINENUMBER  INT,
    L_QUANTITY    DECIMAL(15,2),
    L_EXTENDEDPRICE  DECIMAL(15,2),
    L_DISCOUNT    DECIMAL(15,2),
    L_TAX         DECIMAL(15,2),
    L_RETURNFLAG  CHAR(1),
    L_LINESTATUS  CHAR(1),
    L_SHIPDATE    DATE,
    L_COMMITDATE  DATE,
    L_RECEIPTDATE DATE,
    L_SHIPINSTRUCT CHAR(25),
    L_SHIPMODE     CHAR(10),
    L_COMMENT      VARCHAR(44) )
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE 
LOCATION '${LOCATION}/lineitem_hdfstextsimple';

drop table if exists lineitem_${FILE};

create table lineitem_${FILE}
stored as ${FILE}
as select * from lineitem_text
cluster by L_SHIPDATE;
