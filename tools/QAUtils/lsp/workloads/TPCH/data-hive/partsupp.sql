use ${DB};

drop table if exists partsupp_text;
create external table partsupp_text (PS_PARTKEY INT,
    PS_SUPPKEY     INT,
    PS_AVAILQTY    INT,
    PS_SUPPLYCOST  DECIMAL(15,2),
    PS_COMMENT     VARCHAR(199) ) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE
LOCATION'${LOCATION}/partsupp_hdfstextsimple';

drop table if exists partsupp_${FILE};

create table partsupp_${FILE}
stored as ${FILE}
as select * from partsupp_text
cluster by PS_SUPPKEY;
