use ${DB};

drop table if exists part_text;
create external table part_text (P_PARTKEY INT,
    P_NAME        VARCHAR(55),
    P_MFGR        CHAR(25),
    P_BRAND       CHAR(10),
    P_TYPE        VARCHAR(25),
    P_SIZE        INT,
    P_CONTAINER   CHAR(10),
    P_RETAILPRICE DECIMAL(15,2),
    P_COMMENT     VARCHAR(23) )
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE 
LOCATION '${LOCATION}/part_hdfstextsimple';


drop table if exists part_${FILE};

create table part_${FILE}
stored as ${FILE}
as select * from part_text
cluster by p_brand;
