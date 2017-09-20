use ${DB};

drop table if exists region_text;
create external table region_text (R_REGIONKEY INT,
    R_NAME       CHAR(25),
    R_COMMENT    VARCHAR(152))
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE
LOCATION '${LOCATION}/region_hdfstextsimple';

drop table if exists region_${FILE};

create table region_${FILE}
stored as ${FILE}
as select distinct * from region_text;
