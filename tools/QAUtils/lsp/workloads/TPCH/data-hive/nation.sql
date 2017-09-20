use ${DB};

drop table if exists nation_text;
create external table nation_text (N_NATIONKEY INT,
N_NAME       CHAR(25),
 N_REGIONKEY INT,
 N_COMMENT VARCHAR(152))
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE
LOCATION '${LOCATION}/nation_hdfstextsimple';

drop table if exists nation_${FILE};

create table nation_${FILE}
stored as ${FILE}
as select distinct * from nation_text;
