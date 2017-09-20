-- ----------------------------------------------------------------------

DROP TABLE IF EXISTS region_TABLESUFFIX;
DROP EXTERNAL WEB TABLE IF EXISTS e_region_TABLESUFFIX;

CREATE TABLE region_TABLESUFFIX (
    R_REGIONKEY  INTEGER NOT NULL,
    R_NAME       CHAR(25) NOT NULL,
    R_COMMENT    VARCHAR(152) )
WITH (SQLSUFFIX)
DISTRIBUTED BY(R_REGIONKEY);

CREATE EXTERNAL WEB TABLE e_region_TABLESUFFIX (
    R_REGIONKEY  INTEGER,
    R_NAME       CHAR(25),
    R_COMMENT    VARCHAR(152) )
EXECUTE E'bash -c "$GPHOME/bin/dbgen -b $GPHOME/bin/dists.dss -T r -s SCALEFACTOR "' 
ON 1 FORMAT 'TEXT' (DELIMITER '|');

INSERT INTO region_TABLESUFFIX SELECT * FROM e_region_TABLESUFFIX;

-- ----------------------------------------------------------------------

DROP EXTERNAL TABLE IF EXISTS region_w_hdfstextsimple;
DROP EXTERNAL TABLE IF EXISTS region_r_PXF_TABLE_SUFFIX;

CREATE WRITABLE EXTERNAL TABLE region_w_hdfstextsimple (
    R_REGIONKEY  INTEGER,
    R_NAME       CHAR(25),
    R_COMMENT    VARCHAR(152) )
    LOCATION ('pxf://PXF_NAMENODE:51200PXF_WRITABLE_PATH/region_hdfstextsimple?PROFILE=HdfsTextSimple')
    FORMAT 'TEXT' (delimiter=E',');

INSERT INTO region_w_hdfstextsimple SELECT * FROM region_TABLESUFFIX;

CREATE READABLE EXTERNAL TABLE region_r_PXF_TABLE_SUFFIX (
    R_REGIONKEY  INTEGER,
    R_NAME       CHAR(25),
    R_COMMENT    VARCHAR(152) )
    LOCATION ('pxf://PXF_NAMENODE:51200/PXF_OBJECT_PATHregion_EXTERNAL_DATA_FORMAT?PROFILE=PXF_PROFILE')
    FORMAT 'PXF_FORMAT_TYPE' (PXF_FORMAT_OPTIONS);

-- ----------------------------------------------------------------------

