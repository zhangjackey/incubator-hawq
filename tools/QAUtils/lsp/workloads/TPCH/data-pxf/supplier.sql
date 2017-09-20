-- ----------------------------------------------------------------------

DROP TABLE IF EXISTS supplier_TABLESUFFIX;
DROP EXTERNAL WEB TABLE IF EXISTS e_supplier_TABLESUFFIX;

CREATE TABLE supplier_TABLESUFFIX (
    S_SUPPKEY     INTEGER NOT NULL,
    S_NAME        CHAR(25) NOT NULL,
    S_ADDRESS     VARCHAR(40) NOT NULL,
    S_NATIONKEY   INTEGER NOT NULL,
    S_PHONE       CHAR(15) NOT NULL,
    S_ACCTBAL     DECIMAL(15,2) NOT NULL,
    S_COMMENT     VARCHAR(101) NOT NULL )
WITH (SQLSUFFIX)
DISTRIBUTED BY(S_SUPPKEY);

CREATE EXTERNAL WEB TABLE e_supplier_TABLESUFFIX (
    S_SUPPKEY     INTEGER ,
    S_NAME        CHAR(25) ,
    S_ADDRESS     VARCHAR(40) ,
    S_NATIONKEY   INTEGER ,
    S_PHONE       CHAR(15) ,
    S_ACCTBAL     DECIMAL(15,2) ,
    S_COMMENT     VARCHAR(101) ) 
EXECUTE E'bash -c "$GPHOME/bin/dbgen -b $GPHOME/bin/dists.dss -T s -s SCALEFACTOR -N NUMSEGMENTS -n $((GP_SEGMENT_ID + 1))"'
ON NUMSEGMENTS FORMAT 'TEXT' (DELIMITER '|');

INSERT INTO supplier_TABLESUFFIX SELECT * FROM e_supplier_TABLESUFFIX;

-- ----------------------------------------------------------------------

DROP EXTERNAL TABLE IF EXISTS supplier_w_hdfstextsimple;
DROP EXTERNAL TABLE IF EXISTS supplier_r_PXF_TABLE_SUFFIX;

CREATE WRITABLE EXTERNAL TABLE supplier_w_hdfstextsimple (
    S_SUPPKEY     INTEGER,
    S_NAME        CHAR(25),
    S_ADDRESS     VARCHAR(40),
    S_NATIONKEY   INTEGER,
    S_PHONE       CHAR(15),
    S_ACCTBAL     DECIMAL(15,2),
    S_COMMENT     VARCHAR(101) )
    LOCATION ('pxf://PXF_NAMENODE:51200PXF_WRITABLE_PATH/supplier_hdfstextsimple?PROFILE=HdfsTextSimple')
    FORMAT 'TEXT' (delimiter=E',');

INSERT INTO supplier_w_hdfstextsimple SELECT * FROM supplier_TABLESUFFIX;

CREATE READABLE EXTERNAL TABLE supplier_r_PXF_TABLE_SUFFIX (
    S_SUPPKEY     INTEGER,
    S_NAME        CHAR(25),
    S_ADDRESS     VARCHAR(40),
    S_NATIONKEY   INTEGER,
    S_PHONE       CHAR(15),
    S_ACCTBAL     DECIMAL(15,2),
    S_COMMENT     VARCHAR(101) )
    LOCATION ('pxf://PXF_NAMENODE:51200/PXF_OBJECT_PATHsupplier_EXTERNAL_DATA_FORMAT?PROFILE=PXF_PROFILE')
    FORMAT 'PXF_FORMAT_TYPE' (PXF_FORMAT_OPTIONS);

-- ----------------------------------------------------------------------

