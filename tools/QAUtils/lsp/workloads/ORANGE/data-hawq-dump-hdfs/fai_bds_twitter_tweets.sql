CREATE WRITABLE EXTERNAL TABLE tmp_fai_bds_twitter_tweets_TABLESUFFIX (LIKE fai_bds_twitter_tweets_TABLESUFFIX)
LOCATION
('pxf://PXF_NAMENODE:51200PXF_OBJECT_PATH/fai_bds_twitter_tweets_TABLESUFFIX.txt?PROFILE=HdfsTextSimple')
FORMAT 'TEXT';

INSERT INTO tmp_fai_bds_twitter_tweets_TABLESUFFIX
SELECT * FROM fai_bds_twitter_tweets_TABLESUFFIX;

DROP EXTERNAL TABLE tmp_fai_bds_twitter_tweets_TABLESUFFIX;