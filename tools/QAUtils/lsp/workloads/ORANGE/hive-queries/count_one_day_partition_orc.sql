SELECT COUNT(*) FROM fai_bds_twitter_tweets_orc
WHERE year=year(from_unixtime(unix_timestamp('GENERATE_DATA_START_DATE' , 'yyyyMMdd')))
AND month=month(from_unixtime(unix_timestamp('GENERATE_DATA_START_DATE' , 'yyyyMMdd')))
AND day=day(from_unixtime(unix_timestamp('GENERATE_DATA_START_DATE' , 'yyyyMMdd')));