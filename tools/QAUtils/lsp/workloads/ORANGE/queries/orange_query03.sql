SELECT COUNT(*) FROM fai_bds_twitter_tweets_TABLESUFFIX_nopart
WHERE year=date_part('year', 'GENERATE_DATA_START_DATE'::timestamp)
AND month=date_part('month', 'GENERATE_DATA_START_DATE'::timestamp)
AND day=date_part('day', 'GENERATE_DATA_START_DATE'::timestamp);