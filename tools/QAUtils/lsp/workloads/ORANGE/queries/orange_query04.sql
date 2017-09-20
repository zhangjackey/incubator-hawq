SELECT COUNT(*) FROM fai_bds_twitter_tweets_TABLESUFFIX
WHERE year=date_part('year', 'GENERATE_DATA_START_DATE'::timestamp)
AND month=date_part('month', 'GENERATE_DATA_START_DATE'::timestamp)
AND day=date_part('day', 'GENERATE_DATA_START_DATE'::timestamp);