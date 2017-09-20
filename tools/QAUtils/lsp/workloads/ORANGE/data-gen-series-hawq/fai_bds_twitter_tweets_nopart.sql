INSERT INTO fai_bds_twitter_tweets_nopart_TABLESUFFIX
SELECT :v_date_YYYYMMDD::timestamp + random() * mod(id,86400) * interval '1 sec' AS tweet_tech_timestampchargement
,680000000000000000 +   :v_date_YYYYMMDD::bigint*100000000 + id as tweet_id
,'fr' as tweet_lang
,substr(md5(random()::text) || md5(random()::text) || md5(random()::text) || md5(random()::text) ,1,(random()*100)::integer) as tweet_text
,cast(cast(random() as integer) as boolean) as tweet_is_a_retweet_status
,680000000000000000 + (id * random())::bigint as tweet_retweeted_status_id
,null as tweet_is_a_quoted_status
,null as tweet_quoted_status_id
,:v_date_YYYYMMDD::timestamp + random() * mod(id,86400) * interval '1 sec' as tweet_created_at
,null as tweet_in_reply_to_screen_name
,null as tweet_in_reply_to_status_id
,null as tweet_in_reply_to_user_id
,null as tweet_longitude
,null as tweet_latitude
,(random() * mod(id, 10000000))::bigint as tweet_user_id
,md5(random()::text) as tweet_user_name
,md5(random()::text) as tweet_user_screen_name
,md5(random()::text) as tweet_user_location
,cast(cast(random() as integer) as boolean) as tweet_is_user_verified
,mod(id, 50) as tweet_retweeted_count
,mod(id,12) as tweet_favorite_count
,cast(cast(random() as integer) as boolean) as tweet_truncated
,'Twitter for iPad' as tweet_source
,substr(:v_date_YYYYMMDD,1,4) as year
,substr(:v_date_YYYYMMDD,5,2)::integer as month
,substr(:v_date_YYYYMMDD,7,2)::integer as  day
FROM (select generate_series(1,:v_nb_rows) id) T