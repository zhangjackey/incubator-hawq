DROP EXTERNAL TABLE IF EXISTS fai_bds_twitter_tweets_TABLESUFFIX_nopart;
CREATE EXTERNAL TABLE fai_bds_twitter_tweets_TABLESUFFIX_nopart(
  tweet_tech_timestampchargement timestamp,
  tweet_id bigint,
  tweet_lang text,
  tweet_text text ,
  tweet_is_a_retweet_status boolean,
  tweet_retweeted_status_id bigint,
  tweet_is_a_quoted_status boolean ,
  tweet_quoted_status_id bigint ,
  tweet_created_at timestamp ,
  tweet_in_reply_to_screen_name text ,
  tweet_in_reply_to_status_id bigint ,
  tweet_in_reply_to_user_id  bigint,
  tweet_longitude float8 ,
  tweet_latitude float8 ,
  tweet_user_id bigint ,
  tweet_user_name text ,
  tweet_user_screen_name text ,
  tweet_user_location text ,
  tweet_is_user_verified boolean ,
  tweet_retweeted_count int ,
  tweet_favorite_count int,
  tweet_truncated boolean,
  tweet_source text,
  year text,
  month text,
  day text)
LOCATION ('pxf://PXF_NAMENODE:51200/fai_bds_twitter_tweets_nopart_orc?profile=PXF_PROFILE')
FORMAT 'CUSTOM' (formatter='pxfwritable_import');