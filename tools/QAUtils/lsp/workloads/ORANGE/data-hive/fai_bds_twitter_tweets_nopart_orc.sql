DROP TABLE IF EXISTS fai_bds_twitter_tweets_nopart_TABLESUFFIX_orc;
CREATE TABLE fai_bds_twitter_tweets_nopart_TABLESUFFIX_orc
(
	tweet_tech_timestampchargement  timestamp
	,tweet_id                       bigint
	,tweet_lang                     string
	,tweet_text                     string
	,tweet_is_a_retweet_status      boolean
	,tweet_retweeted_status_id      bigint
	,tweet_is_a_quoted_status       boolean
	,tweet_quoted_status_id         bigint
	,tweet_created_at               timestamp
	,tweet_in_reply_to_screen_name  string
	,tweet_in_reply_to_status_id    bigint
	,tweet_in_reply_to_user_id      bigint
	,tweet_longitude                double
	,tweet_latitude                 double
	,tweet_user_id                  bigint
	,tweet_user_name                string
	,tweet_user_screen_name         string
	,tweet_user_location            string
	,tweet_is_user_verified         boolean
	,tweet_retweeted_count          int
	,tweet_favorite_count           int
	,tweet_truncated                boolean
	,tweet_source                   string
	,year                           string
	,month                          string
	,day                            string)
STORED AS ORC;
INSERT OVERWRITE TABLE fai_bds_twitter_tweets_nopart_TABLESUFFIX_orc
SELECT * FROM fai_bds_twitter_tweets_TABLESUFFIX;