<--- CASSANDRAdb DataStax --->

CREATE TABLE  election_tweets (
    user_id uuid PRIMARY KEY,
    actual_name varchar,
    time_created timestamp,
    screen_name varchar,
    tweet varchar,
    loca_tion text,
    descrip_tion varchar,
    verified boolean,
    followers int,
    source text,
    geo_enabled boolean,
    retweet_count int,
    truncated boolean,
    lang text,
    likes int
    
)



<--- #ORACLE DATABASE --->

CREATE TABLE ADMIN.TWEETS 
    ( 
     TIME_CREATED  TIMESTAMP(6) , 
     SCREEN_NAME   VARCHAR2(4000) , 
     NAME          VARCHAR2(4000) , 
     TWEET         VARCHAR2(4000) , 
     LOCA_TION     VARCHAR2(4000) , 
     DESCRIP_TION  VARCHAR2(4000) , 
     VERIFIED      VARCHAR2(4000) , 
     FOLLOWERS     NUMBER , 
     SOURCE        VARCHAR2(4000) , 
     GEO_ENABLED   VARCHAR2(4000) , 
     RETWEET_COUNT NUMBER , 
     TRUNCATED     VARCHAR2(4000) , 
     LANG          VARCHAR2(4000) , 
     LIKES         NUMBER 
    ) 
    LOGGING 
;
            