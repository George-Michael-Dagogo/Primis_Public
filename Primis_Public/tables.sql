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


<--- AZURE SQL DATABASE --->
CREATE TABLE primary_tweets (
    user_id UNIQUEIDENTIFIER PRIMARY KEY, 
    actual_name varchar(50), 
    time_created varchar(50),
    screen_name varchar(50),
    tweet varchar(500),
    loca_tion text,
    descrip_tion varchar(500),
    verified TEXT,
    followers int,
    source text,
    geo_enabled TEXT

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
            
