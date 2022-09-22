#!pip install tweepy
#!pip install configparser
#!pip install cassandra-driver
#pip install pyodbc
#pip install pandas
#pip install "prefect*"
#!pip install --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlib
#pip install "apache-airflow[celery]==2.3.4" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.3.4/constraints-3.7.txt"

from sqlite3 import connect
from sqlalchemy import create_engine
import tweepy
#import pyodbc
import configparser
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import pandas as pd
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.oauth2 import service_account
from prefect import Flow,task
import datetime
from prefect.schedules import IntervalSchedule

config = configparser.ConfigParser()
config.read("config.ini")


@task(max_retries=3, retry_delay=datetime.timedelta(seconds=5))
def get_data():
    api_key = config['twitter']['api_key']
    api_key_secret = config['twitter']['api_key_secret']
    access_token = config['twitter']['access_token']
    access_token_secret = config['twitter']['access_token_secret']

    auth = tweepy.OAuthHandler(api_key,api_key_secret)
    auth.set_access_token(access_token,access_token_secret)

    api = tweepy.API(auth)

    keywords = ['Buhari OR APC OR  PeterObi OR Tinubu OR PDP OR Atiku']
    #keywords = ['Buhari','APC', 'PeterObi','Tinubu','Atiku']
    #it seems the api does not return every tweet containing at least one or every keyword, it returns the only tweets that contains every keyword
    #solution was to use the OR in the keywords string as this is for tweets search only and might give errors in pure python
    limit = 300

    tweets = tweepy.Cursor(api.search_tweets, q = keywords,count = 200, tweet_mode = 'extended').items(limit)

    columns = ['time_created', 'screen_name','name', 'tweet','loca_tion', 'descrip_tion','verified','followers', 'source','geo_enabled','retweet_count','truncated','lang','likes']
    data = []


    for tweet in tweets:
        data.append([tweet.created_at, tweet.user.screen_name, tweet.user.name,tweet.full_text, tweet.user.location, tweet.user.description,tweet.user.verified,tweet.user.followers_count,tweet.source,tweet.user.geo_enabled,tweet.retweet_count,tweet.truncated,tweet.lang,tweet.favorite_count])
        
    df = pd.DataFrame(data , columns=columns)
    df = df[~df.tweet.str.contains("RT")]
    #removes retweeted tweets
    df = df.reset_index(drop = True)

    df.to_csv('tweets.csv')
    
    sf = pd.read_csv('tweets.csv')
    sf = sf.drop(sf.columns[0],axis = 1) #remove unnamed column sf[0]
    
    #sf.tweet = sf.tweet.str.replace(r'\W'," ")#replace all non aphabetic characters with space
    #sf.descrip_tion = sf.descrip_tion.str.replace(r'\W'," ")
    

    sf.to_csv(r'tweets.csv', index = False, header=True) #save to same csv file



@task(max_retries=3, retry_delay=datetime.timedelta(seconds=5))
def connect_cass():
    cloud_config= {
            'secure_connect_bundle': r'/workspace/primis_private/Primis/prim/secure-connect-omni-database.zip',
            'init-query-timeout': 10,
            'connect_timeout': 10,
            'write_request_timeout_in_ms' : 20000,
            'set-keyspace-timeout': 10
            #when your data is a little bit much
    }
    auth_provider = PlainTextAuthProvider('######################', 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXN9.1etBDEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEx7_mXIY6')
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
    session = cluster.connect("tweet")

    tabela = pd.read_csv('tweets.csv', index_col=None)
    tabela.columns = ['time_created', 'screen_name','actual_name', 'tweet','loca_tion', 'descrip_tion','verified','followers', 'source','geo_enabled','retweet_count','truncated','lang','likes']
              
            
    for i, j in tabela.iterrows():
        item = "'{}','{}',{},{},'{}','{}','{}','{}','{}',{},{},{},'{}',{}".format(j.actual_name,j.descrip_tion,j.followers,j.geo_enabled,j.loca_tion,j.screen_name,j.source,j.time_created,j.tweet,j.verified,j.retweet_count,j.truncated,j.lang,j.likes)
        query1 = "INSERT INTO election_tweets (user_id,actual_name,descrip_tion,followers,geo_enabled,loca_tion,screen_name,source,time_created,tweet,verified,retweet_count,truncated,lang,likes) VALUES (uuid(),"+ item +")IF NOT EXISTS;"
        session.execute(query1)

    #print(query1)

    cluster.shutdown()
    session.shutdown()



@task(max_retries=3, retry_delay=datetime.timedelta(seconds=5))
def connect_azure():
    # download and install odbc driver
    server = 'testtech.database.windows.net'
    database = 'testtech'
    username = 'testtech'
    password = '{XXXXXXXXXXXXXXXXXXXXXXXX@1}'   
    driver= '{ODBC Driver 17 for SQL Server}'


    #Driver={ODBC Driver 17 for SQL Server};
    #Server=tcp:testtech.database.windows.net,1433;
    #Database=testtech;
    #Uid=testtech;
    #Pwd={your_password_here};
    #Encrypt=yes;
    #TrustServerCertificate=no;
    #Connection Timeout=30;

    with pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password+';Connection Timeout=30') as conn:
        with conn.cursor() as cursor:
            #cursor.execute("DROP TABLE primary_tweets")
            #cursor.execute("CREATE TABLE primary_tweets (user_id UNIQUEIDENTIFIER PRIMARY KEY, actual_name varchar(50), time_created varchar(50),screen_name varchar(50),tweet varchar(500),loca_tion text,descrip_tion varchar(500),verified TEXT,followers int,source text,geo_enabled TEXT);")
            tabela = pd.read_csv('tweets.csv')
            tabela.columns = ['time_created', 'screen_name','actual_name', 'tweet','loca_tion', 'descrip_tion','verified','followers', 'source','geo_enabled']
                
                
            for i, j in tabela.iterrows():
                item = "'{}','{}',{},'{}','{}','{}','{}','{}','{}','{}'".format(j.actual_name,j.descrip_tion,j.followers,j.geo_enabled,j.loca_tion,j.screen_name,j.source,j.time_created,j.tweet,j.verified)
                query1 = "INSERT INTO primary_tweets (user_id,actual_name,descrip_tion,followers,geo_enabled,loca_tion,screen_name,source,time_created,tweet,verified) VALUES (newid(),"+ item +");"
                cursor.execute(query1)

@task
def connect_sqlite():
    engine = create_engine('sqlite:///primis.db', echo=False)
    #create engine to connect to your already created database

    ef = pd.read_csv('tweets.csv')
    #read your csv file

    ef.to_sql('election_tweets', engine, if_exists='append', index=False)

@task(max_retries=3, retry_delay=datetime.timedelta(seconds=5))
def connect_gsheets():
    """
    exports tweets.csv to a google sheet
    I needed to visualize the data and Datastax Astra DB(as of 4th september 2022) did not have a straight forward approach to connect with Tableau

    """

    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    SERVICE_ACCOUNT_FILE = 'keys.json'

    of = pd.read_csv('tweets.csv')
    #google sheets doesn't accept nan values
    of  = of.fillna(0)
    #run this first to save the headers
    #af =([of.columns.values.tolist()])
    #before running the function all together
    af =(of.values.tolist())


    creds = None

    creds= service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    # The ID and range of a sample spreadsheet.
    SAMPLE_SPREADSHEET_ID = '1aceSnpINQbt7C32XB27hYbYcKJUZMnjvr78RiKGsiZQ'
    service = build('sheets', 'v4', credentials=creds)

    # Call the Sheets API
    #READ
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID, 
                            range='primis!A1').execute()
    #values = result.get('values', [])


    #WRITE
    request = sheet.values().append(spreadsheetId=SAMPLE_SPREADSHEET_ID, 
                                range='primis!A2', 
                                valueInputOption='USER_ENTERED', 
                                insertDataOption='INSERT_ROWS', 
                                body={'values':af}).execute()

    print(request)


#@flow
#def flow_caso():
#    get_data()
#    connect_sqlite()
#    connect_gsheets()
#flow_caso()

def flow_caso(schedule=None):
    """
    this function is for the orchestraction/scheduling of this script
    """
    with Flow("primis",schedule=schedule) as flow:
        Extract_Transform = get_data()
        Astra_DB = connect_cass()
        Azure_DB = connect_azure()
        SQLite_DB = connect_sqlite()
        Google_Sheets_DB = connect_gsheets()
    return flow


schedule = IntervalSchedule(
    start_date = datetime.datetime.now() + datetime.timedelta(seconds = 2),
    interval = datetime.timedelta(hours=3)
)
flow=flow_caso(schedule)

flow.run()
