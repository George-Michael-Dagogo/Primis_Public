# Primis 
means "first" in latin


## Overview
#### This project basically extracts election related data in Nigeria from the twitter API, transforms it with pandas, saves it to 4 databases which are
#### AstraDB Datastax (Cassandra NoSQL database)
#### AzureSQL 
#### SQLite 
#### Google Sheets and then visualizes this data.

# main.py in the Primis folder

## Expalanation
### The EXTRACTED and TRANSFORMED tweet data has columns consisting of _time_created', 'screen_name','name', 'tweet','location', 'description','verified','followers', 'source','geo_enabled','retweet_count','truncated','lang','likes'_.
### I initially planned on using PySpark for this project but since the need for exceptional speed was not required, I used pandas for transformation instead.

### Each step of the orchestration was put into functions (_def_) for more understanding, good for orchestration and its easier to identify where errors are coming from.
#### get_data() gets the data from the twitter API and transformation occurs in the same function.
#### connect_cass() connects and pushes the data to AstraDB Datastax.
#### connect_azure() connects and pushes to AZURE SQL cloud. I later had to put it off because I was racking up cloud debt.
#### connect_sqlite() pushes data to an SQLite database, this was intended just for backup purposes.
#### connect_gsheets() pushes to a google sheet, this was used as a source for visualization using Tableau.

## Orchestration
#### PREFECT was used as an orchestration tool to rerun the flow every 3 hours.

Visualization was the aim of this project, to see which political party or candidate are being tweeted about, its intensity per location and why.

### google sheet link = https://docs.google.com/spreadsheets/d/1aceSnpINQbt7C32XB27hYbYcKJUZMnjvr78RiKGsiZQ/edit#gid=0



