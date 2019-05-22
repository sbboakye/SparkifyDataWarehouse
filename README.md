# SPARKIFY DATA WAREHOUSE MODEL WITH S3 AND REDSHIFT

## INTRODUCTION

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

## Project Description

In this project, I'll apply what I've learned on data warehouses and AWS to build an ETL pipeline for a database hosted on Redshift. To complete the project, I will need to load data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables.

![Sparkify Database Schema!](data-model-cut.png)

## USING THE STAR SCHEMA TO MODEL DIMENSIONS AND FACT TABLES FROM STAGING TABLES

This is the simplest way to structure tables in a database. As can be seen above the structure looks like a star. This is a combination of a central fact table surrounded by related dimension tables. 

This is optimized for querying large datasets and it is easy to understand. Dimension tables are linked to the fact table with foreign keys.

## PROJECT STRUCTURE
The data for this project is on S3 on AWS.
Configurations can be seen in the `dwh.cfg` file;
```
[S3]
LOG_DATA=s3://udacity-dend/log_data
LOG_JSONPATH=s3://udacity-dend/log_json_path.json
SONG_DATA=s3://udacity-dend/song_data
```

`etl.py` contains the finished etl pipeline to move data from s3 buckets to staging tables on redshift and create dimensions and fact tables for analysis.
We have the `sql_queries.py` file which contains all the sql queries for dropping of tables, creation, copying and insertion.
Lastly, the `create_tables.py` is used to drop tables and create tables depending on existence

To run script:
1. Make sure to run create_tables.py first to drop tables and create tables.
2. Next, run etl.py.
Hopefully everything goes well
