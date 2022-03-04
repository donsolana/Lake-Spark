# Implentation of a Data Lake with Spark, Amazon EMR and Amazon S3

## Introduction

In this project, I implemented a Data Lake storage solution for a music streaming platform. Data Lakes are one of the many ways of storing large volumes of data. Data Lakes provide flexibility as data are stored in their original and raw format. Additionally, good data lakes are built to be highly scalable and fault tolerant. That is, they can handle increasingly large volumes of data and prevent loss during incidents of system failure or disaster. To do this data lakes harness distributed storage and processing. 

In this project, the particular flavour of distributed storage we will use is `Amazon S3` and for processing `Spark` jobs are ran on `Amazon EMR` machines (*EMR are virtual machines purpose built for distributed computing for "big data"*). 



### Architecture

1. Storage: `S3` is used to store files in this project. *S3* offers flexible (i.e schemaless) way to store files in a variety of well known formats. In this project, the raw data are in `json` files which are a common format for application data. These files are transformed into five tables by spark and writing back to `S3` as `Parquet` files, *parquet* are columnar data storage formats, they support data being partioned across storage virtual machines.

2. Processing: `Spark` is used here for ETL, Spark extracts the `json` files from `S3`. It is then used to perform the necessary transformations to transform the raw files into five tables, that resemble a star schema. This tables are created to allow for easier processing during upstream analytics processes.

3. Processing Infrastructure: This files are designed to run on `EMR` clusters.



### How to run Project

1. Sumbit the spark script in `etl.py` to EMR.



### Schema

As mentioned earlier a star schema was used with five tables. Namely:

1. Fact Table

**songplays** - records associated with song plays.

*** start_time, user_id, level, song_id, artist_id, session_id, location, user_agent***

2. Dimension Tables

**users** - users in the app
***user_id, first_name, last_name, gender, level***

**songs** - songs in database
***song_id, title, artist_id, year, duration***

**artists** - artists in database
***artist_id, name, location, latitude, longitude***

**time** - time measurement of observations in songplays
***start_time, hour, day, week, month, year, weekday***


### Dataset
The original(raw files) datasets are in the json data format, stored in `AWS S3` buckets, they include

1. ***log data*** : event level information on individual songplays.

2. ***Song data*** : information about the song such as Artist name, song length etc.


### To-do

Add data quality checks.

### Libraries

1. `Pyspark` - Python interface for running spark. 
2. `configparser` - Handings Configuration files.
3. `datetime` - Manipulating Datetime

### References

1. https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_TABLE_NEW.html

2. Milliseconds to timestamp sql
https://stackoverflow.com/questions/7872720/convert-date-from-long-time-postgres




## References

1. Saving a partioned parquet file. https://stackoverflow.com/questions/43731679/how-to-save-a-partitioned-parquet-file-in-spark-2-1

 _______________________________
|        |                      |  
|        |                      |
|        |                      |
|                              |
|                               |
|                               |
|                               |   
| ______________________________|