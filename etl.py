import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType, DateType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    - read files from 'song_data' 
    - Extract relevant columns fors songs and artists table
    - Write files as parquet to s3
    """
    
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(["song_id", "title", "artist_id", "year", "duration"]).dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data + "songs/dim_songs.parquet")

    # extract columns to create artists table
    artists_table = df.select(["artist_id", "artist_name", "artist_location","artist_latitude","artist_longitude"]).dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists/dim_artists.parquet")


def process_log_data(spark, input_data, output_data):
    """
    - read files from 'log_data' S3 bucket
    
    - Filters for sonplay action ('NextSong')
    
    - converts the time format from millisecond to timestamp
    
    - Extract relevant data into time, user and songplay table
    """
        
    # get filepath to log data file
    logs_data = input_data + "event_log"

    # read log data file
    logs_df = spark.read.json(logs_data)\
                  .select(["artist", "song", "length", "level", "sessionId", "location", "page", "userAgent",
                          "ts","userId", "firstName", "lastName","gender","level"])
    
    # filter by actions for song plays
    logs_df = logs_df.filter(logs_df.page == "NextSong")

    # extract columns for users table    
    users_table = logs_df.select(["userId", "firstName", "lastName", "gender", "level"]).dropDuplicates()
    
    #write table
    users_table.write.parquet(output_data + "users/dim_users.parquet")
    


    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000.0), TimestampType())
    logs_df = logs_df.withColumn("start_time", get_timestamp(col("ts")))

    
    # extract columns to create time table
    time_table = logs_df.select("start_time")\
                    .dropDuplicates()\
                    .withColumn("year", year(col("start_time")))\
                    .withColumn("month", month(col("start_time")))\
                    .withColumn("day", dayofmonth(col("start_time")))\
                    .withColumn("month", month(col("start_time")))\
                    .withColumn("hour", hour(col("start_time")))\
                    .withColumn("week", weekofyear(col("start_time")))\
                .withColumn("weekday", date_format(col("start_time"), "u"))
    
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(output_data + "time/dim_time.parquet")

    # read in song data to use for songplays tables
    song_data = input_data + "song_data/*/*/*/*.json" #get file path
    songs_df = spark.read.json(song_data).select(["artist_name", "title", "duration", "song_id", "artist_id"])
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = logs_df.join(songs_df, (songs_df.artist_name == logs_df.artist) & 
                                      (songs_df.title == logs_df.song) & 
                                      (songs_df.duration == logs_df.length),'inner')\
                        .select(["start_time", "userId", "level", "song_id", "artist_id", "sessionId", "location", "userAgent"]).dropDuplicates()\
                        .dropDuplicates()

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy(col(start_time).year, col(start_time).month).parquet(output_data + "songplay/dim_songplay.parquet")


def main():
    """
    - Creates spark session as 'spark' variable
    - Calls the process_song_data and process_log_data files defined above.
    """
    spark = create_spark_session()
    input_data = config['IO']['INPUT_DATA']
    output_data = config['IO']['OUTPUT_DATA']
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
