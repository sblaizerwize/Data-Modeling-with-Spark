import configparser
import datetime
import os
from pyspark.sql.types import TimestampType
from pyspark.sql.types import DateType
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, asc, desc
from pyspark.sql.functions import date_format, hour, dayofmonth, weekofyear, month, year

def create_spark_session():
    """
    Creates a spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .appName('DataLakes') \
        .getOrCreate()
    spark.sparkContext._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.algorithm.version", "2")
    return spark

def process_song_data(spark, input_data, output_data):
    """
    Process song-data:
    - Loads JSON data into a dataframe
    - Removes duplicates
    - Creates songs and artists tables complying with the star schema design
    """
        
    # get filepath to song data file
    song_data = input_data + "song-data/A/A/*/*.json"

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create artists table
    df_clean = df.dropDuplicates(subset=['song_id'])
    artists_table = df_clean.select(['artist_id','artist_name','artist_location','artist_latitude','artist_longitude'])
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + "/artists/artists_table.parquet")

    # extract columns to create songs table
    songs_table = df_clean.select(['song_id','title','artist_id','year','duration'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy("year","artist_id").parquet(output_data + "/songs/songs_table.parquet")

def process_log_data(spark, input_data, output_data):
    """
    Process log-data:
    - Loads JSON data into a dataframe
    - Removes duplicates and UserId empty strings
    - Creates user table
    - Transforms time from timestamp to daytime format
    - Creates time table
    - Creates songplays table complying with the star schema design
    """
    
    # get filepath to log data file
    log_data = input_data + "log-data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df_drop = df.dropDuplicates(subset=['userId'])
    df_clean = df_drop.filter(df_drop.userId != '')
    df_page = df_clean.filter(df_clean.page == 'NextSong')

    # extract columns for users table    
    users_table = df_page.select(['userId', 'firstName','lastName','gender','level'])
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data + "/users/users_table.parquet")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.datetime.fromtimestamp(x/1000.0), TimestampType())
    df_timestamp = df_page.withColumn("timestamp", get_timestamp(df_page.ts))

    # create datetime column from original timestamp column
    df_timestamp_star = df_timestamp.withColumn("hour", hour(col("timestamp")))\
    .withColumn("day", dayofmonth(col("timestamp")))\
    .withColumn("week", weekofyear(col("timestamp")))\
    .withColumn("month", month(col("timestamp")))\
    .withColumn("year", year(col("timestamp")))\
    .withColumn("weekday", date_format(col("timestamp"), "E"))
    
    # extract columns to create time table
    time_table = df_timestamp_star.select(["timestamp","hour","day","week","month","year","weekday"])
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy("year","month").parquet(output_data + "time/time_table.parquet")

    # read song-data file 
    song_data = input_data + "song-data/A/A/*/*.json"
    df_song = spark.read.json(song_data)
    df_song = df_song.dropDuplicates(subset=['song_id'])

    # generate df_timestamp, song and artist tables 
    df_timestamp.createOrReplaceTempView("df_view")
    df_song.createOrReplaceTempView("song_view")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
    SELECT DISTINCT
        log.timestamp AS start_time,
        log.userId AS user_id, 
        log.level AS level,
        s.song_id AS song_id,
        s.artist_id AS artist_id,
        log.sessionId AS session_id,
        log.location AS location,
        log.userAgent AS user_agent,
        year(log.timestamp) AS year,
        month(log.timestamp) AS month
    FROM df_view log
    JOIN song_view s
    ON log.song = s.title 
    AND log.length = s.duration 
    AND log.artist = s.artist_name
    """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy('year','month').parquet(output_data + "songplays/songplays_table.parquet")

def main():
    """
    Main function that:
    - Creates a spark session
    - Defines source and destiny S3 buckets
    - Process song-data and log-data
    """
    
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sblaizerudacity/star_tables/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
