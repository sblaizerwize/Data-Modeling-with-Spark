import configparser
import datetime
import os
from pyspark.sql.types import TimestampType
from pyspark.sql.types import DateType
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, asc, desc
from pyspark.sql.functions import date_format, hour, dayofmonth, weekofyear, month, year

config = configparser.ConfigParser()
config.read_file(open('dl.cfg'))

os.environ['AWS_ACCESS_KEY_ID']=config.get("CREDENTIALS","AWS_ACCESS_KEY_ID")
os.environ['AWS_SECRET_ACCESS_KEY']=config.get("CREDENTIALS","AWS_SECRET_ACCESS_KEY")

def create_spark_session():
    """
    Creates a spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.access.key", os.environ['AWS_ACCESS_KEY_ID']) \
        .config("spark.hadoop.fs.s3a.secret.key", os.environ['AWS_SECRET_ACCESS_KEY']) \
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
    song_data = input_data + "song-data/song_data/*/*/*/*.json"

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create artists table
    df_clean = df.dropDuplicates(subset=['artist_id'])
    artists_table = df_clean.select(['artist_id','artist_name','artist_location','artist_latitude','artist_longitude'])
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(os.path.join(output_data, 'artists'))
    
    # extract columns to create songs table
    df_clean_songs = df.dropDuplicates(subset=['song_id'])
    songs_table_original = df_clean_songs.select(['song_id','title','artist_id','year','duration'])

    # generate song and artist tables 
    songs_table_original.createOrReplaceTempView("song_view")
    artists_table.createOrReplaceTempView("artist_view")

    # join song and artist tables
    songs_table = spark.sql('''
    SELECT
        song_id,
        title,
        a.artist_id as artist_id,
        year,
        duration,
        a.artist_name as artist
    FROM song_view s
    JOIN artist_view a
    ON s.artist_id = a.artist_id
    ''')
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy("year","artist").parquet(os.path.join(output_data, 'songs'))

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
    log_data = input_data + "log-data/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df_drop = df.dropDuplicates(subset=['userId'])
    df_clean = df_drop.filter(df_drop.userId != '')

    # extract columns for users table    
    users_table = df_clean.select(['userId', 'firstName','lastName','gender','level']).where(df_clean.page == 'NextSong').orderBy(df_clean.userId.cast("float")) 
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(os.path.join(output_data, 'users'))

    # create timestamp column from original timestamp column
    df_drop_ts = df.dropDuplicates(subset = ['ts'])
    get_timestamp = udf(lambda x: datetime.datetime.fromtimestamp(x/1000.0), TimestampType())
    df_timestamp = df_drop_ts.withColumn("timestamp", get_timestamp(df_drop_ts.ts))

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
    time_table.write.mode('overwrite').partitionBy("year","month").parquet(os.path.join(output_data, 'time'))

    # read in song data to use for songplays table
    df.createOrReplaceTempView("df_view")
    time_table.createOrReplaceTempView("time_view")
    spark.udf.register("datetimeUDF", get_timestamp)

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
    SELECT DISTINCT
        datetimeUDF(log.ts) AS start_time,
        log.userId AS user_id, 
        log.level AS level,
        s.song_id AS song_id,
        s.artist_id AS artist_id,
        log.sessionId AS session_id,
        log.location AS location,
        log.userAgent AS user_agent,
        year(datetimeUDF(log.ts)) AS year,
        month(datetimeUDF(log.ts)) AS month
    FROM df_view log
    JOIN song_view s
    ON log.song = s.title AND log.length = s.duration
    JOIN artist_view a
    ON log.artist = a.artist_name AND s.artist_id = a.artist_id
    """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy('year','month').parquet(os.path.join(output_data, 'songplays'))

def main():
    """
    Main function that:
    - Creates a spark session
    - Defines source and destiny S3 buckets
    - Process song-data and log-data
    """

    spark = create_spark_session()
    input_data = "data/"
    output_data = "s3a://sblaizerudacity/star_tables/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
