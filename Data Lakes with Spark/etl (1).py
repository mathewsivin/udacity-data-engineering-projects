import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, minute, second, dayofweek
from pyspark.sql.types import TimestampType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Description: This function is to create a spark session in the cluster 

    Arguments:
        None 
        
    Returns:
        None
    """                                       
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Description: This function is to load the song files into spark dataframe from S3, complete transformations create tables and load the resulting tables back into S3 as paraquet tables

    Arguments:
        spark: the spark session 
        input_data: the url of the input S3 folder where the song files are loaded
        output_data: the url of the output S3 folder where the paraquet tables are stored 

    Returns:
        None
    """                                      
    # get filepath to song data file
    #song_data_path = input_data+"song_data/*.json"
    song_data_path = input_data+"song_data/A/A/A/*.json"                                       
    song_data = spark.read.json(song_data_path)
                                       
    # read song data file
    df = song_data

    # extract columns to create songs table
    songs_table = df.select("song_id","title","artist_id","year","duration")
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year","artist_id").mode('overwrite').parquet(output_data+"songs_table.parquet")

    # extract columns to create artists table
    artists_table = df.select(col("artist_id")\
                          ,col("artist_name").alias("name")\
                          ,col("artist_location").alias("location")\
                          ,col("artist_latitude").alias("latitude")\
                          ,col("artist_longitude").alias("longitude") )
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data+"artists_table.parquet")

def get_timestamp_fn(time_column):
    """
    Description: This takes the epoch timestamp in a column, and convert it to a date timestamp format

    Arguments:
        time_column: the column with the epoch timestamp format

    Returns:
        a datetimestamp format of the input 
    """                                                 
    return datetime.fromtimestamp(time_column/1000)                                       
                                       
def process_log_data(spark, input_data, output_data):
    """
    Description: This function is to load the log files into spark dataframe from S3, complete transformations create tables and load the resulting tables back into S3 as paraquet tables

    Arguments:
        spark: the spark session 
        input_data: the url of the S3 folder 
        output_data: the ulr of the output S3 folder 

    Returns:
        None
    """                                       
    # get filepath to log data file
    #log_data_path = input_data+"log_data/*.json"

    log_data_path = input_data+"log_data/2018/11/*.json"
    # read log data file
    df = spark.read.json(log_data_path)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.select(col("userId").alias("user_id")\
                          ,col("firstName").alias("first_name")\
                          ,col("lastName").alias("last_name")\
                          ,col("gender")\
                          ,col("level")) 
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data+"users_table.parquet")

    # create timestamp column from original timestamp column
    get_timestamp = udf(get_timestamp_fn, TimestampType())
    df = df.withColumn("date_time_converted", get_timestamp("ts")) 
    
    # create datetime column from original timestamp column
    df = df.withColumn("hour", hour(col("date_time_converted")))\
            .withColumn("day", dayofmonth(col("date_time_converted")))\
            .withColumn("week", weekofyear(col("date_time_converted")))\
            .withColumn("month", month(col("date_time_converted")))\
            .withColumn("year", year(col("date_time_converted")))\
            .withColumn("weekday", dayofweek(col("date_time_converted")))
    
    # extract columns to create time table
    time_table = df.select(col("date_time_converted").alias("start_time")\
                          ,col("hour")\
                          ,col("day")\
                          ,col("week")\
                          ,col("month") \
                          ,col("year")\
                          ,col("weekday"))
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year","month").mode('overwrite').parquet(output_data+"time_table.parquet")

    # read in song data to use for songplays table
    songplays_table = df.join(song_data.select("artist_name","duration","title","song_id","artist_id"), (df.artist == song_data.artist_name)\
                          & (df.length == song_data.duration)\
                          & (df.song == song_data.title))

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = songplays_table.select(col("sessionId").alias("songplay_id")\
                          ,col("date_time_converted").alias("start_time")\
                          ,col("userId").alias("user_id")\
                          ,col("level")\
                          ,col("song_id")\
                          ,col("artist_id")\
                          ,col("sessionId").alias("session_id")\
                          ,col("location")\
                          ,col("userAgent").alias("user_agent")\
                          ,col("year")\
                          ,col("month"))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year","month").mode('overwrite').parquet(output_data+"songplays_table.parquet")


def main():
    """
    Description: This function is to create a spark sesssion, load the log and song data, create the resulting tables and load the resulting tables back as paraquet tables into S3 
    """
                                           
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3://aws-emr-resources-528452576569-us-east-2/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
