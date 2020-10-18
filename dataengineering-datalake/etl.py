import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Date,TimestampType
from pyspark.sql.functions import udf, col,monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS CREDS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS CREDS']['AWS_SECRET_ACCESS_KEY']

# function to create spark session
def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
        Function to load song data from S3, extract the song and artist data (create dimentional tables) 
        from the data and load the   data back to S3
        
        Parameters:
        
        spark       : Spark Session
        input_data  : location of song_data
        output_data : S3 location where data (dimentional tables) in parquet format will be stored
    """
    #song_data = 'data/song_data/*/*/*/*.json'
    song_data = 'data/song_data/*/*/*/*.json'
    songSchema = R([
        Fld("artist_id",Str()),
        Fld("artist_latitude",Dbl()),
        Fld("artist_location",Str()),
        Fld("artist_longitude",Dbl()),
        Fld("artist_name",Str()),
        Fld("duration",Dbl()),
        Fld("num_songs",Int()),
        Fld("title",Str()),
        Fld("year",Int()),
    ])
    
    df = spark.read.json(song_data, schema=songSchema)

    song_fields = ["title", "artist_id","year", "duration"]

    songs_table = df.select(song_fields).dropDuplicates().withColumn("song_id", monotonically_increasing_id())
    
    songs_table.write.parquet(output_data + "songs/", mode="overwrite", partitionBy=["year","artist_id"])


    artists_fields = ["artist_id", "artist_name as name", "artist_location as location", "artist_latitude as latitude", "artist_longitude as longitude"]

    artists_table = df.selectExpr(artists_fields).dropDuplicates()

    artists_table.write.parquet(output_data + 'artists/')


def process_log_data(spark, input_data, output_data):
    '''
    Function to load log data from S3, extract the users, time (dimentional tables) and songplays (Fact Table) data 
        from the log data and load the data back to S3
        
        Parameters:
        
        spark       : Spark Session
        input_data  : location of song_data
        output_data : S3 location where data (Fact and dimentional tables) in parquet format will be stored
    
    '''
    #log_data = input_data + 'log_data/*/*/*.json'
    log_data = input_data + 'log_data/*.json'

    #read log data uisng spark json
    df = spark.read.json(log_data) 
    
    df = df.filter(df.page=='NextSong')

    # extract columns for users table    
    users_table = ["userId as user_id","firstName as first_name","lastName as last_name","gender","level"]
    users_table = df.selectExpr(users_table).dropDuplicates()
    
    # write users table to parquet files
    users_table.write.parquet(output_data + 'users/')

    # create timestamp column from original timestamp column
    get_datetime = udf(date_convert, TimestampType())
    
    df = df.withColumn("start_time", get_datetime('ts'))

    
    # extract columns to create time table
    time_table = df.select("start_time").dropDuplicates().withColumn("hour",hour(col("start_time"))) \
                .withColumn("day",day(col("start_time"))).withColumn("week",week(col("start_time"))) \
                .withColumn("year",year(col("start_time"))).withColumn("weekday",date_format(col("start_time")),"E")
    
    
    # time table to parquet files partitioned by year and month
    time_table.write.parquet(os.path.join(output_data, "time_table/"), mode='overwrite', partitionBy=["year","month"])


    # read in song data to use for songplays table
    song_df = spark.read\
                .format("parquet")\
                .option("basePath", os.path.join(output_data, "songs/"))\
                .load(os.path.join(output_data, "songs/*/*/"))

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = df.join(song_df, df.song == song_df.title, how='inner').select(monotonically_increasing_id().alias("songplay_id"),col("start_time"),col("userId").alias("user_id"),"level","song_id","artist_id", col("sessionId").alias("session_id"), "location",col("userAgent").alias("user_agent"))

    songplays_table = songplays_table.join(time_table, songplays_table.start_time == time_table.start_time, how="inner")\
                        .select("songplay_id", songplays_table.start_time, "user_id", "level", "song_id", \
                                "artist_id", "session_id", "location", "user_agent", "year", "month")

    # write songplays table to parquet files partitioned by year and month
    songplays_table.drop_duplicates().write.parquet(os.path.join(output_data, "songplays/"), mode="overwrite",\
                                                    partitionBy=["year","month"])


def main():
    '''
    main function to call the other functions to extract the data from s3, transform and load the data back to s3 
    '''
    spark = create_spark_session()
    input_data = "s3a://sparkify-dend/"
    output_data = "s3a://sparkify-dend/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
