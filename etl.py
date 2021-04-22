import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql import types as t
from pyspark.sql.functions import col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS_CREDIENTIALS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS_CREDIENTIALS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """Create a apache spark session."""

    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
        Load data from song_data dataset and extract columns
        for songs and artists tables, then write the data into parquet 
        files to maintain the schema, while apply compression for data,
        then loading data to s3 (which acts as a Data lake).

        Parameters
        ----------
        spark: session
            This is the spark session that has been created.
        input_data: path
            This is the path to the song_data in the S3 bucket.
        output_data:
            This is the path to where the parquet files will be written.

    """
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(
        'song_id', 'title', 'artist_id', 'year', 'duration') \
        .dropDuplicates()

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id') \
        .parquet(os.path.join(output_data, 'songs/songs.parquet'), 'overwrite')

    # extract columns to create artists table
    artists_table = df.select(
        'artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude') \
        .withColumnRenamed('artist_name', 'name') \
        .withColumnRenamed('artist_location', 'location') \
        .withColumnRenamed('artist_latitude', 'latitude') \
        .withColumnRenamed('artist_longitude', 'longitude') \
        .dropDuplicates()

    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(
        output_data, 'artists/artists.parquet'), 'overwrite')


def process_log_data(spark, input_data, output_data):
    """

        Load data from log_data dataset and extract columns 
        for users and time tables, then joined log_data and song_data datasets
        to extract columns for songplays table with the data needed.
        Finally, writes the tables to parquet files to maintain the schema,
        while apply compression for the data, which will be loaded 
        on S3 Bucket (which acts as a Data lake).

        Parameters
        ----------
        spark: session
            This is the spark session that has been created.
        input_data: path
            This is the path to the log_data in the S3 bucket.
        output_data:
            This is the path to where the parquet files will be written.

    """

    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data/*/*/*.json')

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    actions_df = df.filter(df.page == 'NextSong').select('ts', 'userId', 'level', 'song', 'artist',
                                                               'sessionId', 'location', 'userAgent')

    # extract columns for users table
    users_table = df.select('userId', 'firstName', 'lastName',
                            'gender', 'level') \
        .dropDuplicates()

    # write users table to parquet files
    users_table.write.parquet(os.path.join(
        output_data, 'users/users.parquet'), 'overwrite')

    # create timestamp column from original timestamp column
    actions_df = actions_df.withColumn(
        'timestamp', actions_df.ts.cast(dataType=t.TimestampType()))

    # create datetime column from original timestamp column
    actions_df = actions_df.withColumn(
        'datetime', date_format(actions_df.timestamp, "dd-MM-yyyy HH-mm-ss"))

    # extract columns to create time table
    time_table = actions_df.select(
        col('datetime').alias('start_time'),
        hour('datetime').alias('hour'),
        dayofmonth('datetime').alias('day'),
        weekofyear('datetime').alias('week'),
        month('datetime').alias('month'),
        year('datetime').alias('year')
    ).dropDuplicates()

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(
        os.path.join(output_data, 'time/time.parquet'), 'overwrite')

    # read in song data to use for songplays table
    song_path = os.path.join(input_data, 'song_data/*/*/*/*.json')
    song_df = spark.read.json(song_path)

    # extract columns from joined song and log datasets to create songplays table
    actions_df = actions_df.alias('log_df')
    joined_df = actions_df.join(song_df, col(
        'log_df.artist') == col('song_df.artist_name'), 'inner')

    songplays_table = joined_df.select(
        col('log_df.datetime').alias('start_time'),
        col('log_df.userId').alias('user_id'),
        col('log_df.level').alias('level'),
        col('song_df.song_id').alias('song_id'),
        col('song_df.artist_id').alias('artist_id'),
        col('log_df.sessionId').alias('session_id'),
        col('log_df.location').alias('location'),
        col('log_df.userAgent').alias('user_agent'),
        year('log_df.datetime').alias('year'),
        month('log_df.datetime').alias('month')) \
        .withColumn('songplay_id', monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy(year, month) \
        .parquet(os.path.join(output_data, 'songplays/songplays.parquet'), 'overwrite')


def main():
    """
        Perform the following:
        1- Create a spark session.
        2- Read the song and log data from S3 Bucket.
        3- load data and transform them to tables,
        which will be written to parquet files.
        4- Load the parquet files to S3 Bucket (Data Lake).
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://analysis-output/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
