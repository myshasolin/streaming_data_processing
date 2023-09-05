#!/usr/bin/python3

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import datetime
import subprocess

subprocess.call(['hdfs', 'dfs', '-rm', '-r', '-f', '-skipTrash', '/mysha/dz6/checkpoint'])
subprocess.call(['hdfs', 'dfs', '-rm', '-r', '-f', '-skipTrash', '/mysha/dz6/parquet_files'])

spark = SparkSession.builder.appName("solin_spark").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("id", IntegerType()),
    StructField("age", FloatType()),
    StructField("firstname", StringType()),
    StructField("houses", StringType()),
    StructField("lastname", StringType()),
    StructField("school", StringType()),
    StructField("year_of_birth", IntegerType())
])

raw_files = spark.readStream.format("csv") \
    .schema(schema) \
    .options(path="/mysha/dz6/csv_file/", header=True) \
    .load()

load_time = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

def file_sink(df, freq=10):
    return df.writeStream.format("parquet") \
        .trigger(once=True) \
        .option("checkpointLocation", "/mysha/dz6/checkpoint") \
        .option("path", f"/mysha/dz6/parquet_files/p_date={str(load_time)}") \
        .option("encoding", "UTF-8") \
        .start()

# timed_files = raw_files.withColumn("p_date", F.current_timestamp())
timed_files = raw_files.withColumn("p_date", F.lit("load_time"))

stream = file_sink(timed_files)

stream.awaitTermination()

print("DONE!!")

