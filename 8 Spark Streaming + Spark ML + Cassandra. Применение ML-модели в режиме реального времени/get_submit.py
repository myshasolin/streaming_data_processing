#!/usr/bin/python3

from pyspark.ml import Pipeline, PipelineModel
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType
from pyspark.sql import functions as F
from pyspark.ml.regression import LinearRegression, RandomForestRegressor, GBTRegressor, LinearRegressionModel
from pyspark.ml.feature import OneHotEncoder, VectorAssembler, CountVectorizer, StringIndexer, IndexToString
from pyspark.ml.tuning import CrossValidatorModel
import subprocess

spark = SparkSession.builder.appName("solin_spark").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# схема
kafka_schema = StructType([
    StructField("id", IntegerType()),
    StructField("Education Level", StringType()),
    StructField("Job Title", StringType()),
    StructField("Years of Experience", FloatType())
])


# чтение данных из Kafka
kafka_df = spark.readStream. \
    format("kafka"). \
    option("kafka.bootstrap.servers", "localhost:9092"). \
    option("subscribe", "JobInformation"). \
    option("startingOffsets", "earliest"). \
    option("maxOffsetsPerTrigger", "1"). \
    load()


kafka_data = kafka_df.select(F.from_json(F.col("value").cast("String"), kafka_schema).alias("value")).select("value.*")

# чтение данных из Cassandra
cassandra_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="salary_prediction", keyspace="dz8") \
    .load().drop('Job Title', 'Education Level')

# объединение данных из Kafka и Cassandra по столбцу 'id'
joined_df = kafka_data.join(cassandra_df, on="id", how="left") \
    .select("id", "Age", "Gender", "Education Level", "Job Title", "Years of Experience")


# загрузить обученную и сохранённую модель. Она ещё будет данные трансформировать, вот такая молодец:
model = PipelineModel.load("hdfs://localhost:8020/mysha/dz8/model_RandomForestRegressor")


def get_predict(df, model=model):
    """здесь основная логика трансформации данных, добавления столбца с предсказанием и записи результата в Cassandra"""
    print("вот что на входе:")
    df.show()
    # удалим id, так как модель о таком столбце ничего не знает
    batch_id = df.select("id").first()["id"]
    batch_id = str(df.select("id").first()["id"])
    df = df.drop("id")
    # добавим технических колонок
    df = df.withColumn("Age Group", F.when(df["Age"] <= 30, "young")
                                   .when((df["Age"] > 30) & (df["Age"] <= 45), "midage")
                                   .otherwise("odl"))
    df = df.withColumn("Title Length", F.length(df["Job Title"]))
    # делаем предсказание и возвращаем id
    df_preds = model.transform(df)
    df_preds = df_preds.withColumn("id", F.lit(batch_id))
    df_preds = df_preds.withColumnRenamed("prediction", "salary prediction")
    df_preds = df_preds.select("id", "Age", "Age Group", "Education Level", "Gender", "Job Title", "salary prediction")
    print("вот что на выходе:")
    df_preds.show()
    # записывает строку-батч в Cassandra
    try:
        df_preds.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="salary_prediction", keyspace="dz8") \
            .mode("append") \
            .save()
        print('данные записаны в табицу в Cassandra')
        print('-----' * 20)
    except:
        print('строка не записалась')
        print('-----' * 20)


# функция для запуска стрима
def foreach_batch_sink(df, model, drop_checkpoint=False):
    if drop_checkpoint:
        subprocess.call(['hdfs', 'dfs', '-rm', '-r', '-f', '-skipTrash', '/mysha/dz8/checkpoint'])

    def process_batch(df_batch, batch_id):
        get_predict(df_batch, model)

    return df.writeStream \
        .foreachBatch(process_batch) \
        .trigger(processingTime='100 seconds') \
        .option("checkpointLocation", "/mysha/dz8/checkpoint") \
        .start()



s = foreach_batch_sink(joined_df, model, drop_checkpoint=True)

s.awaitTermination()

