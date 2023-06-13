from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField
from pyspark.sql import SparkSession


# Creating Spark session:
def spark_session():
    spark = SparkSession.builder.master("local[1]").appName("Spark").getOrCreate()
    return spark


def schema_read_df(spark):
    schema = StructType([
        StructField("event_processing", StringType(), True),
        StructField("ght_data_retrieval", StringType(), True),
        StructField("api_client", StringType(), True)
    ])

    df = spark.read.schema(schema).csv("../../resource/ghtorrent-logs.txt")
    return df


def count_lines(df):
    count_total_lines = df.agg(count("*").alias("total_lines"))
    return count_total_lines


def find_warn(df):
    find_warn_log = df.filter(df.event_processing == "WARN").count()
    return find_warn_log


def api_client(df):
    api_client_count = df.filter(col("api_client").like("%api_client%")).agg(count("*").alias("Total_api_Client"))
    return api_client_count


def split_column(df):
    split_col = df.withColumn('id', split(col('api_client'), '--').getItem(0)) \
        .withColumn('client_data', split(col('api_client'), '--').getItem(1)) \
        .withColumn('retriever', split(col('client_data'), ':').getItem(0)) \
        .withColumn('ghtorrent', split(col('client_data'), ':').getItem(1))
    return split_col


# def client_request(split_column):
#     client_requests = split_column.groupBy("id", "client_data").count()
#     return client_requests

# def fail_req(split_col):
#     fail_request = split_col.count()
#     return fail_request
