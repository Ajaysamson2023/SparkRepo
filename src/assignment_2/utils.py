from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType,StructField,IntegerType
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
    find_warn_log = df.filter(col("event_processing").like("%WARN%")).agg(count("*").alias("Warn_message"))
    return find_warn_log


def api_client(df):
    api_client_count = df.filter(col("api_client").like("%api_client%")).agg(count("*").alias("Total_api_Client"))
    return api_client_count


def split_column(df):
    split_col = df.withColumn('client_id', split(col('api_client'), '--').getItem(0)) \
        .withColumn('client_data', split(col('api_client'), '--').getItem(1)) \
        .withColumn('retriever', split(col('client_data'), ':').getItem(0)) \
        .withColumn('ghtorrent', split(col('client_data'), ':').getItem(1))
    return split_col


def client_request(split_col):
    client_requests = split_col.groupBy("client_id").agg(count("*").alias("max_client_request"))
    max_requests = client_requests.orderBy(col("max_client_request").desc())
    max_request = max_requests.limit(1)
    return max_request


def failed_request(split_col):
    failed_req = split_col.filter(col("ghtorrent").like("%Failed%"))
    max_failed = failed_req.groupBy(col("client_id")).agg(count("client_id").alias("Client_id_failed"))
    failed_http = max_failed.orderBy(col("Client_id_failed").desc())
    fail_request = failed_http.limit(1)
    return fail_request


def most_active_repo(split_col):
    most_active = split_col.groupBy("retriever").agg(count("*").alias("gh_torrent_rb_data"))
    most_active_repository = most_active.orderBy(col("gh_torrent_rb_data").desc())
    most_active_repository = most_active_repository.limit(1)
    return most_active_repository

