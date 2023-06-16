from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType
from pyspark.sql import SparkSession
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("../../assignment_2.log"),
        logging.StreamHandler(sys.stdout)
    ])


# Creating Spark session:
def spark_session():
    spark = SparkSession.builder.master("local[1]").appName("Spark").getOrCreate()
    logging.info("Created spark session")
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
    logging.info("Counting number of total lines ")
    return count_total_lines


def find_warn(df):
    find_warn_log = df.filter(col("event_processing").like("%WARN%")).agg(count("*").alias("Warn_message"))
    logging.info("Counting number of warning messages appeared")
    logging.warning("%s Number of counting" % find_warn_log)
    return find_warn_log


def api_client(df):
    api_client_count = df.filter(col("api_client").like("%api_client%")).agg(count("*").alias("Total_api_Client"))
    logging.info("Counting number of api client lines ")
    logging.warning("%s Number of counting" % api_client_count)
    return api_client_count


def split_column(df):
    split_col = df.withColumn('client_id', split(col('api_client'), '--').getItem(0)) \
        .withColumn('client_data', split(col('api_client'), '--').getItem(1)) \
        .withColumn('retriever', split(col('client_data'), ':').getItem(0)) \
        .withColumn('ghtorrent', split(col('client_data'), ':').getItem(1))
    logging.info("Splitted  into different columns")
    logging.warning("%s Number of counting" % split_col)
    return split_col


def client_request(split_col):
    client_requests = split_col.groupBy("client_id").agg(count("*").alias("max_client_request"))
    max_requests = client_requests.orderBy(col("max_client_request").desc())
    max_request = max_requests.limit(1)
    logging.info("Counting number of maximum client requests")
    logging.warning("%s Number of client counting" % max_request)
    return max_request


def failed_request(split_col):
    failed_req = split_col.filter(col("ghtorrent").like("%Failed%"))
    max_failed = failed_req.groupBy(col("client_id")).agg(count("client_id").alias("Client_id_failed"))
    failed_http = max_failed.orderBy(col("Client_id_failed").desc())
    fail_request = failed_http.limit(1)
    logging.info("Counting number of failed requests")
    logging.warning("%s Number of failed counting" % fail_request)
    return fail_request


def active_hour(df):
    most_active_hour = df.groupBy("ght_data_retrieval").agg(count("*").alias("most_active_hour"))
    most_act_hour = most_active_hour.orderBy(col("most_active_hour").desc())
    most_hour_active = most_act_hour.limit(1)
    logging.info("Counting most active hour")
    logging.warning("%s Number of most active hour" % most_hour_active)
    return most_hour_active


# def active_hour(split_col):
#     most_active_hour = split_col.orderBy("ght_data_retrieval").agg(count("*").alias("most_active_hour"))
#     return most_active_hour


def most_active_repo(split_col):
    most_active = split_col.groupBy("retriever").agg(count("*").alias("gh_torrent_rb_data"))
    most_active_repository = most_active.orderBy(col("gh_torrent_rb_data").desc())
    most_active_repository = most_active_repository.limit(1)
    logging.info("Counting most active repository")
    logging.warning("%s Number of most active counting" % most_active_repository)
    return most_active_repository
