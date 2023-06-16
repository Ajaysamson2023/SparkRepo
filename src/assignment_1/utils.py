import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, sum
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("../../assignment_1.log"),
        logging.StreamHandler(sys.stdout)
    ])


# Creating Spark session:
def spark_session():
    spark = SparkSession.builder.master("local[1]").appName("Spark").getOrCreate()
    logging.info("Created spark session")
    return spark


# Reading user data and transaction data:
def dataframe_read_user(spark, n):
    dataframe_user = spark.read.csv(n, header=True, inferSchema=True)
    logging.info("Read the dataframe of user data")
    return dataframe_user


def dataframe_read_transaction(spark, n1):
    dataframe_transact = spark.read.csv(n1, header=True, inferSchema=True)
    logging.info("Read the dataframe of transaction data")
    return dataframe_transact


# Joining user and transaction dataframe:
def join_dataframe(user_df, transaction_df, col_1_df, col_2_df, join_type):
    df_join = user_df.join(transaction_df, user_df[col_1_df] == transaction_df[col_2_df], join_type)
    logging.info("Joined user and transaction dataframe")
    return df_join


# Getting unique location from dataframe:
def unique_locations(join_df):
    unique_df = join_df.groupBy("product_description", "location").agg(count("location").alias("Unique_location"))
    logging.info("Got unique location from dataframe")
    return unique_df


# Getting product bought from dataframe:
def product_bought(join_df):
    product_id = join_df.groupBy("userid").agg(count("product_description").alias("products_bought_each"))
    logging.info("Got product bought from dataframe")
    return product_id


# Getting total spend from dataframe:
def total_spend(join_df):
    sum_user = join_df.groupBy("userid", "product_id").agg(sum("price").alias("sum_of_spend"))
    logging.info("Got total spend  from dataframe")
    return sum_user
