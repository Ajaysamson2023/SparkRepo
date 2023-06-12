from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct, count, sum


# Creating Spark session:
def spark_session():
    spark = SparkSession.builder.master("local[1]").appName("Spark").getOrCreate()
    return spark


# Reading user data and transaction data:
def dataframe_read_user(spark, n):
    dataframe_user = spark.read.csv(n, header=True, inferSchema=True)
    return dataframe_user


def dataframe_read_transaction(spark, n1):
    dataframe_transact = spark.read.csv(n1, header=True, inferSchema=True)
    return dataframe_transact


# Joining user and transaction dataframe:
def join_dataframe(user_df, transaction_df, col_1_df, col_2_df, join_type):
    df_join = user_df.join(transaction_df, user_df[col_1_df] == transaction_df[col_2_df], join_type)
    return df_join


# Getting unique location from dataframe:
def unique_locations(join_df):
    unique_df = join_df.groupBy("product_description", "location").agg(count("location").alias("Unique_location"))
    return unique_df


# Getting product bought from dataframe:
def product_bought(join_df):
    product_id = join_df.groupBy("userid").agg(count("product_description").alias("products_bought_each"))
    return product_id


# Getting total spend from dataframe:
def total_spend(join_df):
    sum_user = join_df.groupBy("userid","product_id").agg(sum("price").alias("sum_of_spend"))
    return sum_user
