from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct, collect_set, sum


# Creating Spark session:
def spark_session():
    spark = SparkSession.builder.master("local[1]").appName("Spark").getOrCreate()
    return spark


# Reading user data and transaction data:
def dataframe_read(spark, n):
    dataframe_user = spark.read.csv(n, header=True, inferSchema=True)
    return dataframe_user


# Joining user and transaction dataframe:
def join_dataframe(user_df, transaction_df):
    df_join = user_df.join(transaction_df, user_df['user_id'] == transaction_df['userid'], "inner")
    return df_join


# Getting unique location from dataframe:
def unique_locations(join_df):
    unique_df = join_df.groupBy("product_description", "location").agg(countDistinct("location"))
    return unique_df


# Getting product bought from dataframe:
def product_bought(join_df):
    product_id = join_df.groupBy('user_id').agg(collect_set('product_id'))
    return product_id


# Getting total spend from dataframe:
def total_spend(join_df):
    sum_user = join_df.groupBy('user_id').agg(sum('price'))
    return sum_user
