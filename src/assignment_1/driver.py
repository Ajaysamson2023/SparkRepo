from src.assignment_1.utils import *

n = '../../resource/user.csv'
n1 = '../../resource/transaction.csv'
col_1_df = 'user_id'
col_2_df = 'userid'
join_type = 'inner'

# Creating Spark session:
spark = spark_session()

# Reading user dataframe:
user_df = dataframe_read_user(spark, n)
user_df.show()

# Reading transaction dataframe:
transaction_df = dataframe_read_transaction(spark, n1)
transaction_df.show()

# Joining user and transaction dataframe:

join_df = join_dataframe(user_df, transaction_df, col_1_df, col_2_df, join_type)
join_df.show()

# Getting unique location from dataframe:
unique_df = unique_locations(join_df)
unique_df.show()

# Getting product bought from dataframe:
product_df = product_bought(join_df)
product_df.show()

# Getting total spend from dataframe:
total_spend_df = total_spend(join_df)
total_spend_df.show()
