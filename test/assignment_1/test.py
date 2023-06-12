import unittest
from src.assignment_1.utils import *
from pyspark.sql.types import *


class MyTestCase(unittest.TestCase):
    spark = spark_session()

    def test_df(self):
        # Schema and data for User:
        schema_user = StructType([
            StructField('user_id', IntegerType(), True),
            StructField('emailid', StringType(), True),
            StructField('nativelanguage', StringType(), True),
            StructField('location', StringType(), True)
        ])

        data_user = [(101, "abc.123@gmail.com", "hindi", "mumbai"),
                     (102, "jhon@gmail.com", "english", "usa"),
                     (103, "madan.44@gmail.com", "marathi", "nagpur"),
                     (104, "local.88@outlook.com", "tamil", "chennai"),
                     (105, "sahil.55@gmail.com", "english", "usa")
                     ]
        # Creating dataframe for user_df:
        user_df = self.spark.createDataFrame(data=data_user, schema=schema_user)
        # user_df.show()
        "---------------------------------------------------------------------------------------------------------------------"
        # Schema and data for User:
        schema_transaction = StructType([
            StructField('transaction_id', IntegerType(), True),
            StructField('product_id', IntegerType(), True),
            StructField('userid', IntegerType(), True),
            StructField('price', IntegerType(), True),
            StructField('product_description', StringType(), True)
        ])

        data_transaction = [(3300101, 1000001, 101, 700, "mouse"),
                            (3300102, 1000002, 102, 900, "keyboard"),
                            (3300103, 1000003, 103, 34000, "tv"),
                            (3300104, 1000004, 101, 35000, "fridge"),
                            (3300105, 1000005, 105, 55000, "sofa")
                            ]
        # Creating dataframe for transaction_df:
        transaction_df = self.spark.createDataFrame(data=data_transaction, schema=schema_transaction)
        # transaction_df.show()
        "---------------------------------------------------------------------------------------------------------------------"
        # Schema and data expected:
        schema_expected = StructType([
            StructField('user_id', IntegerType(), True),
            StructField('emailid', StringType(), True),
            StructField('nativelanguage', StringType(), True),
            StructField('location', StringType(), True),
            StructField('transaction_id', IntegerType(), True),
            StructField('product_id', IntegerType(), True),
            StructField('userid', IntegerType(), True),
            StructField('price', IntegerType(), True),
            StructField('product_description', StringType(), True)
        ])

        data_expected = [(101, "abc.123@gmail.com", "hindi", "mumbai", 3300104, 1000004, 101, 35000, "fridge"),
                         (102, "jhon@gmail.com", "english", "usa", 3300102, 1000002, 102, 900, "keyboard"),
                         (103, "madan.44@gmail.com", "marathi", "nagpur", 3300103, 1000003, 103, 34000, "tv"),
                         (105, "sahil.55@gmail.com", "english", "usa", 3300105, 1000005, 105, 55000, "sofa"),
                         (101, "abc.123@gmail.com", "hindi", "mumbai", 3300101, 1000001, 101, 700, "mouse"),
                         ]
        expected_dataframe = self.spark.createDataFrame(data=data_expected, schema=schema_expected)
        # Transformation of dataframe:
        col_1_df = 'user_id'
        col_2_df = 'userid'
        join_type = 'inner'
        join_df = join_dataframe(user_df, transaction_df, col_1_df, col_2_df, join_type)
        self.assertEqual(sorted(join_df.collect()), sorted(expected_dataframe.collect()))
        # join_df.show()
        "---------------------------------------------------------------------------------------------------------------------"
        # Question-1:Unique location
        expected_location_schema = StructType([
            StructField('product_description', StringType(), True),
            StructField('location', StringType(), True),
            StructField('count_of_location', IntegerType(), True)
        ])

        expected_location_data = [("mouse", "mumbai", 1), ("fridge", "mumbai", 1), ("tv", "nagpur", 1),
                                  ("keyboard", "usa", 1), ("sofa", "usa", 1)]

        expected_location_dataframe = self.spark.createDataFrame(data=expected_location_data,
                                                                 schema=expected_location_schema)
        actual_location_dataframe = unique_locations(join_df)
        self.assertEqual(sorted(actual_location_dataframe.collect()), sorted(expected_location_dataframe.collect()))
        # actual_location_dataframe.show()
        "----------------------------------------------------------------------------------------------------------------------------"
        # Question-2:Product bought by each user:
        product_schema = StructType([
            StructField('userid', IntegerType(), True),
            StructField('products_bought_each', IntegerType(), True),
        ])
        product_data = [(101, 2), (102, 1), (103, 1), (105, 1)]

        expected_product_dataframe = self.spark.createDataFrame(data=product_data,
                                                                schema=product_schema)
        actual_product_dataframe = product_bought(join_df)
        self.assertEqual(sorted(actual_product_dataframe.collect()), sorted(expected_product_dataframe.collect()))
        # actual_product_dataframe.show()
        "----------------------------------------------------------------------------------------------------------------------------"
        # Question-3: Total spend:
        total_schema = StructType([
            StructField('userid', IntegerType(), True),
            StructField('product_id', IntegerType(), True),
            StructField('sum_of_spend', IntegerType(), True)
        ])
        total_data = [
                      (101, 1000004, 35000), (102, 1000002, 900), (103, 1000003, 34000),
                      (105, 1000005, 55000), (101, 1000001, 700),
                      ]
        expected_total_dataframe = self.spark.createDataFrame(data=total_data, schema=total_schema)
        actual_total_dataframe = total_spend(join_df)
        self.assertEqual(sorted(actual_total_dataframe.collect()), sorted(expected_total_dataframe.collect()))
        # actual_total_dataframe.show()

    if __name__ == '__main__':
        unittest.main()
