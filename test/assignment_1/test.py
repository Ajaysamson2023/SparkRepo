import unittest
from src.assignment_1.utils import *
from pyspark.sql.types import *


class MyTestCase(unittest.TestCase):
    spark = spark_session()

    def test_df(self):
        schema_user = StructType([
            StructField('user_id', IntegerType(), True),
            StructField('emailid', StringType(), True),
            StructField('nativelanguage', StringType(), True),
            StructField('location', StringType(), True)
        ])

        data_user = [(101, 'abc.123@gmail.com', 'hindi', 'mumbai'),
                     (102, 'jhon@gmail.com', 'english', 'usa'),
                     (103, 'madan.44@gmail.com', 'marathi', 'nagpur'),
                     ]
        user_df = self.spark.createDataFrame(data=data_user, schema=schema_user)
        user_df.show()
        '---------------------------------------------------------------------------------------------'
        schema_transaction = StructType([
            StructField("transaction_id", IntegerType(), True),
            StructField("product_id", IntegerType(), True),
            StructField("userid", IntegerType(), True),
            StructField("price", IntegerType(), True),
            StructField("product_description", StringType(), True)
        ])

        data_transaction = ([(3300103, 1000003, 103, 34000, 'tv'),
                             (3300104, 1000004, 101, 35000, 'fridge'),
                             (3300105, 1000005, 105, 55000, 'sofa')
                             ])
        transaction_df = self.spark.createDataFrame(data=data_transaction, schema=schema_transaction)
        transaction_df.show()
        '---------------------------------------------------------------------------------------------'
        schema_expected = StructType([
            StructField('user_id', IntegerType(), True),
            StructField('emailid', StringType(), True),
            StructField('nativelanguage', StringType(), True),
            StructField('location', StringType(), True),
            StructField('transaction_id', IntegerType(), True),
            StructField('product_id', IntegerType(), True),
            StructField('userid', IntegerType(), True),
            StructField('price', IntegerType(), True),
            StructField('product_description', StringType(), True)])

        data_expected = [(101, 'abc.123@gmail.com', 'hindi', 'mumbai', 3300103, 1000003, 103, 34000, 'tv'),
                         (102, 'jhon@gmail.com', 'english', 'usa', 3300104, 1000004, 101, 35000, 'fridge'),
                         (103, 'madan.44@gmail.com', 'marathi', 'nagpur', 3300105, 1000005, 105, 55000, 'sofa')
                         ]

        expected_dataframe = self.spark.createDataFrame(data=data_expected, schema=schema_expected)
        '---------------------------------------------------------------------------------------------'
        col_1_df = 'user_id'
        col_2_df = 'userid'
        join_type = 'inner'
        join_df = join_dataframe(user_df, transaction_df, col_1_df, col_2_df, join_type)
        self.assertEqual(sorted(join_df.collect()), sorted(expected_dataframe.collect()))

        '----------------------------------------Question-1-----------------------------------------------------'
        expected_location_schema = StructType([
            StructField('product_description', StringType(), True),
            StructField('location', StringType(), True),
            StructField('count_unique', IntegerType(), True)])

        expected_location_data = [("tv", "mumbai", 1),
                                  ("sofa", "nagpur", 1),
                                  ("fridge", "usa", 1)
                                  ]
        expected_location_dataframe = self.spark.createDataFrame(data=expected_location_data,
                                                                 schema=expected_location_schema)
        actual_location_dataframe = unique_locations(join_df)
        self.assertEqual(sorted(actual_location_dataframe.collect()), sorted(expected_location_dataframe.collect()))

        if __name__ == '__main__':
            unittest.main()
