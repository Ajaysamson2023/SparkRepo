from src.assignment_2.utils import *

import unittest


class MyTestCase(unittest.TestCase):
    def test_spark_2(self):
        spark = spark_session()
        df = schema_read_df(spark)
        actual_schema = StructType([StructField("total_lines", IntegerType(), nullable=True)])
        expected_data = [(281233,)]
        actual_data_lines = spark.createDataFrame(data=expected_data, schema=actual_schema)
        actual_input_lines = count_lines(df)
        expected_output_lines = actual_data_lines
        self.assertEqual(actual_input_lines.collect(), expected_output_lines.collect())

        actual_schema_warn = StructType([StructField("Warn_message", IntegerType(), nullable=True)])
        expected_data_warn = [(3811,)]
        actual_data_lines_warn = spark.createDataFrame(data=expected_data_warn, schema=actual_schema_warn)
        actual_input_lines_warn = find_warn(df)
        expected_output_lines_warn = actual_data_lines_warn
        self.assertEqual(actual_input_lines_warn.collect(), expected_output_lines_warn.collect())

        actual_schema_api_client = StructType([StructField("Total_api_Client", IntegerType(), nullable=True)])
        expected_data_api_client = [(37627,)]
        actual_data_lines_api_client = spark.createDataFrame(data=expected_data_api_client,
                                                             schema=actual_schema_api_client)
        actual_input_lines = api_client(df)
        expected_output_lines = actual_data_lines_api_client
        self.assertEqual(actual_input_lines.collect(), expected_output_lines.collect())

        actual_schema_max_client = StructType([StructField("max_client_request", IntegerType(), nullable=True)])
        expected_data_max_client = [(5859,)]
        actual_data_lines_max_client = spark.createDataFrame(data=expected_data_max_client,
                                                             schema=actual_schema_max_client)
        actual_input_lines = client_request(df)
        expected_output_lines = actual_data_lines_max_client
        self.assertEqual(actual_input_lines.collect(), expected_output_lines.collect())

        actual_schema_id_failed = StructType([StructField("Client_id_failed", IntegerType(), nullable=True)])
        expected_data_id_failed = [(2321,)]
        actual_data_lines_id_failed = spark.createDataFrame(data=expected_data_id_failed,
                                                             schema=actual_schema_id_failed)
        actual_input_failed = failed_request(df)
        expected_output_failed = actual_data_lines_id_failed
        self.assertEqual(actual_input_failed.collect(), expected_output_failed.collect())


if __name__ == '__main__':
    unittest.main()
