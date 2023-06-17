from src.assignment_2.utils import *

import unittest


class MyTestCase(unittest.TestCase):
    def test_spark_2(self):
        spark = spark_session()
        df = schema_read_df(spark)
        data_split = split_column(df)
        # Counting total lines:
        actual_schema = StructType([StructField("total_lines", IntegerType(), nullable=True)])
        actual_data = [(281233,)]
        actual_data_lines = spark.createDataFrame(data=actual_data, schema=actual_schema)
        actual_input_total_lines = count_lines(df)
        expected_output_total_lines = actual_data_lines
        self.assertEqual(actual_input_total_lines.collect(), expected_output_total_lines.collect())

        # Finding warn message:
        actual_schema_warn = StructType([StructField("Warn_message", IntegerType(), nullable=True)])
        actual_data_warn = [(3811,)]
        actual_data_lines_warn = spark.createDataFrame(data=actual_data_warn, schema=actual_schema_warn)
        actual_input_lines_warn = find_warn(df)
        expected_output_lines_warn = actual_data_lines_warn
        self.assertEqual(actual_input_lines_warn.collect(), expected_output_lines_warn.collect())

        # Counting api client:
        actual_schema_api_client = StructType([StructField("Total_api_Client", IntegerType(), nullable=True)])
        actual_data_api_client = [(37627,)]
        actual_data_lines_api_client = spark.createDataFrame(data=actual_data_api_client,
                                                             schema=actual_schema_api_client)
        actual_input_lines_api = api_client(df)
        expected_output_lines_api = actual_data_lines_api_client
        self.assertEqual(actual_input_lines_api.collect(), expected_output_lines_api.collect())

        # Maximum client request,Splitting the column:
        actual_schema_max_client = StructType([StructField("client_id", StringType(), True),
                                               StructField("max_client_request", IntegerType(), True)])
        actual_data_max_client = [(" ghtorrent-12 ", 5859)]

        actual_data_lines_max_client = spark.createDataFrame(data=actual_data_max_client,
                                                             schema=actual_schema_max_client)
        actual_input_lines_max = client_request(data_split)
        expected_output_lines_max = actual_data_lines_max_client
        self.assertEqual(actual_input_lines_max.collect(), expected_output_lines_max.collect())

        # Maximum failed requests:
        actual_schema_id_failed = StructType([StructField("client_id", StringType(), True),
                                              StructField("Client_id_failed", IntegerType(), True)])
        actual_data_id_failed = [(" ghtorrent-13 ", 2321)]
        actual_data_lines_id_failed = spark.createDataFrame(data=actual_data_id_failed,
                                                            schema=actual_schema_id_failed)
        actual_input_failed = failed_request(data_split)
        expected_output_failed = actual_data_lines_id_failed
        self.assertEqual(actual_input_failed.collect(), expected_output_failed.collect())

        # Most active repository:
        actual_schema_repo = StructType([StructField("retriever", StringType(), True),
                                         StructField("gh_torrent_rb_data", IntegerType(), True)])
        actual_data_repo = [(" ghtorrent.rb", 166397)]
        actual_data_lines_repo = spark.createDataFrame(data=actual_data_repo,
                                                       schema=actual_schema_repo)
        actual_input_most_active = most_active_repo(data_split)
        expected_output_active = actual_data_lines_repo
        self.assertEqual(actual_input_most_active.collect(), expected_output_active.collect())


if __name__ == '__main__':
    unittest.main()
