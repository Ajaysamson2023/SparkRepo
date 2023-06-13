from src.assignment_2.utils import *

spark = spark_session()
df = schema_read_df(spark)
df.show()

count_total_lines = count_lines(df)
count_total_lines.show()

find_warn_log = find_warn(df)
# find_warn_log.show()

api_client = api_client(df)
api_client.show()

spilt_column = split_column(df)
spilt_column.show()

# client_request = client_request(split_column)
# client_request.show()

# fail_requests = fail_req(df)
# fail_requests.show()
