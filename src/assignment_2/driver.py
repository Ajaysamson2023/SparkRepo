from src.assignment_2.utils import *

# Creating spark session:
spark = spark_session()
df = schema_read_df(spark)
df.show()
# Counting total lines:
count_total_lines = count_lines(df)
count_total_lines.show()

# Finding warn message:
find_warn_log = find_warn(df)
find_warn_log.show()

# Counting api client:
api_client = api_client(df)
api_client.show()

# Splitting the column:
spilt_column = split_column(df)
spilt_column.show()

# Maximum client request:
client_request_max = client_request(spilt_column)
client_request_max.show()

# Maximum failed requests:
failed_request = failed_request(spilt_column)
failed_request.show()

# Most active repository:
most_active_repository = most_active_repo(spilt_column)
most_active_repository.show()

