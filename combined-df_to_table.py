from sql_server_connection import engine
from data_extraction import combined_df

with engine.connect() as connection:
       # creating the dimension SQL Server tables and sending the data to SQL Server:
       combined_df.to_sql("stg_fdic_data", connection, index=False, if_exists="replace")

