# fdic_bank_data_warehouse
Data Engineering project that builds an entire star schema data warehouse from scratch. 

Derived from [fdic_bank_data_warehouse](https://github.com/Ludwinic1/fdic_bank_data_warehouse)

Python script builds the star schema data warehouse from scratch and completes the following:
- Extracts data from several CSV files containing 5 years of FDIC (Federal Deposit Insurance Corporation) public banking data (**CSV files located in the data folder**) from thousands of banks across the country.
- Combines and cleans the files including reducing the memory footprint significantly (around a 40%+ reduction). 
- Creates all of the dimension tables and the fact table in order to form an OLAP star schema structure.  
- Creates surrogate keys for every dimension table.
- Loads the newly created tables into SQL Server (**creates the tables in SQL Server as well**) using the SQL Alchemy module.
- Creates the primary keys for the dimension tables and the composite primary key for the fact table.
- Creates the foreign key constraints between the tables.  
- Data is loaded from SQL Server into Power BI to visualize the data and compare banks over different time periods or search for details on a particular bank.  
- **See the completed visualizations in Power BI:** 
[link to Power BI vizualizations](https://app.powerbi.com/view?r=eyJrIjoiMWFjNTg0NDktNTZiYi00YWI4LWE2MGEtY2ZjYzJmMmExOGM1IiwidCI6IjE4MDUyNDY3LTFjMmQtNGZjYy1iYjhlLWMxOWNmZDQ2YzAyZCIsImMiOjN9)

 # Tech Stack:
- Programming languages:  Python and SQL.  Python packages/modules used:  Pandas and SQL Alchemy.
- RDBMS:  PostgreSQL Server.
  

# Instructions:
1. Clone the repository locally.
2. Install the requirements from the **requirements.txt** file
3. Replace the variables **SERVER**, **DATABASE** and **DRIVER** in the sql_server_connection.py file to your local SQL Server connection (create a database in PostgreSQL first).
4. Extract the compressed data files in the /data folder
5. Run the files in this order:
   1. sql_server_connection.py
   2. data_extraction.py
   3. create_dimensions.py
   4. dimensions_to_sql_server.py - to load the files into the Dimensions Table
   5. create_fact_table.py    - to load the files into Fact Table
   6. combined-df_to_table.py - to load the files into staging table

Below is the finished star schema design once the data is loaded into PostgreSQL:

![](/Star_Schema_Picture.PNG)





