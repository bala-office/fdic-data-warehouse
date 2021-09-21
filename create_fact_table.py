import sqlalchemy as sa

from create_dimensions import (
    dim_area_population,
    dim_assets,
    dim_bank,
    dim_branch,
    dim_charter,
    dim_currency,
    dim_date,
    dim_deposit_code,
    dim_fdic,
    dim_location,
)
from data_extraction import combined_df
from sql_server_connection import engine

branch_cols = [
    column for column in dim_branch.columns[1:]
]   # skipping branch_key column
fact_fdic = combined_df.merge(dim_branch, on=branch_cols) 
fact_fdic.drop(columns= branch_cols, inplace=True)  

assets_cols = [column for column in dim_assets.columns[1:]]
fact_fdic = fact_fdic.merge(dim_assets, on=assets_cols)

bank_cols = [column for column in dim_bank.columns[1:]]
fact_fdic = fact_fdic.merge(dim_bank, on= bank_cols)
fact_fdic.drop(columns=bank_cols, inplace=True)

dim_location_cols = [column for column in dim_location.columns[1:]]   
fact_fdic = fact_fdic.merge(dim_location, on=dim_location_cols)
fact_fdic.drop(columns=dim_location_cols, inplace=True)

dim_date_cols = [column for column in dim_date.columns[1:]]   
fact_fdic = fact_fdic.merge(dim_date, on=dim_date_cols)
fact_fdic.drop(columns=dim_date_cols, inplace=True)

dim_charter_cols = [column for column in dim_charter.columns[1:]]  
fact_fdic = fact_fdic.merge(dim_charter, on=dim_charter_cols)
fact_fdic.drop(columns=dim_charter_cols, inplace=True)

dim_fdic_cols = [column for column in dim_fdic.columns[1:]]
fact_fdic = fact_fdic.merge(dim_fdic, on=dim_fdic_cols)
fact_fdic.drop(columns=dim_fdic_cols, inplace=True)

dim_currency_cols = [column for column in dim_currency.columns[1:]] 
fact_fdic = fact_fdic.merge(dim_currency, on=dim_currency_cols)
fact_fdic.drop(columns=dim_currency_cols, inplace=True)

col_list = [col for col in fact_fdic.columns] # renaming the duplicate columns
col_list[3] = "delete_this"
col_list[4] = "delete_this2"
fact_fdic.columns = col_list # renaming the columns
fact_fdic.drop(
    columns=["delete_this", "delete_this2"], inplace=True
)   # drops the duplicate columns

dim_area_population_cols = [column for column in dim_area_population.columns[1:]] 
fact_fdic = fact_fdic.merge(dim_area_population, on=dim_area_population_cols)
fact_fdic.drop(columns=dim_area_population_cols, inplace=True)

dim_deposit_code_cols = [column for column in dim_deposit_code.columns[1:]]
fact_fdic = fact_fdic.merge(dim_deposit_code, on=dim_deposit_code_cols)
fact_fdic.drop(columns=dim_deposit_code_cols, inplace=True)

with engine.connect() as connection:
    # Creating the fact table in SQL Server and sending the data to SQL Server.  
    fact_fdic.to_sql(
        "fact_fdic",
        connection, 
        index=False, 
        if_exists="replace",
        dtype={
            "branch_office_deposits": sa.BigInteger,
            "total_assets": sa.BigInteger,
            "total_domestic_deposits": sa.BigInteger,
            "total_deposits": sa.BigInteger,
            "branch_key": sa.INTEGER,
            "bank_key": sa.INTEGER,
            "location_key": sa.INTEGER,
            "date_key": sa.INTEGER,
            "charter_key": sa.INTEGER,
            "fdic_key": sa.INTEGER,
            "currency_key": sa.INTEGER,
            "area_population_key": sa.INTEGER,
            "deposit_code_key": sa.INTEGER,
            "assets_key": sa.INTEGER
        },
    )

    # Creating the composite primary key for the fact table
    connection.execute(
        """ ALTER TABLE fact_fdic ALTER COLUMN branch_key SET NOT NULL;"""
    )
    connection.execute(""" ALTER TABLE fact_fdic ALTER COLUMN bank_key SET NOT NULL;""")
    connection.execute(
        """ ALTER TABLE fact_fdic ALTER COLUMN location_key SET NOT NULL;"""
    )
    connection.execute(""" ALTER TABLE fact_fdic ALTER COLUMN date_key SET NOT NULL;""")
    connection.execute(
        """ ALTER TABLE fact_fdic ALTER COLUMN charter_key SET NOT NULL;""")
    connection.execute(""" ALTER TABLE fact_fdic ALTER COLUMN fdic_key SET NOT NULL;""")
    connection.execute(
        """ ALTER TABLE fact_fdic ALTER COLUMN currency_key SET NOT NULL;""")
    connection.execute(""" ALTER TABLE fact_fdic ALTER COLUMN area_population_key SET NOT NULL;""")
    connection.execute(
        """ ALTER TABLE fact_fdic ALTER COLUMN deposit_code_key SET NOT NULL;""")
    connection.execute(""" ALTER TABLE fact_fdic ALTER COLUMN assets_key SET NOT NULL;""")
    connection.execute(
        """
        ALTER TABLE fact_fdic ADD PRIMARY KEY (branch_key, bank_key, location_key, date_key, charter_key, fdic_key, currency_key,
                                            area_population_key, deposit_code_key, assets_key);
        """
    )

    # creating the foreign key constraints
    connection.execute(
        """
    ALTER TABLE fact_fdic
        ADD FOREIGN KEY (branch_key) REFERENCES dim_branch (branch_key);
    """
    )
    connection.execute(
        """
    ALTER TABLE fact_fdic
        ADD FOREIGN KEY (bank_key) REFERENCES dim_bank (bank_key);
    """
    )
    connection.execute(
        """
    ALTER TABLE fact_fdic
        ADD FOREIGN KEY (location_key) REFERENCES dim_location (location_key);
    """
    )
    connection.execute(
        """
    ALTER TABLE fact_fdic
        ADD FOREIGN KEY (date_key) REFERENCES dim_date (date_key);
    """
    )
    connection.execute(
        """
    ALTER TABLE fact_fdic
        ADD FOREIGN KEY (charter_key) REFERENCES dim_charter (charter_key);
    """
    )
    connection.execute(
        """
    ALTER TABLE fact_fdic
        ADD FOREIGN KEY (fdic_key) REFERENCES dim_fdic (fdic_key);
    """
    )
    connection.execute(
        """
    ALTER TABLE fact_fdic
        ADD FOREIGN KEY (currency_key) REFERENCES dim_currency (currency_key);
    """
    )
    connection.execute(
        """
    ALTER TABLE fact_fdic
        ADD FOREIGN KEY (area_population_key) REFERENCES dim_area_population (area_population_key);
    """
    )
    connection.execute(
        """
    ALTER TABLE fact_fdic
        ADD FOREIGN KEY (deposit_code_key) REFERENCES dim_deposit_code (deposit_code_key);
    """
    )
    connection.execute(
        """
    ALTER TABLE fact_fdic
        ADD FOREIGN KEY (assets_key) REFERENCES dim_assets (assets_key);
    """
    )





