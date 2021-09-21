from sqlalchemy import create_engine
import os

# SERVER = os.environ.get("SQL_SERVER_NAME")
# DATABASE = os.environ.get("FDIC_BANK_DATABASE_NAME")
# DRIVER = os.environ.get("SQL_SERVER_DRIVER")
# DATABASE_CONNECTION = f"mssql://@{SERVER}/{DATABASE}?driver={DRIVER}"
# engine = create_engine(DATABASE_CONNECTION)


# engine = create_engine('postgresql:///tutorial.db')
engine = create_engine('postgresql+psycopg2://postgres:password@localhost:5432/postgres')
# engine = create_engine('sqlite:////Users/balaramachandran/codebase/training/fdic_bank_data_warehouse/foo.db')
