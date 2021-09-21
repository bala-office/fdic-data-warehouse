from sql_server_connection import engine
import sqlalchemy as sa
from create_dimensions import ( 
       dim_branch, 
       dim_bank, 
       dim_area_population,
       dim_location,
       dim_assets,
       dim_date,
       dim_charter,
       dim_fdic,
       dim_currency,
       dim_deposit_code,
)

with engine.connect() as connection:
       # creating the dimension SQL Server tables and sending the data to SQL Server:
       dim_branch.to_sql("dim_branch", connection, index=False, if_exists="replace", 
                                   dtype= { "branch_key": sa.INTEGER, 
                                                 "unique_bank_branch_id": sa.INTEGER,
                                                 "branch_unique_number": sa.INTEGER,
                                                 "branch_address": sa.VARCHAR(120),
                                                 "branch_city_name": sa.VARCHAR(100),
                                                 "branch_county_name": sa.VARCHAR(100),
                                                 "branch_state": sa.VARCHAR(50),
                                                 "branch_zipcode": sa.INTEGER,
                                                 "branch_service_type": sa.INTEGER,
                                                 "branch_city": sa.VARCHAR(120),
                                                 "branch_country_name": sa.VARCHAR(100),
                                                 "branch_county_number": sa.INTEGER,
                                                 "branch_name": sa.VARCHAR(100),
                                                 "branch_state_name": sa.VARCHAR(100),
                                                 "acquisition_date": sa.DateTime(),
                                                 "branch_location_established_date": sa.DateTime(),
                                                 "holding_company_type": sa.VARCHAR(25),
                                                 "bank_office_identifier": sa.INTEGER
                                          })

       # creating the primary key in the table:
       connection.execute("""
       ALTER TABLE dim_branch ALTER COLUMN branch_key SET NOT NULL
       """)   
       connection.execute("""
       ALTER TABLE dim_branch ADD PRIMARY KEY (branch_key)""")

       dim_bank.to_sql("dim_bank", connection, index=False, if_exists="replace", 
                                   dtype= {"bank_key": sa.INTEGER,
                                          "bank_name": sa.VARCHAR(105),
                                          "unique_bank_id_fed_board": sa.INTEGER,
                                          "bank_hq_name": sa.VARCHAR(120),
                                          "cityhcr": sa.VARCHAR(50),
                                          "bank_hq_state": sa.VARCHAR(4),
                                          "unique_bank_id_number": sa.INT,
                                          "address": sa.VARCHAR(100),
                                          "hq_city": sa.VARCHAR(50),
                                          "hq_zipcode": sa.INTEGER,
                                          "bank_class": sa.VARCHAR(4),
                                          "bank_category": sa.INTEGER,
                                          "bank_asset_classification": sa.INTEGER,
                                          "hq_state_name": sa.VARCHAR(50),
                                          "country_hq": sa.VARCHAR(50),
                                          "primary_industry_classification": sa.VARCHAR(50),
                                          "hq_state": sa.VARCHAR(4)
                                          })
       connection.execute("""
       ALTER TABLE dim_bank ALTER COLUMN bank_key SET NOT NULL
       """)
       connection.execute("""
       ALTER TABLE dim_bank ADD PRIMARY KEY (bank_key)""")

       dim_area_population.to_sql("dim_area_population", connection, index=False, if_exists="replace",
                                   dtype= {"area_population_key": sa.INTEGER,
                                          "county_2.5_million+_name": sa.VARCHAR(80),
                                          "urban_area_10000_under_50000": sa.INTEGER,
                                          "urban_area_50000+": sa.INTEGER
                                          }) 
       connection.execute("""
       ALTER TABLE dim_area_population ALTER COLUMN area_population_key SET NOT NULL
       """)
       connection.execute("""
       ALTER TABLE dim_area_population ADD PRIMARY KEY (area_population_key)""")

       dim_location.to_sql("dim_location", connection, index=False, if_exists="replace",
                                   dtype= {"location_key": sa.INTEGER,
                                          "branch_longitude": sa.INTEGER,
                                          "branch_latitude": sa.INTEGER,
                                          "in_usa": sa.INTEGER
                                          })
       connection.execute("""
       ALTER TABLE dim_location ALTER COLUMN location_key SET NOT NULL
       """)
       connection.execute("""
       ALTER TABLE dim_location ADD PRIMARY KEY (location_key)""")

       dim_assets.to_sql("dim_assets", connection, index=False, if_exists="replace",
                     dtype= {"assets_key": sa.INTEGER,
                            "total_assets": sa.BigInteger
                            })
       connection.execute("""
       ALTER TABLE dim_assets ALTER COLUMN assets_key SET NOT NULL
       """)
       connection.execute("""
       ALTER TABLE dim_assets ADD PRIMARY KEY (assets_key)""")

       dim_date.to_sql("dim_date", connection, index=False, if_exists="replace",
                     dtype= {"date_key": sa.INTEGER,
                            "year": sa.INTEGER
                            })
       connection.execute("""
       ALTER TABLE dim_date ALTER COLUMN date_key SET NOT NULL
       """)
       connection.execute("""
       ALTER TABLE dim_date ADD PRIMARY KEY (date_key)""")

       dim_charter.to_sql("dim_charter", connection, index=False, if_exists="replace",
                     dtype = {"charter_key": sa.INTEGER,
                            "charter_type": sa.VARCHAR(30),
                            "charter_agency_name": sa.VARCHAR(50),
                            "charter_abbreviation": sa.VARCHAR(30)})
       connection.execute("""
       ALTER TABLE dim_charter ALTER COLUMN charter_key SET NOT NULL
       """)
       connection.execute("""
       ALTER TABLE dim_charter ADD PRIMARY KEY (charter_key)""")

       dim_fdic.to_sql("dim_fdic", connection, index=False, if_exists="replace",
                     dtype= {"fdic_key": sa.INTEGER,
                                   "fdic_regional_office_id": sa.INTEGER,
                                   "fdic_regional_office_name": sa.VARCHAR(50),
                                   "fed_district_id": sa.INTEGER,
                                   "fed_district_name": sa.VARCHAR(50)})
       connection.execute("""
       ALTER TABLE dim_fdic ALTER COLUMN fdic_key SET NOT NULL
       """)
       connection.execute("""
       ALTER TABLE dim_fdic ADD PRIMARY KEY (fdic_key)""")

       dim_currency.to_sql("dim_currency", connection, index=False, if_exists="replace",
                     dtype = {"currency_key": sa.INTEGER,
                                   "currency_district_id": sa.FLOAT,
                                   "currency_district_name": sa.VARCHAR(80),
                                   "regulatory_agency_name": sa.VARCHAR(30)})
       connection.execute("""
       ALTER TABLE dim_currency ALTER COLUMN currency_key SET NOT NULL
       """)
       connection.execute("""
       ALTER TABLE dim_currency ADD PRIMARY KEY (currency_key)""")

       dim_deposit_code.to_sql("dim_deposit_code", connection, index=False, if_exists="replace", 
                            dtype= {"deposit_code_key": sa.INTEGER,
                                          "deposit_code": sa.VARCHAR(5)})
       connection.execute("""
       ALTER TABLE dim_deposit_code ALTER COLUMN deposit_code_key SET NOT NULL
       """)
       connection.execute("""
       ALTER TABLE dim_deposit_code ADD PRIMARY KEY (deposit_code_key)""")


