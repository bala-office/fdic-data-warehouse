import pandas as pd
import numpy as np

pd.set_option("max_rows", 25)
pd.set_option("max_columns", 80)

# reading in the 5 FDIC csv files and assigning them to dictionary keys.  
columns = [
    "ADDRESBR", 
    "ADDRESS", 
    "ASSET",
    "BKCLASS", 
    "BKMO", 
    "BRCENM",
    "BRNUM", 
    "BRSERTYP", 
    "CBSA_DIV_NAMB", 
    "CHARTER", 
    "CHRTAGNN",
    "CHRTAGNT", 
    "CITY", 
    "CITY2BR", 
    "CITYBR", 
    "CITYHCR",
    "CLCODE", 
    "CNTRYNA", 
    "CNTRYNAB", 
    "CNTYNAMB", 
    "CNTYNUMB",
    "DEPDOM", 
    "DEPSUM", 
    "DEPSUMBR", 
    "FDICDBS", 
    "FDICNAME", 
    "FED",
    "FEDNAME", 
    "HCTMULT", 
    "METROBR",
    "MICROBR", 
    "NAMEBR", 
    "NAMEFULL",
    "NAMEHCR", 
    "OCCDIST", 
    "OCCNAME", 
    "REGAGNT", 
    "RSSDHCR",
    "RSSDID", 
    "SIMS_ACQUIRED_DATE", 
    "SIMS_ESTABLISHED_DATE",
    "SIMS_LATITUDE", 
    "SIMS_LONGITUDE", 
    "SPECDESC", 
    "SPECGRP",
    "STALP", 
    "STALPBR", 
    "STALPHCR", 
    "STNAME", 
    "STNAMEBR", 
    "UNINUMBR",
    "UNIT", 
    "USA", 
    "YEAR", 
    "ZIP", 
    "ZIPBR",
]

df_dictionary = {}
for item in range(2016, 2021):
    df_dictionary["df_" + str(item)] = pd.read_csv(
        "data/All_" + str(item) + ".csv", 
        encoding="Latin-1", 
        usecols= lambda x: x.upper() in columns
    )

combined_df = pd.concat([df for df in df_dictionary.values()], ignore_index=True)
combined_df.columns = combined_df.columns.str.lower()

combined_df.rename(
    columns={
        "addresbr": "branch_address", 
        "asset":"total_assets", 
        "bkclass":"bank_class", 
        "bkmo": "bank_office_identifier",
        "brcenm":"deposit_code",
        "brnum" :"unique_bank_branch_id",
        "brsertyp":"branch_service_type",
        "cbsa_div_namb" : "county_2.5_million+_name",
        "chrtagnn":"charter_agency_name",
        "charter":"charter_type",
        "chrtagnt":"charter_abbreviation", 
        "city":"hq_city", 
        "city2br": "branch_city",
        "citybr": "branch_city_name",
        "clcode":"bank_category", 
        "cntryna":"country_hq",
        "cntrynab" : "branch_country_name",
        "cntynamb":"branch_county_name", 
        "cntynumb": "branch_county_number",
        "depdom":"total_domestic_deposits", 
        "depsum": "total_deposits",
        "depsumbr": "branch_office_deposits",
        "fdicdbs": "fdic_regional_office_id", 
        "fdicname": "fdic_regional_office_name",
        "fed": "fed_district_id", 
        "fedname": "fed_district_name",
        "hctmult": "holding_company_type",
        "insagnt1": "insurance_status",
        "insured" : "insured_category", 
        "metrobr" : "urban_area_50000+", 
        "microbr" : "urban_area_10000_under_50000",
        "namebr" : "branch_name", 
        "namefull" : "bank_name",
        "namehcr" : "bank_hq_name", 
        "occdist" : "currency_district_id", 
        "occname" : "currency_district_name", 
        "regagnt" : "regulatory_agency_name", 
        "rssdhcr" : "unique_bank_id_fed_board", 
        "rssdid" : "unique_bank_id_number", 
        "sims_acquired_date" : "acquisition_date", 
        "sims_established_date" : "branch_location_established_date",
        "sims_latitude" : "branch_latitude",
        "sims_longitude" : "branch_longitude", 
        "specdesc" : "primary_industry_classification", 
        "specgrp" : "bank_asset_classification_category", 
        "stalp" : "hq_state", 
        "stalpbr" : "branch_state",
        "stalphcr" : "bank_hq_state",
        "stname" : "hq_state_name", 
        "stnamebr": "branch_state_name", 
        "uninumbr" : "branch_unique_number", 
        "unit" : "only_main_office_no_branches", 
        "usa" : "in_usa", 
        "zip" : "hq_zipcode", 
        "zipbr" : "branch_zipcode",
    }, 
    inplace=True,
)

replace_columns = [
    "total_assets", 
    "total_domestic_deposits", 
    "branch_office_deposits", 
    "total_deposits"
]
for column in replace_columns:
    combined_df[column] = combined_df[column].str.replace(",", "")

category_columns = [
    "bank_class", 
    "branch_service_type", 
    "county_2.5_million+_name", 
    "charter_type", 
    "bank_category", 
    "fdic_regional_office_id", 
    "fdic_regional_office_name", 
    "fed_district_id", 
    "fed_district_name",
    "holding_company_type", 
    "urban_area_50000+", 
    "urban_area_10000_under_50000", 
    "currency_district_id", 
    "currency_district_name",
    "primary_industry_classification", 
    "bank_asset_classification_category", 
    "bank_hq_state", 
    "charter_agency_name", 
    "charter_abbreviation", 
    "regulatory_agency_name", 
    "deposit_code"
]

for column in category_columns:
    combined_df[column] = combined_df[column].astype("category")

combined_df = combined_df.astype(
    {
        "acquisition_date": "datetime64",
        "branch_location_established_date": "datetime64",
        "branch_county_number": "int32", 
        "branch_zipcode": "int32", 
        "total_deposits": "int64",
        "bank_office_identifier": "int8", 
        "only_main_office_no_branches": "float64",
        "in_usa": "int8", "total_assets": 
        "int64", "total_domestic_deposits": "int64",
        "branch_office_deposits": "int64",
    }
)
        
















