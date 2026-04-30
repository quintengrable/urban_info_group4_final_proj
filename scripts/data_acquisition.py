#installing & importing
# !pip install -r requirements.txt
import pandas as pd
import geopandas as gpd
from census import Census

#### GETTING AND CLEANING ACS DATA

#set up API key. This is our API key, update with your own before running
api_key = 'd4d5470c09f30e27329890124dea292c7da295b7'
c = Census(key=api_key)

#Create a dictionary for the ACS table columns we want to pull in - commute times, and label the columns what we want
vars = {
    'NAME': 'NAME',                 #always want to bring in the name and geoID columns
    'GEO_ID': 'GEO_ID',
    'B01003_001E': 'total_pop',     #from a different table, getting the full tract populations
    'B01003_001M': 'total_pop_moe',
    'B08302_001E': 'total_commuters',         #this is total number of commuters aged 16+
    'B08302_001M': 'total_commuters_moe',     #moe = margin of error, we might want to use this for any stats analysis later
    'B08302_002E': '00:00_05:00',
    'B08302_002M': '00:00_05:00_moe',
    'B08302_003E': '05:00_05:30',
    'B08302_003M': '05:00_05:30_moe',
    'B08302_004E': '05:30_06:00',
    'B08302_004M': '05:30_06:00_moe',
    'B08302_005E': '06:00_06:30',
    'B08302_005M': '06:00_06:30_moe',
    'B08302_006E': '06:30_07:00',
    'B08302_006M': '06:30_07:00_moe',
    'B08302_007E': '07:00_07:30',
    'B08302_007M': '07:00_07:30_moe',
    'B08302_008E': '07:30_08:00',
    'B08302_008M': '07:30_08:00_moe',
    'B08302_009E': '08:00_08:30',
    'B08302_009M': '08:00_08:30_moe',
    'B08302_010E': '08:30_09:00',
    'B08302_010M': '08:30_09:00_moe',
    'B08302_011E': '09:00_10:00',
    'B08302_011M': '09:00_10:00_moe',
    'B08302_012E': '10:00_11:00',
    'B08302_012M': '10:00_11:00_moe',
    'B08302_013E': '11:00_12:00',
    'B08302_013M': '11:00_12:00_moe',
    'B08302_014E': '12:00_16:00',
    'B08302_014M': '12:00_16:00_moe',
    'B08302_015E': '16:00_00:00',
    'B08302_015M': '16:00_00:00_moe'
}

#load the raw data
departure_times_raw = pd.DataFrame(
    c.acs5.get( #api call to get the data, acs5 corresponds to the 5-year estimates
        list(vars.keys()), #use the dictionary keys to get the ACS columns we want
        {'for': 'tract', # all tracts
         'in': 'state:06 county:001,013,041,055,075,081,085,095,097'}, # California = 06, 9 Bay Area counties
        year=2023 #2019-2023
    ))

#Save the raw data
departure_times_raw.to_csv("data/raw/acs5_2023_Bay_Area_departure_times_raw.csv",index=False)

#Starting to clean the ACS data
#load data with renamed columns
departure_times = pd.DataFrame(
    c.acs5.get( #api call to get the data, acs5 corresponds to the 5-year estimates
        list(vars.keys()), #use the dictionary keys to get the ACS columns we want
        {'for': 'tract', # all tracts
         'in': 'state:06 county:001,013,041,055,075,081,085,095,097'}, # California = 06, 9 Bay Area counties
        year=2023 #2019-2023
    )).rename(columns=vars)

#convert data types
departure_times_cleaned = departure_times.convert_dtypes()

#creating a new column for geoid
departure_times_cleaned["GEOID"] = departure_times_cleaned["GEO_ID"].str[-11:]
departure_times_cleaned.drop("GEO_ID", axis = 1, inplace = True)

#Now to save the cleaned ACS data
departure_times_cleaned.to_csv("data/processed/acs5_2023_Bay_Area_departure_times_cleaned.csv",index=False)

import sys
import os
from pathlib import Path

parent_path = Path(__file__).parent.parent / "data/processed"
filename = "acs5_2023_Bay_Area_departure_times_cleaned.csv"
file_path = parent_path / filename
departure_times_cleaned = pd.read_csv(file_path)


#### GETTING & CLEANING LODES8 DATA

import pygris        # package designed to simplify accessing and loading US Census Bureau data
from pygris.data import get_lodes

lodes8_2023_od = get_lodes(state = "CA", year = 2023, lodes_type = "od") # create dataframe by pulling CA 2023 OD LODES data

# save LODES raw data as a parquet file:
# variable containing path string and name
lodes8_2023_od_table_path = "data/lodes8_2023_od_table_raw.parquet"
# saving LODES raw data
lodes8_2023_od.to_parquet(lodes8_2023_od_table_path)

# load in raw data from saved parquet file
lodes8_2023_od = pd.read_parquet("data/lodes8_2023_od_table_raw.parquet")

parent_path = Path(__file__).parent.parent / "data/raw"
filename = "lodes8_2023_od_table_raw.parquet"
file_path = parent_path / filename
lodes8_2023_od = pd.read_parquet(file_path)

#Cleaning the LODES8 data

# we will need to filter the CA data by these codes to get only the data for these 9 counties
bay_area_geocodes = ['06001', # Alameda
                     '06013', # Contra Costa
                     '06041', # Marin
                     '06055', # Napa
                     '06075', # SF
                     '06081', # San Mateo
                     '06085', # Santa Clara
                     '06095', # Solano
                     '06097'  # Sonoma
                     ]

# casting the two parts of the OD pair as a string just in case it wasn't already that
lodes8_2023_od['w_geocode'] = lodes8_2023_od['w_geocode'].astype(str)
lodes8_2023_od['h_geocode'] = lodes8_2023_od['h_geocode'].astype(str)

# creating two new columns in the dataframe to slice out the state and county part of the work and home geocodes for future filtering
lodes8_2023_od['work_county'] = lodes8_2023_od['w_geocode'].str[:5]
lodes8_2023_od['home_county'] = lodes8_2023_od['h_geocode'].str[:5]

#filter for the bay area
bay_area_lodes = lodes8_2023_od[(lodes8_2023_od['work_county'].isin(bay_area_geocodes)) | (lodes8_2023_od['home_county'].isin(bay_area_geocodes))].copy()

# creating two new columns in the dataframe to slice out the tract part of the work and home geocodes for future filtering
bay_area_lodes['work_tract'] = bay_area_lodes['w_geocode'].str[:11]
bay_area_lodes['home_tract'] = bay_area_lodes['h_geocode'].str[:11]

#Processing LODES data for plots 
parent_path = Path(__file__).parent.parent / "data/processed"
filename = "bay_area_lodes_od_table.parquet"
file_path = parent_path / filename
bay_area_lodes = pd.read_parquet(file_path)

#Determine if home and work tracts are the same
bay_area_lodes["within_tract"] = bay_area_lodes["work_tract"] == bay_area_lodes["home_tract"]

#Group by home and work tracts
tract_pop = bay_area_lodes.groupby('work_tract')['S000'].sum().to_frame().rename_axis("GEOID")
tract_pop = tract_pop.rename(columns = {"S000": "work_pop"}).rename_axis("GEOID")
tract_pop["home_pop"] = bay_area_lodes.groupby("home_tract")["S000"].sum()

#Find number of within tract movements
#Determine if home and work tracts are the same
bay_area_lodes["within_tract"] = bay_area_lodes["work_tract"] == bay_area_lodes["home_tract"]
#group and sum
tract_pop["within_tract"] = bay_area_lodes.groupby('work_tract')['within_tract'].sum()

#### MTC EPC DATA

parent_path = Path(__file__).parent.parent / "data/raw"
filename = "EPC_2020_acs2018.geojson"
file_path = parent_path / filename
epc = gpd.read_file(file_path)

#drop unnecessary columns
cols_to_drop = ['state_fip','county_fip', 'tract','tot_pop_ci','tot_pop_ov', 'pop_over75', 'pop_poc', 'pop_spfam', 'pop_lep',
                'pop_below2', 'pop_disabi', 'pop_hus_re','over75_1_2', 'poc_1_2', 'spfam_1_2', 'lep_1_2','below2_1_2',
                'disab_1_2', 'hus_re_1_2', 'zvhh_1_2', 'pct_over75', 'pct_poc', 'pct_spfam', 'pct_lep', 'pct_below2',
                'pct_disab', 'pct_hus_re',]
epc.drop(columns = cols_to_drop, inplace = True)

#rename columns
epc.rename(columns = {'tot_pop_po': 'total_pop_income', 'tot_hh': 'total_households', 'tot_fam': 'total_families',
       'pop_zvhhs': 'pop_zero_veh', 'pct_zvhhs': 'pct_zero_veh',}, inplace = True)


#### CLEAN BAY AREA CENSUS TRACT GEOMETRIES

#read California census tracts
#data from: https://www.census.gov/cgi-bin/geo/shapefiles/index.php, year 2022
parent_path = Path(__file__).parent.parent / "data/raw"
filename = "tl_2022_06_tract/tl_2022_06_tract.shp"
file_path = parent_path / filename
california = gpd.read_file(file_path)

#change the crs
california.to_crs(4326, inplace = True)

# Bay area county codes:
# 001 ALAMEDA
# 013 CONTRA COSTA
# 041 MARIN
# 055 NAPA
# 075 SAN FRANCISCO
# 081 SAN MATEO
# 085 SANTA CLARA
# 095 SOLANO
# 097 SONOMA
#filter:
county_codes = ['001', '013', '041', '055', '075', '081', '085', '095', '097']
bay_counties = california[california["COUNTYFP"].isin(county_codes)]

#saving the reprojected & cleaned bay counties to a parquet file
bay_counties.to_parquet("data/bay_counties.parquet")
bay_counties.to_parquet("data/processed/bay_counties_cleaned.parquet")