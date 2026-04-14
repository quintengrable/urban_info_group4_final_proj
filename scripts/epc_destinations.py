#%%
import geopandas as gpd
import pandas as pd
import r5py
import r5py.sampledata.helsinki
import shapely
import datetime as dt
from pathlib import Path

#%%
# read in LODES pairs
base_path = Path(__file__).parent.parent
lodes_od_path = base_path / "data" / "processed" / "bay_area_lodes_od_table.parquet"
lodes_od_pairs = pd.read_parquet(lodes_od_path)

#%%
lodes_od_pairs.head()

# %%
# read in EPC data
base_path = Path(__file__).parent.parent
epc_data_path = base_path / "data" / "processed" / "day_night_pop_change.parquet"
epc_data = gpd.read_parquet(epc_data_path)

#%%
epc_data.head()

# %%
# clean epc data
epc_data = epc_data[epc_data['is_epc_2050']==True].copy().reset_index()
epc_data = epc_data.drop(columns=['index','daytime_pop','resident_pop','total_pop','within_tract','home_pop','work_pop','geoid','INTPTLON','INTPTLAT','FUNCSTAT'])
epc_data.head()

# %%
# clean lodes data
lodes_od_pairs = lodes_od_pairs.drop(columns=['SA01','SA02','SA03','SE01','SE02','SE03','SI01','SI02','SI03','index','w_geocode','h_geocode','work_county','home_county'])
lodes_od_pairs.head()

#%%
# merge epc data with lodes od pairs
epc_od_pairs = lodes_od_pairs.merge(epc_data, left_on="home_tract", right_on="GEOID")
epc_od_pairs = epc_od_pairs.drop(columns=['is_epc_2050']) # all rows are epcs now

#%%
epc_od_pairs.head()
epc_od_pairs = gpd.GeoDataFrame(epc_od_pairs, geometry='geometry', crs="EPSG:4326")

#%%
# save the data so it can be used in the pub_trans_mobility file
base_path = Path(__file__).parent.parent
epc_odpairs_path = base_path / "data" / "processed" / "epc_odpairs.parquet"
epc_od_pairs.to_parquet(epc_odpairs_path)

#%%
##########################################################
##########################################################
# top 5 tracts commuted to for each EPC
#%%
# Sum commuters for each OD pair
od_counts = epc_od_pairs.groupby(['home_tract', 'work_tract'])['S000'].sum().reset_index()

# Sort by home_tract (ascending) and commuters (descending)
od_counts = od_counts.sort_values(['home_tract', 'S000'], ascending=[True, False])

# Calculate total commuters per home tract
total_home = od_counts.groupby('home_tract')['S000'].transform('sum')

# Calculate percentage of total tract outflow
od_counts['pct_of_home_total'] = (od_counts['S000'] / total_home) * 100

# Get the top 5 work tracts for each home tract
top_5_commute = od_counts.groupby('home_tract').head(3)

print(top_5_commute)
top_5_commute.head(21)
