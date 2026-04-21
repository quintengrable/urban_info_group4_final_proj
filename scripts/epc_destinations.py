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

# %%
# read in EPC data
base_path = Path(__file__).parent.parent
epc_data_path = base_path / "data" / "processed" / "day_night_pop_change.parquet"
epc_data = gpd.read_parquet(epc_data_path)

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

# %%
###########################################################
##########################################################
# Calculate the neighbors of epc tracts
#%%
base_path = Path(__file__).parent.parent
pop_grid_path = base_path / "data" / "processed" / "day_night_pop_change.parquet"
#pull in tract polygon geometry data
population_grid = gpd.read_parquet(pop_grid_path)
#clean dataset to only relevant columns
population_grid = population_grid.drop(columns=['FUNCSTAT','INTPTLAT','INTPTLON','geoid','work_pop','home_pop','within_tract','resident_pop','daytime_pop'])

# %%
# create a function to find neighbors of EPCs that aren't EPCs

def get_neighboring_non_epcs(gdf:gpd.GeoDataFrame, epc_col:str='is_epc_2050', id_col:str='GEOID')->gpd.GeoDataFrame:
    # separate EPCs and non-EPCs
    epcs = gdf[gdf[epc_col] == True][[id_col, 'geometry']].copy()
    non_epcs = gdf[(gdf[epc_col] == False) & (gdf['total_pop'] > 0)][[id_col, 'geometry', 'total_pop']].copy()    
    # spatial join to find non-EPCs that neighbor EPCs
    neighbors_gdf = gpd.sjoin(
        non_epcs, 
        epcs[[id_col, 'geometry']], 
        how='inner', 
        predicate='touches',
        lsuffix='neighbor', 
        rsuffix='epc'
    )
    
    # remove duplicates
    neighbors_gdf = neighbors_gdf.drop_duplicates(subset=[f'{id_col}_neighbor'])
    neighbors_gdf = neighbors_gdf.reset_index().drop(columns=['index_epc','GEOID_epc','index'])
    
    return neighbors_gdf

#%%
neighbor_map_gdf = get_neighboring_non_epcs(population_grid)

#%%
# mapping neighbor non-epc tracts to verify function worked

neighbor_map_gdf = get_neighboring_non_epcs(population_grid)

m = population_grid[population_grid['is_epc_2050'] == True].explore(
    color="orange",
    style_kwds={'fillOpacity': 0.5, 'color': 'black', 'weight': 1},
    name="EPC Tracts",
    tooltip=["GEOID", "is_epc_2050"]
)

neighbor_map_gdf.explore(
    m=m,
    color="royalblue",
    style_kwds={'fillOpacity': 0.7, 'color': 'white', 'weight': 2},
    name="Non-EPC Neighbors",
    tooltip=["GEOID_neighbor"]
)

m

map_output_path = base_path / "visualizations" / "qg_testing.html"
m.save(map_output_path)

#%%
# merge neighbor non-epc data with lodes od pairs
neighbor_od_pairs = lodes_od_pairs.merge(neighbor_map_gdf, left_on="home_tract", right_on="GEOID_neighbor")
neighbor_od_pairs = gpd.GeoDataFrame(neighbor_od_pairs, geometry='geometry', crs="EPSG:4326")

#%%
# save the data so it can be used in the pub_trans_mobility file
base_path = Path(__file__).parent.parent
neighbor_odpairs_path = base_path / "data" / "processed" / "neighbor_odpairs.parquet"
neighbor_od_pairs.to_parquet(neighbor_odpairs_path)
# %%
