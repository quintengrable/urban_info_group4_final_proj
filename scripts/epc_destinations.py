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

# %%
###########################################################
##########################################################
# Calculate the neighbors of epc tracts
#%%
epc_data.head()

#lodes_od_pairs.head()

#%%
#this will pull the data from the correct folder regardless of where cwd is
base_path = Path(__file__).parent.parent
pop_grid_path = base_path / "data" / "processed" / "day_night_pop_change.parquet"
#pull in tract polygon geometry data
population_grid = gpd.read_parquet(pop_grid_path)
#clean dataset to only relevant columns
population_grid = population_grid.drop(columns=['FUNCSTAT','INTPTLAT','INTPTLON','geoid','work_pop','home_pop','within_tract','resident_pop','daytime_pop'])

#%%
population_grid.head()

# %%
# create a function to find neighbors of EPCs that aren't EPCs

def get_neighboring_non_epcs(gdf:gpd.GeoDataFrame, epc_col:str='is_epc_2050', id_col:str='GEOID')->gpd.GeoDataFrame:
    # separate EPCs and non-EPCs
    epcs = gdf[gdf[epc_col] == True][[id_col, 'geometry']].copy()
    non_epcs = gdf[gdf[epc_col] == False][[id_col, 'geometry']].copy()
    
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
    
    return neighbors_gdf

#%%
neighbor_map_gdf = get_neighboring_non_epcs(population_grid)

#%%
import matplotlib.pyplot as plt

# 1. Run your function to get the neighbors
neighbor_map_gdf = get_neighboring_non_epcs(population_grid)

# 2. Setup the figure
fig, ax = plt.subplots(figsize=(12, 12))

# Layer 1: Plot ALL tracts in light gray as a background
population_grid.plot(ax=ax, color='whitesmoke', edgecolor='lightgray', linewidth=0.5)

# Layer 2: Plot the EPCs in a distinct color (e.g., Orange)
population_grid[population_grid['is_epc_2050'] == True].plot(
    ax=ax, color='orange', alpha=0.7, label='EPC Tracts'
)

# Layer 3: Plot your neighbors in a contrasting color (e.g., Blue)
neighbor_map_gdf.plot(
    ax=ax, color='royalblue', edgecolor='white', linewidth=1, label='Non-EPC Neighbors'
)

# Formatting
ax.set_title("Bay Area EPCs and their Non-EPC Neighbors", fontsize=15)
ax.set_axis_off() # Removes latitude/longitude coordinates for a cleaner look
plt.show()

#%%
# 1. Generate the neighbor GeoDataFrame using your function
neighbor_map_gdf = get_neighboring_non_epcs(population_grid)

# 2. Create the base map with the EPCs (The "Source")
# We'll color them orange to represent the priority areas
m = population_grid[population_grid['is_epc_2050'] == True].explore(
    color="orange",
    style_kwds={'fillOpacity': 0.5, 'color': 'black', 'weight': 1},
    name="EPC Tracts",
    tooltip=["GEOID", "is_epc_2050"] # Shows these details on hover
)

# 3. Add the Neighbors layer (The "Result")
# We'll use blue to highlight the tracts your function found
neighbor_map_gdf.explore(
    m=m, # This tells geopandas to draw on the same map 'm'
    color="royalblue",
    style_kwds={'fillOpacity': 0.7, 'color': 'white', 'weight': 2},
    name="Non-EPC Neighbors",
    tooltip=["GEOID_neighbor"]
)

# 4. Display the map
m