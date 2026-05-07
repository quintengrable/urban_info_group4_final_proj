# %%
import geopandas as gpd
import pandas as pd
import shapely
import datetime as dt
from pathlib import Path
import folium
import matplotlib.pyplot as plt
import logging

#%%
# read in epc tt matrix - from all epc origins to all destinations 
base_path = Path(__file__).parent.parent
epc_car_tt_path = base_path / "data" / "processed" / "epc_car_tt_mapping.parquet"
epc_car_travel_times = pd.read_parquet(epc_car_tt_path)

#%%
#filter matrix for actual od pairs below!!

#%%
# read in epc od pairs
base_path = Path(__file__).parent.parent
epc_odpairs_path = base_path / "data" / "processed" / "epc_odpairs.parquet"
epc_odpairs = gpd.read_parquet(epc_odpairs_path)

#%%
epc_odpairs.head()

#%% 
#compare to transit travel times 

#read in transit travel times 
base_path = Path(__file__).parent.parent
epc_pubtrans_tt_path = base_path / "data" / "processed" / "epc_pubtrans_tt_mapping.parquet"
epc_transit_travel_times = pd.read_parquet(epc_pubtrans_tt_path)
epc_transit_travel_times.rename(columns = {'travel_time': 'transit_travel_time'}, inplace = True)
epc_transit_travel_times.head()
#%%
# inner merge to filter travel time matrix to actual od pairs
epc_odpair_car_travel_times = epc_odpairs.merge(
    epc_car_travel_times, 
    left_on=['home_tract', 'work_tract'], 
    right_on=['from_id', 'to_id'], 
    how='inner'
)

# clean df
epc_odpair_car_travel_times.drop(columns = ["geometry", "GEOID", 'within_tract', 'home_tract', 'work_tract', 'S000'], inplace = True)
epc_odpair_car_travel_times.rename(columns = {'travel_time': "car_travel_time"}, inplace = True)
#%%
epc_odpair_car_travel_times.sample(10)

#%%
# merge travel time matrices 
epc_travel_times = pd.merge(
    epc_odpair_car_travel_times,
    epc_transit_travel_times,
    on = ['from_id', 'to_id'],
    how = 'inner'
)

epc_travel_times.sample(10)
#%%
# average travel time by origin destination
average_times_car = epc_odpair_car_travel_times.groupby('from_id')['car_travel_time'].mean().reset_index()
average_times_transit = epc_transit_travel_times.groupby('from_id')['transit_travel_time'].mean().reset_index()

average_times = pd.merge(
    average_times_transit, 
    average_times_car, 
    on = "from_id",
    how = "inner"
)
print(average_times.info())
average_times.sample(10)
#%%
# map: average car travel times for epc tracts
base_path = Path(__file__).parent.parent
data_file = base_path / "data" / "processed" / "day_night_pop_change.parquet"
#pull in population data from previous analyses
population_grid = gpd.read_parquet(data_file)
epc_grid = population_grid[population_grid['is_epc_2050']==True].copy().reset_index().drop(columns="index")

avg_epc_tt = epc_grid.merge(average_times, left_on="GEOID", right_on="from_id")
avg_epc_tt['car_travel_time'] = avg_epc_tt['car_travel_time'].round(2)
avg_epc_tt.head()

m = avg_epc_tt.explore("car_travel_time", 
                       cmap="Greens",
                       tiles="CartoDB positron",
                       tooltip=["car_travel_time","GEOID","total_pop"]
                       )

map_output_path = base_path / "visualizations" / "car_avg_epc_tt_map.html"
m.save(map_output_path)

###########################################################################
#%%
# neighbour travel times
base_path = Path(__file__).parent.parent
neighbor_pubtrans_tt_path = base_path / "data" / "processed" / "neighbor_car_tt_mapping.parquet"
car_neighbor_travel_times = pd.read_parquet(neighbor_pubtrans_tt_path)

base_path = Path(__file__).parent.parent
neighbor_odpairs_path = base_path / "data" / "processed" / "neighbor_odpairs.parquet"
neighbor_odpairs = pd.read_parquet(neighbor_odpairs_path)

neighbor_odpairs.head()
car_neighbor_travel_times.head()
#%%
# inner merge to filter travel time matrix to actual od pairs
neighbor_odpair_travel_times = neighbor_odpairs.merge(
    neighbor_travel_times, 
    left_on=['home_tract', 'work_tract'], 
    right_on=['from_id', 'to_id'], 
    how='inner'
)

#%%
neighbor_odpair_travel_times.info()

#%%
neighbor_average_times = neighbor_odpair_travel_times.groupby('from_id')['travel_time'].mean().reset_index()

print(neighbor_average_times)

#%%
neighbor_average_times.info()

#%%
neighbor_tracts.head()
#%%
#create a map of the travel times
avg_neighbor_tt = neighbor_tracts.merge(neighbor_average_times, left_on="GEOID_neighbor", right_on="from_id")
avg_neighbor_tt.head()
#%%
avg_neighbor_tt['travel_time'] = avg_neighbor_tt['travel_time'].round(2)
avg_neighbor_tt.info()

#%%
# drop rows where travel time was zero
avg_neighbor_tt = avg_neighbor_tt[avg_neighbor_tt['travel_time'] != 0]

#%%
avg_neighbor_tt.info()

#%%
m = avg_neighbor_tt.explore("travel_time", 
                       cmap="Oranges",
                       tiles="CartoDB positron",
                       tooltip=["travel_time","GEOID_neighbor","total_pop"]
                       )
m 
# %%
map_output_path = base_path / "visualizations" / "avg_neighbor_tt_map.html"
m.save(map_output_path)
# %%
###########################################################################
# plot EPCs and neighbors together
m = avg_neighbor_tt.explore(
    column="travel_time", 
    cmap="Oranges",
    tiles="CartoDB positron",
    tooltip=["travel_time", "GEOID_neighbor", "total_pop"],
    name="Neighbor Tracts", # layer label
)

m = avg_epc_tt.explore(
    column="travel_time", 
    cmap="Greens",
    tooltip=["travel_time", "GEOID", "total_pop"],
    m=m,                    
    name="EPC Tracts",      # layer label
)

# toggle layers
folium.LayerControl().add_to(m)

m

# save map
map_output_path = base_path / "visualizations" / "avg_epc_neigh_tt_map.html"
m.save(map_output_path)
# %%
#################################################################
#%%
avg_neighbor_tt_all = avg_neighbor_tt['travel_time'].mean()
avg_neighbor_tt_all = avg_neighbor_tt_all.round(2)
avg_neighbor_tt_all

# %%
avg_epc_tt_all = avg_epc_tt['travel_time'].mean()
avg_epc_tt_all = avg_epc_tt_all.round(2)
avg_epc_tt_all

#%%
# compare average mobility per county
neigh_tt_by_county = avg_neighbor_tt.copy()
neigh_tt_by_county['county'] = neigh_tt_by_county['GEOID_neighbor'].str[:5]
neigh_county_avg = neigh_tt_by_county.groupby('county')['travel_time'].mean().copy()
neigh_county_avg

#%%
epc_tt_by_county = avg_epc_tt.copy()
epc_tt_by_county['county'] = epc_tt_by_county['GEOID'].str[:5]
epc_county_avg = epc_tt_by_county.groupby('county')['travel_time'].mean().copy()
epc_county_avg

#%%
# dictionary of Bay Area counties
bay_geocode_map = {'06001': 'Alameda',
                     '06013': 'Contra Costa',
                     '06041': 'Marin',
                     '06055': 'Napa',
                     '06075': 'San Francisco',
                     '06081': 'San Mateo',
                     '06085': 'Santa Clara',
                     '06095': 'Solano',
                     '06097': 'Sonoma',
                     }


county_tt_plot = pd.DataFrame({
    'EPC Average': epc_county_avg,
    'Neighbor Average': neigh_county_avg
})

county_tt_plot['county_name'] = county_tt_plot.index.map(bay_geocode_map)

print(county_tt_plot.head(10))

#%%
county_tt_plot.set_index('county_name').plot(kind='barh', figsize=(12, 8))

plt.title('Travel Time Comparison by County')
plt.xlabel('Average Travel Time')
plt.ylabel('County')
plt.legend()
plt.tight_layout()

save_path = base_path / "visualizations" / "county_avg_tt.png"
plt.savefig(save_path, dpi=300, bbox_inches='tight')

plt.show()
#%%