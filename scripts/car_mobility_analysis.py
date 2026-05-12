# %%
import geopandas as gpd
import pandas as pd
import shapely
import datetime as dt
from pathlib import Path
import folium
import matplotlib.pyplot as plt
import logging


# some naming convention notes: 
# xxx_xxx_tt_matrix - refers to matrix of all OD pairs given by r5
# xxx_xxx_tt - refers to matrix filtered to where origins actually go, based on LODES data
#%%
# read in epc tt matrix - from all epc origins to all destinations 
base_path = Path(__file__).parent.parent
epc_car_tt_path = base_path / "data" / "processed" / "epc_car_tt_mapping.parquet"
epc_car_tt_matrix = pd.read_parquet(epc_car_tt_path)

#%%
# filter for od pairs
base_path = Path(__file__).parent.parent
epc_odpairs_path = base_path / "data" / "processed" / "epc_odpairs.parquet"
epc_odpairs = gpd.read_parquet(epc_odpairs_path)

# inner merge to filter travel time matrix to actual od pairs
epc_car_tt = epc_odpairs.merge(
    epc_car_tt_matrix, 
    left_on=['home_tract', 'work_tract'], 
    right_on=['from_id', 'to_id'], 
    how='inner'
)

# clean df
epc_car_tt.drop(columns = ["geometry", "GEOID", 'within_tract', 'home_tract', 'work_tract', 'S000'], inplace = True)
epc_car_tt.rename(columns = {'travel_time': "car_travel_time"}, inplace = True)
epc_car_tt.sample(10)

#%% 
#compare to transit travel times 
#read in transit travel times 
base_path = Path(__file__).parent.parent
epc_pubtrans_tt_path = base_path / "data" / "processed" / "epc_pubtrans_tt_mapping.parquet"
epc_transit_tt = pd.read_parquet(epc_pubtrans_tt_path)
epc_transit_tt.rename(columns = {'travel_time': 'transit_travel_time'}, inplace = True)
epc_transit_tt.head()
#%%
# travel time by od pair, car and transit 
epc_travel_times = pd.merge(
    epc_car_tt,
    epc_transit_tt,
    on = ['from_id', 'to_id'],
    how = 'inner'
)

epc_travel_times.sample(10)
#%%
# average travel time by origin destination
average_times_car = epc_car_tt.groupby('from_id')['car_travel_time'].mean().reset_index()
average_times_transit = epc_transit_tt.groupby('from_id')['transit_travel_time'].mean().reset_index()

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

avg_epc_tt_car = epc_grid.merge(average_times, left_on="GEOID", right_on="from_id")
avg_epc_tt_car['car_travel_time'] = avg_epc_tt_car['car_travel_time'].round(2)
avg_epc_tt_car.head()

m = avg_epc_tt_car.explore("car_travel_time", 
                       cmap="Greens",
                       tiles="CartoDB positron",
                       tooltip=["car_travel_time","GEOID","total_pop"]
                       )

map_output_path = base_path / "visualizations" / "car_avg_epc_tt_map.html"
m.save(map_output_path)

###########################################################################
# repeat analysis for neighbours - od pair travel times, avg travel times, plots
#%%
# get neighbour travel times for od pairs and average neighbor travel times
base_path = Path(__file__).parent.parent
file_path = base_path / "data" / "processed" / "neighbor_car_tt_mapping.parquet"
neighbor_car_tt_matrix = pd.read_parquet(file_path)

base_path = Path(__file__).parent.parent
neighbor_odpairs_path = base_path / "data" / "processed" / "neighbor_odpairs.parquet"
neighbor_odpairs = pd.read_parquet(neighbor_odpairs_path)

# inner merge to filter travel time matrix to actual od pairs
neighbor_car_tt = neighbor_odpairs.merge(
    neighbor_car_tt_matrix, 
    left_on=['home_tract', 'work_tract'], 
    right_on=['from_id', 'to_id'], 
    how='inner'
)

#clean filtered travel time matrix 
#neighbor_car_tt.drop(columns = ['S000', 'work_tract', 'home_tract'], inplace = True)
neighbor_car_tt.rename(columns = {'travel_time':'car_travel_time'}, inplace = True)
neighbor_car_tt.head(10)

neighbor_average_times = neighbor_car_tt.groupby('from_id')['car_travel_time'].mean().reset_index().round(2)
#neighbor_average_times = neighbor_car_tt.groupby('from_id')['transit_travel_time'].mean().reset_index().round(2)

neighbor_average_times.head()

#%%
# get geometry for neighbors
base_path = Path(__file__).parent.parent
neighbor_tracts_path = base_path / "data" / "processed" / "neighbor_tracts.parquet"
neighbor_tracts = gpd.read_parquet(neighbor_tracts_path)

neighbor_tracts = neighbor_tracts.to_crs(epsg=2227)
neighbor_tracts['centroids'] = neighbor_tracts.geometry.centroid
neighbor_tracts = neighbor_tracts.set_geometry('geometry')
neighbor_tracts = neighbor_tracts.to_crs(epsg=4326)
neighbor_tracts.head()

#%%
# merge travel times with geometry
avg_neighbor_tt_car = neighbor_tracts.merge(neighbor_average_times, left_on="GEOID_neighbor", right_on="from_id")
avg_neighbor_tt_car['car_travel_time'] = avg_neighbor_tt_car['car_travel_time'].round(2)
# drop rows where travel time was zero
avg_neighbor_tt_car = avg_neighbor_tt_car[avg_neighbor_tt_car['car_travel_time'] != 0]
avg_neighbor_tt_car.head()
#%%
# plot car travel time for neighbours only
m = avg_neighbor_tt.explore("car_travel_time", 
                       cmap="Oranges",
                       tiles="CartoDB positron",
                       tooltip=["car_travel_time","GEOID_neighbor","total_pop"]
                       )
m 
# %%
map_output_path = base_path / "visualizations" / "car_avg_neighbor_tt_map.html"
m.save(map_output_path)
# %%
###########################################################################
# plot car travel time for EPCs and neighbors together
m = avg_neighbor_tt_car.explore(
    column="car_travel_time", 
    cmap="Oranges",
    tiles="CartoDB positron",
    tooltip=["car_travel_time", "GEOID_neighbor", "total_pop"],
    name="Neighbor Tracts", # layer label
)

m = avg_epc_tt_car.explore(
    column="car_travel_time", 
    cmap="Greens",
    tooltip=["car_travel_time", "GEOID", "total_pop"],
    m=m,                    
    name="EPC Tracts",      # layer label
)

# toggle layers
folium.LayerControl().add_to(m)

m

# save map
map_output_path = base_path / "visualizations" / "car_avg_epc_neigh_tt_map.html"
m.save(map_output_path)

#%%
path = base_path / "data" / "processed" / "avg_epc_tt_transit.parquet"
avg_epc_tt_transit = gpd.read_parquet(path)

path = base_path / "data" / "processed" / "avg_neighbor_tt_transit.parquet"
avg_neighbor_tt_transit = gpd.read_parquet(path)
avg_neighbor_tt_transit.head()


#%% 
##############################################################################
# map: transit and car times for epcs and neighbours, 2 layers
# had some issues with legend showing up so needed to create legend manually
import branca.colormap as cm
for map_name in dir(cm.linear):
    print(map_name)
#%%
m = folium.Map(location=[37.6, -122.2], zoom_start=9, tiles="CartoDB positron")

# groups
car_group = folium.FeatureGroup(name="Car Travel Mode").add_to(m)
transit_group = folium.FeatureGroup(name="Transit Travel Mode", show=False).add_to(m)

# neighbours - oranges
avg_neighbor_tt_car.explore(
    column="car_travel_time", cmap="Greens", m=car_group, legend=False,
    tooltip=["car_travel_time", "GEOID_neighbor", "total_pop"]
)
# car neighbours legend
car_neigh_min = avg_neighbor_tt_car["car_travel_time"].min()
car_neigh_max = avg_neighbor_tt_car["car_travel_time"].max()
car_neigh_legend = cm.linear.Greens_09.scale(car_neigh_min, car_neigh_max)
car_neigh_legend.caption = "Car Travel Time, Neighbours (mins)"
m.add_child(car_neigh_legend)

# epc - purples
avg_epc_tt_car.explore(
    column="car_travel_time", cmap="Blues", m=car_group, legend=False,
    tooltip=["car_travel_time", "GEOID", "total_pop"]
)
# car epc legend
car_epc_min = avg_epc_tt_car["car_travel_time"].min()
car_epc_max = avg_epc_tt_car["car_travel_time"].max()
car_epc_legend = cm.linear.Blues_09.scale(car_epc_min, car_epc_max)
car_epc_legend.caption = "Car Travel Time, EPCs (mins)"
m.add_child(car_epc_legend)

# neighbours - oranges
avg_neighbor_tt_transit.explore(
    column="travel_time", cmap="Oranges", m=transit_group, legend=False,
    tooltip=["travel_time", "GEOID_neighbor", "total_pop"]
)
# transit neighbours legend
tran_neigh_min = avg_neighbor_tt_transit["travel_time"].min()
tran_neigh_max = avg_neighbor_tt_transit["travel_time"].max()
tran_neigh_legend = cm.linear.Oranges_09.scale(tran_neigh_min, tran_neigh_max)
tran_neigh_legend.caption = "Transit Travel Time, Neighbours (mins)"
m.add_child(tran_neigh_legend)

# epc - purples 
avg_epc_tt_transit.explore(
    column="travel_time", cmap="Purples", m=transit_group, legend=False,
    tooltip=["travel_time", "GEOID", "total_pop"]
)
# transit epc legend
tran_epc_min = avg_epc_tt_transit["travel_time"].min()
tran_epc_max = avg_epc_tt_transit["travel_time"].max()
tran_epc_legend = cm.linear.Purples_09.scale(tran_epc_min, tran_epc_max)
tran_epc_legend.caption = "Transit Travel Time, EPCs (mins)"
m.add_child(tran_epc_legend)

folium.LayerControl(collapsed=False).add_to(m)

map_output_path = base_path / "visualizations" / "combined_travel_time_map.html"
m.save(map_output_path)
m
# %%
#################################################################
# create bar graph of travel times by car for epcs and neighbours by county
#%%
# get average travel times 
avg_neighbor_tt_all = avg_neighbor_tt['car_travel_time'].mean().round(2)
avg_epc_tt_all = avg_epc_tt['car_travel_time'].mean().round(2)

# group and map by county
bay_area_geocodes = {'06001': 'Alameda',
                     '06013': 'Contra Costa',
                     '06041': 'Marin',
                     '06055': 'Napa',
                     '06075': 'San Francisco',
                     '06081': 'San Mateo',
                     '06085': 'Santa Clara',
                     '06095': 'Solano',
                     '06097': 'Sonoma',
                     }

neigh_tt_by_county = avg_neighbor_tt.copy()
neigh_tt_by_county['county'] = neigh_tt_by_county['GEOID_neighbor'].str[:5]
neigh_county_avg = neigh_tt_by_county.groupby('county')['car_travel_time'].mean().copy()

epc_tt_by_county = avg_epc_tt.copy()
epc_tt_by_county['county'] = epc_tt_by_county['GEOID'].str[:5]
epc_county_avg = epc_tt_by_county.groupby('county')['car_travel_time'].mean().copy()

# combine epc and neighbours averages
county_tt_plot = pd.DataFrame({
    'EPC AvgTT by Car': epc_county_avg,
    'Neighbors AvgTT by Car': neigh_county_avg
})

county_tt_plot['county_name'] = county_tt_plot.index.map(bay_area_geocodes)

county_tt_plot.set_index('county_name').plot(kind='barh', figsize=(12, 8))

plt.title('Car Travel Time Comparison by County between EPCs and Neighbours')
plt.xlabel('Average Travel Time')
plt.ylabel('County')
plt.legend()
plt.tight_layout()

save_path = base_path / "visualizations" / "county_avg_tt.png"
plt.savefig(save_path, dpi=300, bbox_inches='tight')


plt.show()
#%%
############################################################################
# create bar graph of car vs transit times, avg for each county

avg_epc_tt
# %%
average_times['county'] = average_times['from_id'].str[:5]
average_times_by_county = average_times.groupby('county')[['car_travel_time', 'transit_travel_time']].mean().round(2).copy()
average_times_by_county['county_name'] = average_times_by_county.index.map(bay_area_geocodes)

average_times_by_county

average_times_by_county.set_index('county_name').plot(kind='barh', figsize=(12, 8), color = ['#8C149C', '#2E8A27'])

plt.title('Travel Time Comparison by County between Car and Transit')
plt.xlabel('Average Travel Time')
plt.colorbar
plt.ylabel('County')
plt.legend()
plt.tight_layout()

save_path = base_path / "visualizations" / "county_avg_car_vs_transit.png"
plt.savefig(save_path, dpi=300, bbox_inches='tight')


plt.show()
# %%
