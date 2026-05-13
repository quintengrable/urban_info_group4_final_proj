# %%
import geopandas as gpd
import pandas as pd
import r5py
import shapely
import datetime as dt
from pathlib import Path
import folium
import matplotlib.pyplot as plt

#%%
#this will pull the data from the correct folder regardless of where cwd is
base_path = Path(__file__).parent.parent
data_file = base_path / "data" / "processed" / "day_night_pop_change.parquet"
#pull in population data from previous analyses
population_grid = gpd.read_parquet(data_file)

#%%
#clean dataset to only relevant columns
population_grid = population_grid.drop(columns=['FUNCSTAT','INTPTLAT','INTPTLON','geoid','work_pop','home_pop','within_tract','resident_pop','daytime_pop'])
population_grid.head()

#%%
#get centroids
population_grid = population_grid.to_crs(epsg=2227)
population_grid['centroids'] = population_grid.geometry.centroid
population_grid = population_grid.set_geometry('geometry')
population_grid = population_grid.to_crs(epsg=4326)

#%%
#creating a bay area transport network
bay_area_pbf = base_path / "data" / "raw" / "san-francisco-bay_california.osm.pbf"
bay_area_gtfs = base_path / "data" / "raw" / "GTFSTransitData_RG.zip"

transport_network = r5py.TransportNetwork(
    bay_area_pbf,
    [bay_area_gtfs],
    allow_errors=True
)

transport_network

#%%
#bay area origins
origins = population_grid.copy()
origins['id'] = origins['GEOID']
origins = origins[['id', 'centroids']].copy()
origins = origins.set_geometry('centroids')

#%%
################################################################################
################################################################################
# create tt matrix for EPCs
#%%
epc_grid = population_grid[population_grid['is_epc_2050']==True].copy().reset_index().drop(columns="index")
epc_grid.info()

#%%
#epc origins
epc_origins = epc_grid.copy()
epc_origins['id'] = epc_origins['GEOID']
epc_origins = epc_origins[['id', 'centroids']].copy()
epc_origins = epc_origins.set_geometry('centroids')

#%%
#destinations
destinations = population_grid.copy()
destinations['id'] = destinations['GEOID']
destinations = destinations[['id', 'centroids']].copy()
destinations = destinations.set_geometry('centroids')
destinations.count()

#%%
#EPC tt matrix

epc_travel_times = r5py.TravelTimeMatrix(
    transport_network,
    origins=epc_origins,
    destinations=destinations,
    departure=dt.datetime(2026, 4, 7, 8, 30),
    transport_modes=[
        r5py.TransportMode.TRANSIT,
        r5py.TransportMode.WALK,
    ],
    snap_to_network=True,
    max_time_walking=dt.timedelta(minutes=20),
    max_time=dt.timedelta(minutes=120),
    departure_time_window=dt.timedelta(minutes=10)
)

#%%
#save epc tt matrix
epc_pubtrans_tt_path = base_path / "data" / "processed" / "epc_pubtrans_tt_mapping.parquet"
epc_travel_times.to_parquet(epc_pubtrans_tt_path)

#%%
# read in epc tt matrix so we don't have to rerun computation
base_path = Path(__file__).parent.parent
epc_pubtrans_tt_path = base_path / "data" / "processed" / "epc_pubtrans_tt_mapping.parquet"
epc_travel_times = pd.read_parquet(epc_pubtrans_tt_path)

#%%
#filter matrix for actual od pairs below!!

#%%
# read in epc od pairs
base_path = Path(__file__).parent.parent
epc_odpairs_path = base_path / "data" / "processed" / "epc_odpairs.parquet"
epc_odpairs = gpd.read_parquet(epc_odpairs_path)

#%%
# inner merge to filter travel time matrix to actual od pairs
epc_odpair_travel_times = epc_odpairs.merge(
    epc_travel_times, 
    left_on=['home_tract', 'work_tract'], 
    right_on=['from_id', 'to_id'], 
    how='inner'
)

#%%
average_times = epc_odpair_travel_times.groupby('from_id')['travel_time'].mean().reset_index()

#%%
#create a map of the travel times
avg_epc_tt = epc_grid.merge(average_times, left_on="GEOID", right_on="from_id")
avg_epc_tt.head()

#%%
avg_epc_tt['travel_time'] = avg_epc_tt['travel_time'].round(2)
avg_epc_tt.head()
#%%
m = avg_epc_tt.explore("travel_time", 
                       cmap="Greens",
                       tiles="CartoDB positron",
                       tooltip=["travel_time","GEOID","total_pop"]
                       )
m 
# %%
map_output_path = base_path / "visualizations" / "avg_epc_tt_map.html"
m.save(map_output_path)

#%%
#######################################################################
#######################################################################
# now create tt matric for non-EPC neighbors of EPCs
#%%
# read in neighbor tracts
base_path = Path(__file__).parent.parent
neighbor_tracts_path = base_path / "data" / "processed" / "neighbor_tracts.parquet"
neighbor_tracts = gpd.read_parquet(neighbor_tracts_path)

#%%
#get centroids of neighbor tracts
neighbor_tracts = neighbor_tracts.to_crs(epsg=2227)
neighbor_tracts['centroids'] = neighbor_tracts.geometry.centroid
neighbor_tracts = neighbor_tracts.set_geometry('geometry')
neighbor_tracts = neighbor_tracts.to_crs(epsg=4326)

#%%
# non-epc neighbor origins
neighbor_tract_origins = neighbor_tracts.copy()
neighbor_tract_origins['id'] = neighbor_tract_origins['GEOID_neighbor']
neighbor_tract_origins = neighbor_tract_origins[['id', 'centroids']].copy()
neighbor_tract_origins = neighbor_tract_origins.set_geometry('centroids')

#%%
#destinations
destinations = population_grid.copy()
destinations['id'] = destinations['GEOID']
destinations = destinations[['id', 'centroids']].copy()
destinations = destinations.set_geometry('centroids')

#%%
# neighbors tt matrix

neighbor_travel_times = r5py.TravelTimeMatrix(
    transport_network,
    origins=neighbor_tract_origins,
    destinations=destinations,
    departure=dt.datetime(2026, 4, 7, 8, 30),
    transport_modes=[
        r5py.TransportMode.TRANSIT,
        r5py.TransportMode.WALK,
    ],
    snap_to_network=True,
    max_time_walking=dt.timedelta(minutes=20),
    max_time=dt.timedelta(minutes=120),
    departure_time_window=dt.timedelta(minutes=10)
)

#%%
#save epc tt matrix
neighbor_pubtrans_tt_path = base_path / "data" / "processed" / "neighbor_pubtrans_tt_mapping.parquet"
neighbor_travel_times.to_parquet(neighbor_pubtrans_tt_path)

#%%
# read in epc tt matrix so we don't have to rerun computation
base_path = Path(__file__).parent.parent
neighbor_pubtrans_tt_path = base_path / "data" / "processed" / "neighbor_pubtrans_tt_mapping.parquet"
neighbor_travel_times = pd.read_parquet(neighbor_pubtrans_tt_path)

#%%
############################################################################
#filter matrix for actual od pairs below!!

#%%
# read in neighbor od pairs
base_path = Path(__file__).parent.parent
neighbor_odpairs_path = base_path / "data" / "processed" / "neighbor_odpairs.parquet"
neighbor_odpairs = pd.read_parquet(neighbor_odpairs_path)

#%%
# inner merge to filter travel time matrix to actual od pairs
neighbor_odpair_travel_times = neighbor_odpairs.merge(
    neighbor_travel_times, 
    left_on=['home_tract', 'work_tract'], 
    right_on=['from_id', 'to_id'], 
    how='inner'
)

#%%
neighbor_average_times = neighbor_odpair_travel_times.groupby('from_id')['travel_time'].mean().reset_index()

#%%
#create a map of the travel times
avg_neighbor_tt = neighbor_tracts.merge(neighbor_average_times, left_on="GEOID_neighbor", right_on="from_id")

#%%
avg_neighbor_tt['travel_time'] = avg_neighbor_tt['travel_time'].round(2)

#%%
# drop rows where travel time was zero
avg_neighbor_tt = avg_neighbor_tt[avg_neighbor_tt['travel_time'] != 0]

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
m = avg_epc_tt.explore(
    location=[37.6, -122.2],    #Wylie added in to set the initial map view
    zoom_start=9,   #Wylie added in to set the initial map view
    column="travel_time", 
    cmap="Purples",
    tooltip=["travel_time", "GEOID", "total_pop"],
    name="EPC Tracts",      # layer label
    tiles="CartoDB positron",
    legend_kwds={"caption": "EPC Travel Time (min)"}
    # style_kwds={
    #     "color": "black",
    #     "weight": 1,
    #     "opacity": 1
    # }
)

m = avg_neighbor_tt.explore(
    location=[37.6, -122.2],    #Wylie added in to set the initial map view
    zoom_start=9,   #Wylie added in to set the initial map view
    column="travel_time", 
    cmap="Oranges",
    tiles="CartoDB positron",
    tooltip=["travel_time", "GEOID_neighbor", "total_pop"],
    name="Neighbor Tracts", # layer label
    m=m,
    legend_kwds={"caption": "Neighbor Travel Time (min)"}                    
    # style_kwds={
    #     "color": "black",
    #     "weight": 1,
    #     "opacity": 1
    # }
)

# toggle layers
folium.LayerControl().add_to(m)
m
# save map
map_output_path = base_path / "visualizations" / "avg_epc_neigh_tt_map.html"
m.save(map_output_path)

#Wylie added in to also save map to docs\page_assets 
map_output_path = base_path / "docs" / "page_assets" / "avg_epc_neigh_tt_map.html"
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
    'EPC AvgTT by Transit': epc_county_avg,
    'Neighbor AvgTT by Transit': neigh_county_avg
})

county_tt_plot['county_name'] = county_tt_plot.index.map(bay_geocode_map)

print(county_tt_plot.head(10))

#%%
county_tt_plot.set_index('county_name').plot(kind='barh', figsize=(12, 8))

plt.title('Transit Travel Time Comparison by County')
plt.xlabel('Average Travel Time')
plt.ylabel('County')
plt.legend(loc='lower right', frameon=True, framealpha=1)
plt.tight_layout()

save_path = base_path / "visualizations" / "county_avg_tt.png"
plt.savefig(save_path, dpi=300, bbox_inches='tight')

save_path = base_path / "docs" / "page_assets" / "county_avg_tt.png"
plt.savefig(save_path, dpi=300, bbox_inches='tight')
plt.show()
#%%
