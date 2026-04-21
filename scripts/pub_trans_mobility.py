# %%
import geopandas as gpd
import pandas as pd
import r5py
import r5py.sampledata.helsinki
import shapely
import datetime as dt
from pathlib import Path

#%%
#this will pull the data from the correct folder regardless of where cwd is
base_path = Path(__file__).parent.parent
data_file = base_path / "data" / "processed" / "day_night_pop_change.parquet"

#%%
#pull in data from previous analyses
population_grid = gpd.read_parquet(data_file)
population_grid.head()

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
#testing with a landmark
ferry_building = gpd.GeoDataFrame(
    {
        "id": ["ferry_building"],
        "geometry": [shapely.Point(-122.3937, 37.7955)]
    },
    crs="EPSG:4326",
)

overview_map = population_grid.explore("total_pop", cmap="Reds")
overview_map = ferry_building.explore(m=overview_map, marker_type="marker")
overview_map

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
#bay area test destination create tt matrix
destinations = ferry_building.copy()

travel_times = r5py.TravelTimeMatrix(
    transport_network,
    origins=destinations,
    destinations=origins,
    departure=dt.datetime(2026, 4, 7, 8, 30),
    transport_modes=[
        r5py.TransportMode.TRANSIT,
        r5py.TransportMode.WALK,
    ],
    snap_to_network=True,
    max_time_walking=dt.timedelta(minutes=20),
    max_time=dt.timedelta(minutes=180),
    departure_time_window=dt.timedelta(minutes=10)
)

#%%
travel_times.head(100)

#%%
#create a map of the travel times
travel_times_mapping = population_grid.merge(travel_times, left_on="GEOID", right_on="from_id")
travel_times_mapping.head()

#%%
#destinations and origins swapped
travel_times_mapping = population_grid.merge(travel_times, left_on="GEOID", right_on="to_id")
travel_times_mapping.head()

#%%
#save tt matrix to processed data folder
pub_trans_tt_path = base_path / "data" / "processed" / "pub_trans_tt_mapping.parquet"
travel_times_mapping.to_parquet(pub_trans_tt_path)

#%%
#read in tt matrix so we don't have to run all the above calulcations again
base_path = Path(__file__).parent.parent
pub_trans_tt_path = base_path / "data" / "processed" / "pub_trans_tt_mapping.parquet"
travel_times_mapping1 = gpd.read_parquet(pub_trans_tt_path)

#%%
travel_times_mapping1.info()
travel_times_mapping1[travel_times_mapping1['travel_time'].notna()].count()

#%%
m = travel_times_mapping1.explore("travel_time", 
                                  cmap="Greens",
                                  tiles="CartoDB positron",
                                  )
m 
#travel_times_mapping1[travel_times_mapping1['travel_time'].notna()].explore("travel_time", cmap="Greens")
# %%
map_output_path = base_path / "visualizations" / "ferry_building_tt_map.html"
m.save(map_output_path)

#%%
################################################################################
################################################################################
#%%
#let's try with only epcs to see if computing is faster
(travel_times_mapping1[travel_times_mapping1['is_epc_2050']==True]).count()
#294 EPCs

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
epc_travel_times.info()

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
epc_odpairs.head()

#%%
# inner merge to filter travel time matrix to actual od pairs
epc_odpair_travel_times = epc_odpairs.merge(
    epc_travel_times, 
    left_on=['home_tract', 'work_tract'], 
    right_on=['from_id', 'to_id'], 
    how='inner'
)

#%%
epc_odpair_travel_times.info()

#%%
average_times = epc_odpair_travel_times.groupby('from_id')['travel_time'].mean().reset_index()

print(average_times)

#%%
average_times.info()
#%%
#create a map of the travel times
avg_epc_tt = epc_grid.merge(average_times, left_on="GEOID", right_on="from_id")
avg_epc_tt.head()

#%%
m = avg_epc_tt.explore("travel_time", 
                       cmap="Greens",
                       tiles="CartoDB positron",
                       )
m 
# %%
map_output_path = base_path / "visualizations" / "avg_epc_tt_map.html"
m.save(map_output_path)