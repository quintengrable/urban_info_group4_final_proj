# %%
import geopandas as gpd
import pandas as pd
import r5py
#import r5py.sampledata.helsinki
import shapely
import datetime as dt
from pathlib import Path
import folium
import matplotlib.pyplot as plt
import logging
logging.basicConfig(level=logging.INFO)

#%%
base_path = Path(__file__).parent.parent
data_file = base_path / "data" / "processed" / "day_night_pop_change.parquet"
#pull in population data from previous analyses
population_grid = gpd.read_parquet(data_file)
population_grid = population_grid.drop(columns=['FUNCSTAT','INTPTLAT','INTPTLON','geoid','work_pop','home_pop','within_tract','resident_pop','daytime_pop'])
population_grid.head()

#get centroids
population_grid = population_grid.to_crs(epsg=2227)
population_grid['centroids'] = population_grid.geometry.centroid
population_grid = population_grid.set_geometry('geometry')

population_grid = population_grid.to_crs(epsg=4326)

#%%
#creating a bay area transport network
bay_area_pbf = base_path / "data" / "raw" / "san-francisco-bay_california.osm.pbf"
transport_network = r5py.TransportNetwork(
    bay_area_pbf,
    allow_errors=True
)

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

# %% 
# logger for debug
r5py_logger = logging.getLogger("r5py")
r5py_logger.setLevel(logging.INFO)
#%%
#EPC tt matrix

epc_travel_times = r5py.TravelTimeMatrix(
    transport_network,
    origins=destinations,
    destinations=epc_origins,
    departure=dt.datetime(2026, 4, 7, 8, 30),
    transport_modes=[
        r5py.TransportMode.CAR,
    ],
    snap_to_network=True,
    max_time=dt.timedelta(minutes=120)
)

#%%
epc_travel_times.info()
epc_travel_times.head(10)
#%%
#save epc tt matrix
epc_car_tt_path = base_path / "data" / "processed" / "epc_car_tt_mapping.parquet"
epc_travel_times.to_parquet(epc_car_tt_path)

#%%
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
destinations.count()

#%%
# neighbors tt matrix

neighbor_travel_times = r5py.TravelTimeMatrix(
    transport_network,
    origins=destinations,
    destinations=neighbor_tract_origins,
    departure=dt.datetime(2026, 4, 7, 8, 30),
    transport_modes=[
        r5py.TransportMode.CAR,
    ],
    snap_to_network=True,
    max_time=dt.timedelta(minutes=120)
)

#%%
#neighbor_travel_times.info()
travel_times.head()
#%%
#save epc tt matrix
neighbor_car_tt_path = base_path / "data" / "processed" / "neighbor_car_tt_mapping.parquet"
neighbor_travel_times.to_parquet(neighbor_car_tt_path)