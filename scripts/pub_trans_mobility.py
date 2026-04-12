# %%
import geopandas as gpd
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
origins = population_grid.head(20).copy()
origins['id'] = origins['GEOID']
origins = origins.set_geometry('centroids')
origins.head(3)

#%%
#bay area test destination create tt matrix
destinations = ferry_building.copy()

travel_times = r5py.TravelTimeMatrix(
    transport_network,
    origins=origins,
    destinations=destinations,
    departure=dt.datetime(2026, 4, 14, 8, 30),
    transport_modes=[
        r5py.TransportMode.TRANSIT,
        r5py.TransportMode.WALK,
    ],
    snap_to_network=True,
)

#%%
travel_times.head(30)

#%%
#create a map of the travel times
travel_times_mapping = population_grid.merge(travel_times, left_on="GEOID", right_on="from_id")
travel_times_mapping.head()

#%%
#save tt matrix to processed data folder
pub_trans_tt_path = base_path / "data" / "processed" / "pub_trans_tt_mapping.csv"
travel_times_mapping.to_csv(pub_trans_tt_path)
#%%
#read in tt matrix so we don't have to run all the above calulcations again
pub_trans_tt_path = base_path / "data" / "processed" / "pub_trans_tt_mapping.csv"
travel_times_mapping1 = gpd.read_file(pub_trans_tt_path)

#%%
travel_times_mapping1.head()

#%%
travel_times_mapping.head()
#%%
#travel_times = travel_times.set_geometry('geometry') 

#%%
travel_times_mapping.explore("travel_time", cmap="Greens")