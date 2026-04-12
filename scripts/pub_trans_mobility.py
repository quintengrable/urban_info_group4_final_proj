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
population_grid1 = gpd.read_parquet(data_file)
population_grid1.head()

#%%
#clean dataset to only relevant columns
population_grid1 = population_grid1.drop(columns=['FUNCSTAT','INTPTLAT','INTPTLON','geoid','work_pop','home_pop','within_tract','resident_pop','daytime_pop'])
population_grid1.head()

#%%
#get centroids
population_grid1 = population_grid1.to_crs(epsg=2227)
population_grid1['centroids'] = population_grid1.geometry.centroid
population_grid1 = population_grid1.set_geometry('geometry')

population_grid1 = population_grid1.to_crs(epsg=4326)

#%%
#testing with a landmark
ferry_building = gpd.GeoDataFrame(
    {
        "id": ["ferry_building"],
        "geometry": [shapely.Point(-122.3937, 37.7955)]
    },
    crs="EPSG:4326",
)

overview_map1 = population_grid1.explore("total_pop", cmap="Reds")
overview_map1 = ferry_building.explore(m=overview_map1, marker_type="marker")
overview_map1

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
origins1 = population_grid1.head(20).copy()
origins1['id'] = origins1['GEOID']
origins1 = origins1.set_geometry('centroids')
origins1.head(3)

#%%
#bay area test destination
destinations1 = ferry_building.copy()

travel_times = r5py.TravelTimeMatrix(
    transport_network,
    origins=origins1,
    destinations=destinations1,
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
