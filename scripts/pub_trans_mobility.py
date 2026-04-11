# %%
import geopandas
import r5py
import r5py.sampledata.helsinki
import shapely

population_grid = geopandas.read_file(r5py.sampledata.helsinki.population_grid)

railway_station = geopandas.GeoDataFrame(
    {
        "id": ["railway_station"],
        "geometry": [shapely.Point(24.94152, 60.17066)]
    },
    crs="EPSG:4326",
)

print("Successfully loaded population grid!")
print(population_grid.head())

overview_map = population_grid.explore("population", cmap="Reds")
overview_map = railway_station.explore(m=overview_map, marker_type="marker")
overview_map

# %%
transport_network = r5py.TransportNetwork(
    r5py.sampledata.helsinki.osm_pbf,
    [r5py.sampledata.helsinki.gtfs],
    r5py.sampledata.helsinki.elevation_model,
)

transport_network

# %%
print(transport_network)
# %%
import datetime

origins = population_grid.copy()
origins.geometry = origins.geometry.centroid

destinations = railway_station.copy()

travel_times = r5py.TravelTimeMatrix(
    transport_network,
    origins=origins,
    destinations=destinations,
    departure=datetime.datetime(2022, 2, 22, 8, 30),
    transport_modes=[
        r5py.TransportMode.TRANSIT,
        r5py.TransportMode.WALK,
    ],
    snap_to_network=True,
)

#%%
travel_times.head()