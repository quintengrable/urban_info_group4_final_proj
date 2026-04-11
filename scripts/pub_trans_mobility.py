import geopandas
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