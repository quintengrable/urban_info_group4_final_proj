import geopandas
import shapely
import r5py  # Just the main module

# In v1.1.3, sample data is accessed via r5py.sampledata.<function>
# No separate import is required.
population_grid_path = r5py.sampledata.helsinki_population_grid()
population_grid = geopandas.read_file(population_grid_path)

railway_station = geopandas.GeoDataFrame(
    {
        "id": ["railway_station"],
        "geometry": [shapely.Point(24.94152, 60.17066)]
    },
    crs="EPSG:4326",
)

print("Successfully loaded population grid!")
print(population_grid.head())