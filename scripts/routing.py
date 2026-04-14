#%%
import pandas as pd
import os
from pathlib import Path
from helper_function import get_path
import osmnx as ox
# folder = "processed"
# filename = "test"
# parent_path = Path.cwd().parent/ ("data/" + folder) # test 
# print("Data directory:", parent_path.resolve())
# file_path = os.path.join(parent_path, filename)
# print(file_path)
print(get_path("processed", "bay_area_lodes_od_table.parquet"))
#bay_area_lodes = pd.read_parquet(get_path("processed", "bay_area_lodes_od_table.parquet"))
#%%
G_path = get_path('processed', 'G.graphml')
G = ox.load_graphml(G_path)
