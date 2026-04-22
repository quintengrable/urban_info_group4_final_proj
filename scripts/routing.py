#%%
import pandas as pd
import os
from pathlib import Path
from helper_function import get_path
import osmnx as ox
import geopandas as gpd
import numpy as np
import pygris

bay_area_lodes = pd.read_parquet(get_path("processed", "bay_area_lodes_od_table.parquet"))
#%%
G_path = get_path('processed', 'G.graphml')
G = ox.load_graphml(G_path)

# %%
# run if you need to confirm if entire graph is connected 
#G_connected = ox.utils_graph.get_largest_component(G, strongly=True)
#ox.plot_graph(G_connected, node_size = 0)

# %%
ox.plot_graph(G, node_size = 0)
# %% 
tracts_gdf = pygris.tracts(state="CA", year=2020)
tracts_gdf['centroid'] = tracts_gdf.geometry.centroid
# %%
# create OD matrix 
tt = pd.read_parquet(get_path("processed", "epc_pubtrans_tt_mapping.parquet"))
tt.drop(columns = 'travel_time', inplace = True)
# %%
def compute_tt(row):
    try:
        return nx.shortest_path_length(
            G,
            source=row["from_id"],
            target=row["to_id"],
            weight="travel_time"
        )
    except:
        return np.nan  # unreachable

tt["travel_time_sec"] = tt.apply(compute_tt, axis=1)
tt["travel_time_min"] = tt["travel_time_sec"] / 60
# %%
tt.head()
# %%
