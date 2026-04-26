#%%
import pandas as pd
import os
from pathlib import Path
from helper_function import get_path
import networkx as nx
import osmnx as ox
import geopandas as gpd
import numpy as np
import pygris

bay_area_lodes = pd.read_parquet(get_path("processed", "bay_area_lodes_od_table.parquet"))
#%%
G_path = get_path('processed', 'G.graphml')
G = ox.load_graphml(G_path)

# %%
G = ox.add_edge_speeds(G)
G = ox.add_edge_travel_times(G)
# %%
# run if you need to confirm if entire graph is connected 
#G_connected = ox.utils_graph.get_largest_component(G, strongly=True)
#ox.plot_graph(G_connected, node_size = 0)

# %%
ox.plot_graph(G, node_size = 0)
# %% 
tracts_gdf = pygris.tracts(state="CA", year=2020)
tracts_gdf['centroid'] = tracts_gdf.geometry.centroid
tracts_gdf.head()
# %%
# create OD matrix 
tt = pd.read_parquet(get_path("processed", "epc_pubtrans_tt_mapping.parquet"))

# merge: origin centroid
tracts_gdf.rename(columns={'GEOID': 'from_id', 'centroid': 'from_centroid'}, inplace = True)
tt = pd.merge(
    tt,
    tracts_gdf[['from_id', 'from_centroid']],
    on = "from_id", 
    how = "left"
    )

# merge: destination centroid 
tracts_gdf.rename(columns={'from_id': 'to_id', 'from_centroid': 'to_centroid'}, inplace = True)
tt = pd.merge(
    tt,
    tracts_gdf[['to_id', 'to_centroid']],
    on = "to_id", 
    how = "left"
    )

tt.drop(columns = 'travel_time', inplace = True)
tt.head()
# %%
# snap centroids to node on graph 

from_geoseries = gpd.GeoSeries(tt['from_centroid'])
to_geoseries = gpd.GeoSeries(tt['to_centroid'])

# origins: 
tt['from_node'] = ox.distance.nearest_nodes(
    G, 
    X=from_geoseries.x, 
    Y=from_geoseries.y
)

# destinations:
tt['to_node'] = ox.distance.nearest_nodes(
    G, 
    X=to_geoseries.x, 
    Y=to_geoseries.y
)
# %%
def compute_tt(row):
    try:
        return nx.shortest_path_length(
            G,
            source=row["from_node"],
            target=row["to_node"],
            weight="travel_time"
        )
    except:
        return np.nan  # unreachable

tt["travel_time_sec"] = tt.apply(compute_tt, axis=1)
tt["travel_time_min"] = tt["travel_time_sec"] / 60
# %%
def debug_tt(row):
    try:
        return nx.shortest_path_length(
            G,
            source=row["from_node"],
            target=row["to_node"],
            weight="travel_time"
        )
    except Exception as e:
        return type(e).__name__  # Returns the name of the error (e.g., 'NodeNotFound')

# Run this temporarily to see the errors
error_counts = tt.apply(debug_tt, axis=1).value_counts()
print(error_counts)
tt.head()
# %%
