# this script downloads maps from OSMnx and saves the graph. 
# run once, run again if changes to graph is needed 

import numpy as np
import osmnx as ox
import networkx as nx
from helper_function import get_path

bay_area_counties = ['Alameda', 'Contra Costa', 'Marin', 'Napa', 'San Francisco', 'San Mateo', 'Santa Clara', 'Solano', 'Sonoma']
graphs = {}

for county in bay_area_counties: 
    pass_name = county + ' County, California, USA'
    print(f"Downloading: {pass_name}")
    graphs[county] = ox.graph_from_place((pass_name), network_type = "drive")

G = nx.compose_all(list(graphs.values()))
ox.save_graphml(G, get_path("processed", "G.graphml"))
print(f'Saved to: {get_path("processed", "G.graphml")}')