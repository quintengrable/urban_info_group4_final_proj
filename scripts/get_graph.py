# this script downloads maps from OSMnx and saves the graph. 
# run once, run again if changes to graph is needed 

import numpy as np
import osmnx as ox
import networkx as nx
from helper_function import get_path

bay_area_counties = [
    'Alameda County, California',
    'Contra Costa County, California',
    'Marin County, California',
    'Napa County, California',
    'San Francisco County, California',
    'San Mateo County, California',
    'Santa Clara County, California',
    'Solano County, California',
    'Sonoma County, California'
]
graphs = {}

print('downloading')
G = ox.graph_from_place(bay_area_counties, network_type="drive", which_result=None)

ox.save_graphml(G, get_path("processed", "G.graphml"))
print(f'Saved to: {get_path("processed", "G.graphml")}')