# urban_info_group4_final_proj
Final project repository for Group 4 for Urban Informatics and Visualization

This project takes LODES OD Pair Data and ACS Census Tract Population Data and uses it to learn about mobility insights of Bay Area Commuters. There is a special
emphasis on equity priority communities (EPCs) and calculating if the locations they commute to work by car and public transit take longer than the residents of neighboring non-EPC tracts.

# Data
Please note that our project has several files larger than the 50 MB limit to upload to GitHub. Monica has directed us to make a note of that here and to include those large files in our final submission ZIP folder. 

When retrieving ACS data, an API key is necessary, available here: https://api.census.gov/data/key_signup.html

This project did not automate the retrieval of the MTC GTFS files. Sign up for you own API key and get your GTFS files here:
https://511.org/open-data/token

This project did not automate the retrieval of the OSM Extracts for the Bay Area. Sign up for your own API key and get your extract here: https://app.interline.io/osm_extracts/interactive_view

Both the OSM file and the GTFS files are necessary to create the transport network when working with R5Py. Routing calculations cannot be performed without obtaining this data first.

# Project Structure
* `/data`: Contains raw and processed data files necessary for the analysis
* `/notebooks`: 
    * `data_aquisition.ipynb`: Acquires the ACS data, LODES data, and MTC data
    * `day_night_pop.ipynb`: Performs initial analysis on daytime and nighttime population of Bay Area
    * `summary_statistics_visualization.ipynb`: 
* `/scripts`:
    * `car_mobility.py`: using R5PY to route cars and get travel times
    * `epc_destinations.py`: Harmonizing LODES OD pairs with EPCs and EPC neighbors
    * `get_graph.py`: 
    * `helper_function.py`: helper functions to aid in development
    * `pub_trans_mobility.py`: using R5PY to route public transit and get travel times
    * `routing.py`: 
* `/visualizations`: All the visualization outputs from the project

1. Start with running the data_aquisition notebook, this will let you get the datasets for LODES8 2023, ACS, and MTC's EPC data for the Bay Area. 
2. Follow up by running the rest of the python notebooks
3. Run the epc_destinations script, this will initilize data for the R5PY routing
4. Run the pub_trans_mobility and car_mobility scripts.
