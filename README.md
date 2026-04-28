# urban_info_group4_final_proj
Final project repository for Group 4 for Urban Informatics and Visualization

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
