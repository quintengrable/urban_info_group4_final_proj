#%%
#### IMPORTS
import pandas as pd
import geopandas as gpd
import sys
import os
from pathlib import Path
import matplotlib.pyplot as plt
#%%
#### LOADING FILES

#loading in the acs data
# parent_path = Path.cwd().parent / "data/processed"
# filename = "acs5_2023_Bay_Area_departure_times_cleaned.csv"
# file_path = os.path.join(parent_path, filename)
# departure_times_cleaned = pd.read_csv(file_path)
base_path = Path(__file__).parent.parent
file_path = base_path / "data" / "processed" / "acs5_2023_Bay_Area_departure_times_cleaned.csv"
departure_times_cleaned = pd.read_csv(file_path)

#loading in the lodes data
# parent_path = Path.cwd().parent / "data/processed"
# filename = "tract_pop.parquet"
# file_path = os.path.join(parent_path, filename)
# tract_pop = pd.read_parquet(file_path)
base_path = Path(__file__).parent.parent
file_path = base_path / "data" / "processed" / "tract_pop.parquet"
tract_pop = pd.read_parquet(file_path)

#loading in the EPC data
# parent_path = Path.cwd().parent / "data/raw"
# filename = "EPC_2020_acs2018.geojson"
# file_path = os.path.join(parent_path, filename)
# epc = gpd.read_file(file_path)
base_path = Path(__file__).parent.parent
file_path = base_path / "data" / "raw" / "EPC_2020_acs2018.geojson"
epc = gpd.read_file(file_path)

#loading in the Bay counties geometries
# parent_path = Path.cwd().parent / "data/processed"
# filename = 'bay_counties_cleaned.parquet'
# file_path = os.path.join(parent_path, filename)
# bay_counties = gpd.read_parquet(file_path)
base_path = Path(__file__).parent.parent
file_path = base_path / "data" / "processed" / "bay_counties_cleaned.parquet"
bay_counties = gpd.read_parquet(file_path)

#### MERGING AND FILTERING DATA

#merging bay_counties and epc data
epc_relevant_columns = ['geoid', 'epc_2050']

bay_counties = bay_counties.merge(
    epc[epc_relevant_columns], #only merge the relevant columns
    left_on="GEOID",
    right_on="geoid",
    how="left"
)

#adding a boolean column, True = tract is EPC, False = tract is not EPC
bay_counties["epc_2050"] = bay_counties["epc_2050"].notna()

#rename to make it clear it's a bool
bay_counties = bay_counties.rename(columns={"epc_2050": "is_epc_2050"})

#%%
#saving the merged gdf, called bay_tracts_with_epcs.parquet
base_path = Path(__file__).parent.parent
file_path = base_path / "data" / "processed" / "bay_tracts_with_epcs.parquet"
bay_counties.to_parquet(file_path)

#merge on GEOID and work_tract columns, for daytime populations.
bay_counties = pd.merge(
    bay_counties, tract_pop,
    how = "left",
    left_on = "GEOID",
    right_index = True
    )

#re-cleaning the departure time acs data - seems like datatypes get changed when saved to csv, so this is to be safe
cols_to_str = ['state','county','tract','GEOID']    #columns that should be strings

departure_times_cleaned[cols_to_str] = departure_times_cleaned[cols_to_str].astype(str)

#original integer dtype got rid of the leading zero on GEOID, add back in:
departure_times_cleaned["GEOID"] = departure_times_cleaned["GEOID"].str.zfill(11)

#merging bay_counties with the acs departure time data
#add a new column called Base_Pop: match GEOID, use total commuter pop from departure_times_cleaned
bay_counties = pd.merge(
    bay_counties, departure_times_cleaned[["GEOID", "total_pop","total_commuters"]],
    how = "left",
    on = "GEOID"
)

#rename columns and add total daytime population
bay_counties = bay_counties.rename(columns = {"total_commuters": "resident_pop"})

#### PLOTTING PERCENTAGE OF POPULATION WHO ARE COMMUTERS

#making a list of the columns we don't need
cols_to_drop = ["STATEFP", "COUNTYFP", "TRACTCE", "NAME", "NAMELSAD", "MTFCC", 'ALAND', 'AWATER', 'geoid', 'work_pop', 'home_pop', 'within_tract']

#make a copy of gdf with only necessary cols
commuter_pcts = bay_counties.drop(columns = cols_to_drop)

#calculating the percentage of total population
commuter_pcts['commuter_pct'] = 100* commuter_pcts['resident_pop'] / commuter_pcts['total_pop']

#creating commuter map!
import mapping_functions    #custom functions made to map our data

custom_bins = [10, 20, 25, 30, 35, 40, 45, 50, 60]  #bins for the colorscale

commuter_pct_map = mapping_functions.plt_gdf_cmap_outlinegeos(
    gdf=commuter_pcts,
    cmap_col= "commuter_pct",
    colormap= "Greens",
    cmap_scale_bins= custom_bins,
    cmap_scale_label= "Percentage of Population who are Workers",
    cmap_scale_max= custom_bins[-1],
    cmap_scale_min= 0,
    tooltip_cols= "commuter_pct",
    cmap_layer_name= "Worker Percentage",
    outline_col= "is_epc_2050",
    outline_color= "#FF0090",
    outline_name= "EPC tracts (2050)"
)

#saving the map to visualization folder
base_path = Path(__file__).parent.parent
map_output_path = base_path / "visualizations" / "pct_commuter_pop.html"
commuter_pct_map.save(map_output_path)

#### PLOTTING DAYTIME & NIGHTIME COMMUTER POPULATIONS

#drop the unnecessary columns, create column of daytime population
bay_counties_plot = bay_counties.drop(columns = ["STATEFP", "COUNTYFP", "TRACTCE", "NAME", "NAMELSAD", "MTFCC", 'ALAND', 'AWATER'])
bay_counties_plot["daytime_pop"] = bay_counties_plot["resident_pop"] + bay_counties_plot["work_pop"] - bay_counties_plot["within_tract"]

#%%
#saving the final gdf as a parquet
base_path = Path(__file__).parent.parent
file_path = base_path / "data/processed" / "day_night_pop_change.parquet"
bay_counties_plot.to_parquet(file_path)

#creating daytime map!
custom_bins = [3000, 6000, 9000, 12000, 15000, 20000, 25000, 30000, 40000, 50000]

daytime_map = mapping_functions.plt_gdf_cmap_outlinegeos(
    gdf=bay_counties_plot,
    cmap_col= "daytime_pop",
    colormap= "Oranges",
    cmap_scale_bins= custom_bins,
    cmap_scale_label= "Daytime Worker Population",
    cmap_scale_max= 50000,
    cmap_scale_min= 0,
    tooltip_cols= "daytime_pop",
    cmap_layer_name= "Daytime Worker Population",
    outline_col= "is_epc_2050",
    outline_color= "blue",
    outline_name= "EPC tracts (2050)"
)

#saving the map to visualization folder
base_path = Path(__file__).parent.parent
map_output_path = base_path / "visualizations" / "daytime_pop.html"
daytime_map.save(map_output_path)

#creating nighttime map!
custom_bins = [500, 1000, 1500, 2000, 2500, 3000, 3500]

nighttime_map = mapping_functions.plt_gdf_cmap_outlinegeos(
    gdf=bay_counties_plot,
    cmap_col= "resident_pop",
    colormap= "Purples",
    cmap_scale_bins= custom_bins,
    cmap_scale_label= "Nighttime Worker Population",
    cmap_scale_max= custom_bins[-1],
    cmap_scale_min= 0,
    tooltip_cols= "resident_pop",
    cmap_layer_name= "Nighttime Worker Population",
    outline_col= "is_epc_2050",
    outline_color= "#FFB700",
    outline_name= "EPC tracts (2050)"
)

#saving the map to visualization folder
base_path = Path(__file__).parent.parent
map_output_path = base_path / "visualizations" / "nighttime_pop.html"
nighttime_map.save(map_output_path)

#### PLOTTING NET NIGHT TO DAY CHANGE IN POPULATION OF COMMUTERS

#calculating the net change
bay_counties_plot["net_worker_change"] = bay_counties_plot["work_pop"] - bay_counties_plot["resident_pop"]

#making the map!
custom_bins = [-5000, -4000, -3000, -2000, -1000, 0, 1000, 2000, 3000, 4000, 5000]

net_change_map = mapping_functions.plt_gdf_cmap_outlinegeos(
    gdf=bay_counties_plot,
    cmap_col= "net_worker_change",
    colormap= "PuOr_r",
    cmap_scale_bins= custom_bins,
    cmap_scale_label= "Net Worker Change From Night to Day",
    cmap_scale_max= 6000,
    cmap_scale_min= -6000,
    tooltip_cols= ["net_worker_change"],
    cmap_layer_name= "Net Daily Change of Workers",
    outline_col= "is_epc_2050",
    outline_color= "blue",
    outline_name= "EPC tracts (2050)"
)

#saving the map to visualization folder
base_path = Path(__file__).parent.parent
map_output_path = base_path / "visualizations" / "net_worker_change.html"
net_change_map.save(map_output_path)

#### MAKING HISTOGRAMS TO SHOW DIFFERENCE BETWEEN EPCS & BAY AREA

import seaborn as sns

#VERSION 1: histogram of net change in daily pop at tract level
ax = sns.histplot(bay_counties_plot,
             x='net_worker_change',
             bins=500,
             kde = True
             )

ax.set(xlabel="Net change in daily population", ylabel="Tract Count",
       title="Net Daily Population Change Distribution for All Tracts");
# semicolon mutes text outputs

#%%
base_path = Path(__file__).parent.parent
file_path = base_path / "visualizations" / "netpopchange_alltracts.png"
plt.savefig(file_path, dpi=300, bbox_inches="tight")

# VERSION 2: edited bounds to zoom in on main hump
ax = sns.histplot(bay_counties_plot,
             x='net_worker_change',
             bins=500,
             kde=True   # we can remove this if we think its not valuable
             )

ax.set(xlabel="Net change in daily population", ylabel="Tract Count",
       title="Net Daily Population Change Distribution for All Tracts");
# semicolon mutes text outputs

ax.set_xlim(-5000, 10000);
# setting manual x axis limits to not see outliers and get better idea of actual distribution shape

base_path = Path(__file__).parent.parent
file_path = base_path / "visualizations" / "netpopchange_alltracts_cropped.png"
plt.savefig(file_path, dpi=300, bbox_inches="tight")

#VERSION 3: plot another that is specifically EPC tracts

ax = sns.histplot(bay_counties_plot[bay_counties_plot["is_epc_2050"]],
             x='net_worker_change',
             bins=500,
             kde=True,   # we can remove this if we think its not valuable
             color = "orange"
             )

ax.set(xlabel="Net change in daily population", ylabel="Tract Count",
       title="Net Daily Population Change Distribution for EPCs");
# semicolon mutes text outputs

# setting manual x axis limits to not see outliers and get better idea of actual distribution shape
base_path = Path(__file__).parent.parent
file_path = base_path / "visualizations" / "netpopchange_EPCtracts.png"
plt.savefig(file_path, dpi=300, bbox_inches="tight")

# VERSION 4: plot another that is specifically EPC tracts

ax = sns.histplot(bay_counties_plot[bay_counties_plot["is_epc_2050"]],
             x='net_worker_change',
             bins=500,
             kde=True, # we can remove this if we think its not valuable
             color = "orange"
             )

ax.set(xlabel="Net change in daily population", ylabel="Tract Count",
       title="Net Daily Population Change Distribution for EPCs");
# semicolon mutes text outputs

ax.set_xlim(-5000, 10000);
# setting manual x axis limits to not see outliers and get better idea of actual distribution shape

base_path = Path(__file__).parent.parent
file_path = base_path / "visualizations" / "netpopchange_EPCtracts_cropped.png"
plt.savefig(file_path, dpi=300, bbox_inches="tight")

#VERSION 5: plot zoomed in versions together

# all tracts in blue
sns.histplot(bay_counties_plot,
             x='net_worker_change',
             bins=500,
             kde=True,
             color="skyblue",
             label="All Tracts")

# EPC tracts in orange
sns.histplot(bay_counties_plot[bay_counties_plot["is_epc_2050"]],
             x='net_worker_change',
             bins=500,
             kde=True,
             color="orange",
             label="EPCs 2050")

plt.xlim(-5000, 10000)
plt.xlabel("Net change in daily population")
plt.ylabel("Tract Count")
plt.title("Net Daily Population Change Distribution: All tracts vs. EPCs")
plt.legend() # This adds the labels to the corner

base_path = Path(__file__).parent.parent
file_path = base_path / "visualizations" / "netpopchange_overlaid.png"
plt.savefig(file_path, dpi=300, bbox_inches="tight")

# VERSION 6: plot zoomed in versions together

# all tracts in blue
sns.histplot(bay_counties_plot,
             x='net_worker_change',
             bins=500,
             kde=True,
             color="skyblue",
             label="All Tracts")

# EPC tracts in orange
sns.histplot(bay_counties_plot[bay_counties_plot["is_epc_2050"]],
             x='net_worker_change',
             bins=500,
             kde=True,
             color="orange",
             label="EPCs 2050")

plt.yscale('log')
plt.ylim(1,200)
plt.xlim(-5000, 10000)
plt.xlabel("Net change in daily population")
plt.ylabel("Tract Count")
plt.title("Net Daily Population Change Distribution: All tracts vs. EPCs")
plt.legend() # This adds the labels to the corner


base_path = Path(__file__).parent.parent
file_path = base_path / "visualizations" / "netpopchange_overlaid_logscale.png"
plt.savefig(file_path, dpi=300, bbox_inches="tight")
# %%
