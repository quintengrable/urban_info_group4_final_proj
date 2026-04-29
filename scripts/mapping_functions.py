"""
mapping_functions.py

Functions to produce map visualisations
"""
#### IMPORTS ####
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import folium

#### FUNCTIONS ####
#Function 1: plt_gdf_cmap_outlinegeos
#Plots interractive choropleth map with certain geometries outlined

def plt_gdf_cmap_outlinegeos(
    gdf: gpd.GeoDataFrame,
    cmap_col: str,
    colormap: str,
    cmap_scale_bins: int,
    cmap_scale_label: str,
    cmap_scale_max: int,
    cmap_scale_min: int,
    tooltip_cols: str,
    cmap_layer_name: str,
    outline_col: str,
    outline_color: str,
    outline_name: str
) -> folium.folium.Map:
    """
    Takes a geodataframe and plots an interactive choropleth map,
    outlining certain geographies, with tooltips.

    Parameters
    ----------
    gdf: gpd.GeoDataFrame
      geodataframe with at least columns of: values to plot, geometries,
      and a boolean column indicating whether you want that geometry outlined

    cmap_col: str
      column name from gdf for the data you want plotted as a colormap

    colormap: str
      name of the colormap color scheme
      matplotlib.Colormap, branca.colormap or function
      If data is diverging around zero, pick a colormap that does the same
      Otherwise, hues of just one color is best (larger values = darker)

    cmap_scale_bins: int
      list of integers that define the bins for colors in the scale
      list can be any length
      if data is diverging around zero, you'll want values mirrored around zero
      eg[-100,-50,0,50,100]

    cmap_scale_label: str
      label for the color scale eg. "Net Daytime Worker Change"

    cmap_scale_max: int
      The maximum value of the colormap scale. Usually best to be ~90th
      percentile, but look at your data distribution to decide

    cmap_scale_min: int
      The minimum value of the colormap scale. If data diverges around zero,
      this should be equal to -cmapscale_max. Otherwise, usually best to be 
      ~10th percentile, but look at your data distribution to decide.

    tooltip_cols: str
      list of gdf column names to go into the tooltip, can be any length

    cmap_layer_name: str
      name for the colormap layer of the map, eg. "Net Daytime Worker Change"

    outline_col: str
      name of the column with booleans indicating whether the geometry will be
      outlined (True if you want to outline the geometry, False if not)

    outline_color: str
      either color name or hex code
      Make sure this contrasts against the colormap

    outline_name: str
    name for the outline layer of the map, eg. "EPC Tracts (2050)"

    Returns
    -------
    m
      interactive map (can then be saved as an html)
    """
    # Base colormap of net change
    m = gdf.explore(
        column=cmap_col,
        scheme="UserDefined",
        classification_kwds={"bins": cmap_scale_bins},
        cmap=colormap,
        legend=True,
        legend_kwds={"caption": cmap_scale_label,"fmt": "{:.0f}"},
        vmax=cmap_scale_max,
        vmin=cmap_scale_min,
        tiles="CartoDB positron",
        tooltip=tooltip_cols,
        popup=tooltip_cols,
        style_kwds={
            "weight": 0.3,          #thin borders for non-outlined geometries
            "fillOpacity": 0.7,
            },
        missing_kwds={              #make the water blue like the daytime map
            "color": "#a8d5e2",
            "fillOpacity": 0.8
            },
        name=cmap_layer_name
        )

    #EPC overlay to outline them
    gdf[gdf[outline_col]].explore(
        m=m,
        color="none",
        style_kwds={
            "color": outline_color,
            "weight": 1,           #thicker border for outlined geometries
            "fillOpacity": 0,
            "fill": False
        },
        tooltip=False,
        name=outline_name
    )

    #Layer control to toggle EPC on/off
    folium.LayerControl().add_to(m)

    return m