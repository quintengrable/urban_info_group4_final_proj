# Shared helper functions for the urban analytics project.

from pathlib import Path
import os
from typing import Union

import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.axes import Axes
from matplotlib.figure import Figure

PathLike = Union[str, Path]


# Data loading

def load_tract_geodata(
    filepath: PathLike,
    pop_col: str = "pop",
    area_col: str = "area_km2",
    crs: int = 4326,
) -> gpd.GeoDataFrame:
    """
    Load a census tract GeoJSON, filter rows where ``pop_col`` is
    zero or missing, compute population density, and reproject.

    Parameters
    ----------
    filepath : str | Path
        Path to the GeoJSON file.
    pop_col : str
        Column name for population count.
    area_col : str
        Column name for area in km².
    crs : int
        EPSG code for output projection.

    Returns
    -------
    geopandas.GeoDataFrame
        GeoDataFrame with an added ``density`` column.
    """
    gdf: gpd.GeoDataFrame = gpd.read_file(filepath)
    gdf = gdf[gdf[pop_col] > 0].copy()
    gdf["density"] = gdf[pop_col] / gdf[area_col]
    return gdf.to_crs(epsg=crs)


# Data cleaning

def clean_numeric_col(
    df: pd.DataFrame,
    col: str,
    fill_value: int | float = 0,
) -> pd.DataFrame:
    """
    Coerce a column to numeric and fill missing values.

    Parameters
    ----------
    df : pandas.DataFrame
        Input DataFrame.
    col : str
        Column to clean.
    fill_value : int | float
        Value used to fill missing entries after coercion.

    Returns
    -------
    pandas.DataFrame
        Copy of the DataFrame with the cleaned column.
    """
    df = df.copy()
    df[col] = pd.to_numeric(df[col], errors="coerce")
    df[col] = df[col].fillna(fill_value)
    return df


# Visualization

def plot_choropleth(
    gdf: gpd.GeoDataFrame,
    col: str,
    title: str,
    cmap: str = "YlOrRd",
    figsize: tuple[int, int] = (10, 8),
    legend: bool = True,
) -> tuple[Figure, Axes]:
    """
    Plot a choropleth map of a GeoDataFrame column.

    Parameters
    ----------
    gdf : geopandas.GeoDataFrame
        GeoDataFrame to plot.
    col : str
        Column to visualize.
    title : str
        Map title.
    cmap : str
        Matplotlib colormap name.
    figsize : tuple[int, int]
        Figure dimensions in inches.
    legend : bool
        Whether to show a legend.

    Returns
    -------
    tuple[matplotlib.figure.Figure, matplotlib.axes.Axes]
        Figure and axes objects.
    """
    fig, ax = plt.subplots(figsize=figsize)
    gdf.plot(column=col, cmap=cmap, legend=legend, ax=ax)
    ax.set_title(title, fontsize=14, pad=12)
    ax.axis("off")
    return fig, ax

#get path
def get_path(parent: str, filename: str)-> str: 
    """
    Get path for importing files. 

    Parameters
    ----------
    parent: str
        "raw" or "processed", depending on what data you are working with 
    filename: str
        name of file with extension 

    Returns
    -------
    file_path 
        file path 
    """
    parent_path = Path.cwd()/ ("data/" + parent) # test 
    print("Data directory:", parent_path.resolve())
    file_path = os.path.join(parent_path, filename)

    return file_path