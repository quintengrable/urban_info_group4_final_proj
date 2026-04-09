import pandas as pd
import os
from pathlib import Path
from helper_function import get_path

bay_area_lodes = pd.read_parquet(get_path("processed", "bay_area_lodes_od_table.parquet"))
