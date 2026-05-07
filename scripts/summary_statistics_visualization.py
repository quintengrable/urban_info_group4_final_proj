
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
import sys
import os
from helper_function import get_path
from pathlib import Path

bay_area_lodes = pd.read_parquet(get_path("processed", "bay_area_lodes_od_table.parquet"))

#TODO: confirm there are extra counties because people who live OR work in bay area are counted
bay_area_lodes['home_county'].unique()

# group by county

county_pop = bay_area_lodes.groupby('work_county')['S000'].sum().to_frame().rename_axis("GEOID")
county_pop = county_pop.rename(columns = {"S000": "work_pop"}).rename_axis("GEOID")

county_pop["home_pop"] = bay_area_lodes.groupby("home_county")["S000"].sum()

# since those who either live or work in bay area are counted, filter this data for only those who live OR work in bay area.

bay_area_geocodes = ['06001', # Alameda
                     '06013', # Contra Costa
                     '06041', # Marin
                     '06055', # Napa
                     '06075', # SF
                     '06081', # San Mateo
                     '06085', # Santa Clara
                     '06095', # Solano
                     '06097'  # Sonoma
                     ]

bay_geocode_map = {'06001': 'Alameda',
                     '06013': 'Contra Costa',
                     '06041': 'Marin',
                     '06055': 'Napa',
                     '06075': 'San Francisco',
                     '06081': 'San Mateo',
                     '06085': 'Santa Clara',
                     '06095': 'Solano',
                     '06097': 'Sonoma',
                     }

county_pop = county_pop[county_pop.index.isin(bay_area_geocodes)]

county_pop['County'] = county_pop.index.map(bay_geocode_map)
county_pop['pop_diff'] = county_pop['work_pop'] - county_pop['home_pop']
county_pop = county_pop.sort_values(by='pop_diff', ascending=True)

# plot day vs night populations per county
fig, ax = plt.subplots(figsize=(9, 6))

y = county_pop['County']

y_pos = np.arange(len(y))
height = 0.35

ax.barh(y_pos + height/2, width = county_pop['work_pop'], height = height, color = '#f39c12', label = "Work County")
ax.barh(y_pos - height/2, width = county_pop['home_pop'], height = height, color = '#0000D1', label = "Home County")

ax.set_yticks(y_pos)
ax.set_yticklabels(y)

ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f'{int(x):,}'))

ax.set_ylabel('County')
ax.set_xlabel('Population Count')
ax.set_title('County Daytime vs Nighttime Population')
ax.spines[['top', 'right']].set_visible(False)

ax.legend(loc='lower right')

save_path = Path(__file__).parent.parent / "visualizations" / "day_night_county.png"
plt.savefig(
    save_path, 
    dpi=300,              
    bbox_inches='tight',   
    facecolor='white',     
    transparent=False
)

plt.tight_layout()
plt.show()

