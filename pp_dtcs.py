# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.14.7
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Post-process/plot temperature chains logged by BeadedStream
#
# Only concerns data logged by D605 loggers (2022 onwards).
#
# Andrew Tedstone, June 2024

# %%
import pandas as pd
import xarray as xr
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns


# %%
def plot_dtc(data, title):
    with sns.color_palette('viridis', n_colors=len(data.columns)):
        fig, ax = plt.subplots()
        data.plot(ax=ax)
        plt.title(title)
        plt.grid()
        plt.ylabel('degrees C')
    return fig, ax


# %% [markdown]
# ## FS1

# %%
fs1 = pd.read_csv('/Volumes/Science/Geosciences/Cassandra/fielddata_202405/level-0/fs1/beadedcloud_fs1__2023___temp_C__depth_m.csv',
                  index_col=0, skiprows=0, parse_dates=True)

# %%
fs1 = fs1.drop(columns=['timezone', 'Battery (V)', 'Panel Temp (C)'])
fs1

# %%
plot_dtc(fs1, 'FS1 (beadedcloud)\nImportant: depths uncorrected!')
plt.savefig('/Volumes/Science/Geosciences/Cassandra/firn_stations/beadedstream_dtc_overviews/beadedcloud_fs1__2023___temp_C__depth_m.png', dpi=300)

# %% [markdown]
# ## FS2

# %%
fs2 = pd.read_csv('/Volumes/Science/Geosciences/Cassandra/fielddata_202305/level-0/FS2/beadedcloud_FS2 (2022).csv',
                  index_col=0, skiprows=0, parse_dates=True)
fs2 = fs2.drop(columns=['timezone', 'Battery (V)', 'Panel Temp (C)'])

plot_dtc(fs2, 'FS2 (beadedcloud)\nImportant: depths uncorrected!')
plt.savefig('/Volumes/Science/Geosciences/Cassandra/firn_stations/beadedstream_dtc_overviews/beadedcloud_FS2 (2022).png', dpi=300)

# %%
fs2

# %%
fs2 = pd.read_csv('/Volumes/Science/Geosciences/Cassandra/fielddata_202405/level-0/fs2/beadedcloud_fs2__2023___temp_C__depth_m.csv',
                  index_col=0, skiprows=0, parse_dates=True)
fs2 = fs2.drop(columns=['timezone', 'Battery (V)', 'Panel Temp (C)'])

# %%
fs2

# %%
plot_dtc(fs2, 'FS2 (beadedcloud)\nImportant: depths uncorrected!')
plt.savefig('/Volumes/Science/Geosciences/Cassandra/firn_stations/beadedstream_dtc_overviews/beadedcloud_fs2__2023___temp_C__depth_m.png', dpi=300)

# %% [markdown]
# ## FS3

# %%
fs3 = pd.read_csv('/Volumes/Science/Geosciences/Cassandra/fielddata_202305/level-0/FS3/beadedcloud_FS3 (2022).csv',
                  index_col=0, skiprows=0, parse_dates=True)
fs3 = fs3.drop(columns=['timezone', 'Battery (V)', 'Panel Temp (C)'])

plot_dtc(fs3, 'FS3 (beadedcloud)\nImportant: depths uncorrected!')
plt.savefig('/Volumes/Science/Geosciences/Cassandra/firn_stations/beadedstream_dtc_overviews/beadedcloud_FS3 (2022).png', dpi=300)

# %%
fs3 = pd.read_csv('/Volumes/Science/Geosciences/Cassandra/fielddata_202405/level-0/fs3/beadedcloud_fs3__2023___temp_C__depth_m.csv',
                  index_col=0, skiprows=0, parse_dates=True)
fs3 = fs3.drop(columns=['timezone', 'Battery (V)', 'Panel Temp (C)'])
plot_dtc(fs3, 'FS3 (beadedcloud)\nImportant: depths uncorrected!')
plt.savefig('/Volumes/Science/Geosciences/Cassandra/firn_stations/beadedstream_dtc_overviews/beadedcloud_fs3__2023___temp_C__depth_m.png', dpi=300)

# %% [markdown]
# ## Ignore

# %%
sensor_positions = np.arange(0, -1.80, -0.15)
install_depths = sensor_positions[first_valid_sensor-1:] + depth_of_first_valid_sensor

# From field notes. Depth is -ve (so +ve corresponds to location above surface)
first_valid_sensor = 1
depth_of_first_valid_sensor = -0.18

da = xr.DataArray(
    temps_only.to_numpy(),
    coords={'time': temps_only.index.values, 'install_depth':install_depths}
)
da.name = 'dtc'
