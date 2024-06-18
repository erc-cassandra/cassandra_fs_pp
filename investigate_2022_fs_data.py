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

# %%

# %%
import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import xarray as xr

# %%
bs2 = pd.read_csv('/Volumes/Science/Geosciences/Cassandra/fielddata_202305/level-0/beadedstream_retrieved_at_factory/D6050043 - logged data (FS2).csv',
                  index_col=0, skiprows=1, parse_dates=True)

# %% [markdown]
# ## FS3 DTC 2022

# %%
bs3 = pd.read_csv('/scratch/rlim_retention_scratch/D6050048 - logged data (FS3).csv',
                  index_col=0, skiprows=1, parse_dates=True)

# %%
# From field notes. Depth is -ve (so +ve corresponds to location above surface)
first_valid_sensor = 1
depth_of_first_valid_sensor = -0.18

# %%
temps_only = bs3.filter(regex='Unnamed')

# %%
temps_only

# %%
sensor_positions = np.arange(0, -1.80, -0.15)

# %%
sensor_positions

# %%
install_depths = sensor_positions[first_valid_sensor-1:] + depth_of_first_valid_sensor

# %%
da = xr.DataArray(
    temps_only.to_numpy(),
    coords={'time': temps_only.index.values, 'install_depth':install_depths}
)

# %%
da

# %%
da.name = 'dtc'

# %%
da.to_netcdf('/scratch/rlim_retention_scratch/fs2_slab_cold_content/fs3_dtc.nc')

# %% [markdown]
# ## Produce xarray DataArray

# %%
# From field notes. Depth is -ve (so +ve corresponds to location above surface)
first_valid_sensor = 1
depth_of_first_valid_sensor = 1.34

# %%
temps_only = bs2.filter(regex='[0-9]* m')

# %%
sensor_positions = np.array([float(v[:-2]) for v in temps_only.columns])

# %%
install_depths = sensor_positions[first_valid_sensor-1:] + depth_of_first_valid_sensor

# %%
da = xr.DataArray(
    temps_only.to_numpy(),
    coords={'time': temps.index.values, 'install_depth':install_depths}
)

# %%
da.name = 'dtc'

# %%
da.to_netcdf('/scratch/rlim_retention_scratch/fs2_slab_cold_content/fs2_dtc.nc')

# %%
plt.figure()
da.plot()

# %%
da.plot.line(x='time')

# %% [markdown]
# ## Code for extracting profiles for Horst

# %%
# %matplotlib widget
plt.figure()
sns.heatmap(bs2.filter(regex='[0-9]* m').T, cmap='viridis', vmin=-10, vmax=0)
plt.tight_layout()

# %%
slab_temps.iloc[-1]

# %%
slab_temps = bs2.filter(regex='[0-9]* m').loc[:,'-2.15 m':]
slab_temps.loc['2022-06-28'].mean().to_csv('/scratch/rlim_retention_scratch/fs2_dtc/fs2_dtc_2022_06_28.csv')
slab_temps.loc['2022-08-24'].mean().to_csv('/scratch/rlim_retention_scratch/fs2_dtc/fs2_dtc_2022_08_24.csv')
slab_temps.loc['2022-08-24'].mean().to_csv('/scratch/rlim_retention_scratch/fs2_dtc/fs2_dtc_2022_08_24.csv')
slab_temps.loc['2022-09-17'].mean().to_csv('/scratch/rlim_retention_scratch/fs2_dtc/fs2_dtc_2022_09_17.csv')

# %%
bs2.columns

# %%
plt.figure()
slab_temps['-3.15 m'].plot()

# %%
bs3 = pd.read_csv('/Volumes/Science/Geosciences/Cassandra/fielddata_202305/level-0/beadedstream_telemetry/FS3 (2022).csv',
                  index_col=0, parse_dates=True)

# %%
# %matplotlib widget
plt.figure()
sns.heatmap(bs3.filter(regex='[0-9]* m').T, cmap='RdBu_r', vmin=-10, vmax=10)
plt.tight_layout()

# %%
bs3.index

# %%
plt.figure()
plt.plot(bs3.index, bs3['-1.65 m'], 'x-')

# %%
bs5b = pd.read_csv('/Volumes/Science/Geosciences/Cassandra/fielddata_202305/level-0/beadedstream_telemetry/FS5 - Site B (2022).csv',
                  index_col=0)

# %%
plt.figure()
sns.heatmap(bs5b.filter(regex='[0-9]* m').T, cmap='RdBu_r', vmin=-10, vmax=10)
plt.tight_layout()

# %%
hourly = fsd['FS2'].t_air.resample(time='1H').mean()
hourly = hourly.where(hourly > 0)
pdh = hourly.sel(time=slice('2021-06-01','2021-09-30')).sum()
print(pdh)

# %%
hourly.sel(time=slice('2021-06-01','2021-09-30')).groupby(hourly['time.month'].sel(time=slice('2021-06-01','2021-09-30'))).sum()

# %%
hourly.sel(time=slice('2022-06-01','2022-09-30')).groupby(hourly['time.month'].sel(time=slice('2022-06-01','2022-09-30'))).sum()

# %%
fs1_recalc = xr.open_dataset('/Volumes/Science/Geosciences/Cassandra/firn_stations/level-2/FS1.nc')

# %%
plt.figure()
for sensor in range(1,7):
    fs1_recalc.tdr_depth.sel(tdr_sensor=sensor).plot(label=sensor)
#fs1_recalc.surface_height.plot(label='surface height')
plt.grid()
plt.legend()

# %%
import copy
offset1 = -0.35
D = []
udg = fs1_recalc.surface_height.loc['2022-05':].to_pandas()
udg = udg - udg.iloc[0:20].mean()
offset1 = offset1 #+ udg.iloc[0:20].mean()
offset_tracker = []
for ix,udgt in udg.iteritems():
    # This works when initial UDG is +ve
    #Dt = udgt - np.abs(offset1)
    # ----
    Dt = udgt + offset1
    Dt = np.minimum(0, Dt)
    offset1 = np.where(Dt == 0, (udgt*-1), offset1)
    D.append(Dt)
    offset_tracker.append(offset1)

# %%
plt.figure()
plt.plot(udg.index, D, label='D')
plt.plot(udg.index, udg, label='UDG')
plt.grid()
plt.legend()

# %%
fs1_recalc = None

# %%
plt.figure()
data = fs1_recalc
for sensor in data.tdr_sensor:
    plt.scatter(
        data.time.to_index(), 
        data['tdr_depth'].sel(tdr_sensor=sensor), 
        c=data['tdr_t'].sel(tdr_sensor=sensor).values, 
        **dict(cmap='RdBu_r', vmin=-10, vmax=10), label=int(sensor.values))

plt.colorbar()
plt.ylabel(data['tdr_depth'].attrs['units'])
plt.grid()

# %%
slab21 = data['surface_height'] - 1.4
slab21.sel(time=slice('2021-04', '2021-09')).plot(color='gray', alpha=0.5, linewidth=2)

# %%
h22 = data['surface_height'].sel(time=slice('2022-05-10','2022-09'))
hs22 = h22.sel(time=slice('2022-05-10','2022-05-12')).mean()
slab22 = (h22 - hs22) - 1.0
slab22.plot(color='gray', alpha=0.6, linewidth=2)

# %%
h22

# %%
#h23 = data['surface_height'].sel(time='2023-04').mean()
slab23 = h23 - 0.45
plt.plot([pd.Timestamp(2023,5,1), pd.Timestamp(2023,5,20)],[slab23]*2, 'o-', color='gray', alpha=0.5, markersize=10)
