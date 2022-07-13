""" 
Level-1 csv file to Level-2 netCDF

"""

import pandas as pd
import xarray as xr
import numpy as np
import os
import math

import cassandra_fs_pp as fs_pp

import matplotlib.pyplot as plt


data_root = '/Volumes/Science/Geosciences/Cassandra'
config_file = os.path.join(data_root, 'firn_stations/ppconfig', 'fs1.toml')

fs = fs_pp.fs(config_file, data_root)
fs.load_level1_dataset('/Users/tedstona/Desktop/fs1_nosort.csv')
fs.ds_level1.index.name = 'time'

fs.level1_to_level2()

# Organise UDG data
# udg_norm = fs._normalise_udg()
# udg_norm = fs._filter_udg(udg_norm)

udg_median = fs.ds_level2['TCDT(m)'].rolling('3D', center=True).median()
udg_median_xr = xr.DataArray(udg_median, dims=['time'])


## Organise Sub-Surface data
def subsurf_DataArray(name, units, pattern, sensors_info):
    arr = xr.DataArray(
        fs.ds_level2.filter(regex=pattern),
        dims=['time', 'sensor'],
        coords={
            'time': fs.ds_level2.index, 
            'sensor': list(sensors_info.keys()), 
            'install_depth': ('sensor', list(sensors_info.values()))
        }
    )
    arr.attrs['standard_name'] = name
    arr.attrs['units'] = units
    return arr

# TDRs
tdr_info = fs.config['level1_2']['tdr_depth']
active_tdrs = {}
for tdr in tdr_info:
    if 'TDR%s_T(C)' %tdr in fs.ds_level2.columns:
        active_tdrs[int(tdr)] = tdr_info[tdr]
print (active_tdrs)
tdr_t = subsurf_DataArray('land_ice_temperature', 'degree_Celsius', 'TDR[0-9]\_T', active_tdrs)
tdr_ec = subsurf_DataArray('electrical_conductivity', 'dS/m', 'TDR[0-9]\_EC', active_tdrs)
tdr_vwc = subsurf_DataArray('volumetric_water_content', 'm^3/m^3', 'TDR[0-9]\_VWC', active_tdrs)
tdr_perm = subsurf_DataArray('permittivity', '', 'TDR[0-9]\_Perm', active_tdrs)
tdr_vr = subsurf_DataArray('voltage_ratio', '', 'TDR[0-9]\_VR', active_tdrs)
tdr_period = subsurf_DataArray('period', 'microseconds', 'TDR[0-9]\_Period', active_tdrs)

# DTC
dtc1_pos = fs.load_dtc_positions(key='dtc1_info').to_dict()
dtc1 = subsurf_DataArray('land_ice_temperature', 'degree_Celsius', 'DTC1_[0-9]+', dtc1_pos)

# EC
# To do


## Organise surface data
def surf_DataArray(name, units, key):
    arr = xr.DataArray(
        fs.ds_level2[key],
        dims=['time'],
        coords={'time':fs.ds_level2.index},
        )
    arr.attrs['standard_name'] = name
    arr.attrs['units'] = units
    return arr

t_air = surf_DataArray('air_temperature', 'degree_Celsius', 'T107_C')
udg = surf_DataArray('distance_to_surface_from_stake', 'm', 'TCDT(m)')




# To do : write to netcdf

## some enforcer for data quality ??






############
## LOADING FUNCS
# To provide as part of level-2 data loading suite:

def calc_depth_tdrs(
    arr : xr.DataArray, 
    udg : xr.DataArray
    ) -> xr.DataArray:
    """
    Consider this problem in terms of the total distance between the UDG
    and the installed TDR.
    This total distance changes whenever the TDR is exposed at the surface and 
    then gets lowered with that surface, increasing the offset.
    We need to account for periods of surface lowering before we can calculate
    the new burial depth of the TDR.
    """
    offset = float(udg_median_xr.isel(time=0)) - arr.install_depth.values
    D = []
    for t in arr.time:
        udg = float(udg_median_xr.sel(time=t))
        Dt = udg - offset
        Dt = np.minimum(0, Dt)
        offset = np.where(Dt == 0, udg, offset)
        D.append(Dt)
    DD = np.array(D)
    arr.coords['depth'] = (('time', 'sensor'), DD)
    return arr


def calc_depth(
    arr : xr.DataArray,
    udg : xr.DataArray
    ) -> xr.DataArray:
    """
    Suitable for calculating depth of sensors whose position in space are not
    affected by melting and accumulation.
    """
    arr.coords['depth'] = arr.install_depth + udg
    return arr

dtc1 = add_depth(dtc1)

# What other Level-2 tools might be needed - is add_depth() the only one?