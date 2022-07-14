""" 
Level-1 csv file to Level-2 netCDF

"""

import pandas as pd
import xarray as xr
import numpy as np
import os
import math
import copy

import cassandra_fs_pp as fs_pp

import matplotlib.pyplot as plt

# Set up options
data_root = '/Volumes/Science/Geosciences/Cassandra'
config_file = os.path.join(data_root, 'firn_stations/ppconfig', 'fs1.toml')

# Load level-1 data
fs = fs_pp.fs(config_file, data_root)
fs.load_level1_dataset('/Users/tedstona/Desktop/fs1_nosort.csv')
fs.ds_level1.index.name = 'time'

# Convert the data to level-2 format.
fs.level1_to_level2()

# Smooth the UDG record
udg_median = fs.ds_level2['TCDT(m)'].rolling('3D', center=True).median()
udg_median_xr = xr.DataArray(udg_median, dims=['time'])


## -----------------------------------------------------------------------------
## Organise Sub-Surface data
def subsurf_DataArray(sensor_type, name, units, pattern, sensors_info):
    arr = xr.DataArray(
        fs.ds_level2.filter(regex=pattern),
        dims=['time', '%s_sensor' %sensor_type],
        coords={
            'time': fs.ds_level2.index, 
            '%s_sensor' %sensor_type: list(sensors_info.keys()), 
            '%s_install_depth' %sensor_type: ('%s_sensor' %sensor_type, list(sensors_info.values()))
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
tdr_t = subsurf_DataArray('tdr', 'land_ice_temperature', 'degree_Celsius', 'TDR[0-9]\_T', active_tdrs)
tdr_ec = subsurf_DataArray('tdr', 'electrical_conductivity', 'dS/m', 'TDR[0-9]\_EC', active_tdrs)
tdr_vwc = subsurf_DataArray('tdr', 'volumetric_water_content', 'm^3/m^3', 'TDR[0-9]\_VWC', active_tdrs)
tdr_perm = subsurf_DataArray('tdr', 'permittivity', '', 'TDR[0-9]\_Perm', active_tdrs)
tdr_vr = subsurf_DataArray('tdr', 'voltage_ratio', '', 'TDR[0-9]\_VR', active_tdrs)
tdr_period = subsurf_DataArray('tdr', 'period', 'microseconds', 'TDR[0-9]\_Period', active_tdrs)

#DTC
sensor_positions_f, first_sensor, depth = fs.config['level1_2']['dtc_info']['1']
sensor_positions = fs.load_dtc_positions(filename=os.path.join(fs.data_root,sensor_positions_f))
dtc_depths_t0 = fs.chain_installation_depths(sensor_positions, first_sensor, depth)
# consider mask of valid DTC sensors - this is only relevant where extra sensors
# have been coiled at the surface.
dtc1 = subsurf_DataArray('dtc1', 'land_ice_temperature', 'degree_Celsius', 'DTC1_[0-9]+', 
    dtc_depths_t0)

# EC
sensor_positions_f, first_sensor, depth = fs.config['level1_2']['ec_info']['1']
sensor_positions = pd.read_csv(os.path.join(fs.data_root,sensor_positions_f)).squeeze()
ec_depths_t0 = fs.chain_installation_depths(sensor_positions, first_sensor, depth)
ec1 = subsurf_DataArray('ec1', 'electrical_conductivity', 'microSiemens', 'EC\([0-9]+\)', 
    ec_depths_t0)


## -----------------------------------------------------------------------------
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
battv = surf_DataArray('battery_minimum', 'volts', 'BattV_Min')


# ------------------------------------------------------------------------------
## Create xarray Dataset
attrs = {
    'creator_name': 'Andrew Tedstone',
    'creator_email': 'andrew.tedstone@unifr.ch',
    'source': 'https://www.github.com/erc-cassandra/cassandra_fs_pp/bin/level2.py',
    'institution': 'University of Fribourg, Switzerland',
    'title':'Near-surface and sub-surface data from {site}, Greenland Ice Sheet'.format(site=fs.config['site']),
    'processing_level':'Level 2',
    'product_version': 'v1',
    'license':'Creative Commons Attribution 4.0 International (CC-BY-4.0) https://creativecommons.org/licenses/by/4.0',
    'latitude':0,
    'longitude':0,
    'position_date':'2021-04',
    'timezone':'UTC'
}


dataset = xr.Dataset(
    data_vars={
        'DTC1':dtc1,
        'EC1':ec1,
        'surface_height':udg,
        't_air':t_air,
        'TDR_EC':tdr_ec,
        'TDR_T':tdr_t,
        'TDR_VWC':tdr_vwc,
        'TDR_Perm':tdr_perm,
        'TDR_Period':tdr_period,
        'TDR_VR':tdr_vr,
        'batt':battv
    },
    attrs=attrs,
)


# Write to netcdf
dataset.to_netcdf(os.path.join(fs.data_root, 'firn_stations/level-2/%s.nc' %fs.config['site']))
