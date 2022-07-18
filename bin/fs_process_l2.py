#!/usr/bin/env python
""" 
Level-1 csv file to Level-2 netCDF

"""

import pandas as pd
import xarray as xr
import numpy as np
import os
import math
import copy
import argparse

import cassandra_fs_pp as fs_pp


import pdb

parser = argparse.ArgumentParser('Process level-1 data up to level-2 status.')

parser.add_argument('site', type=str, help='Name of site, normally corresponding to TOML metadata file.')

cwd = os.getcwd()
parser.add_argument('-data_root', type=str, default=cwd,
    help='Path to Level-1 file (no need to specify if current directory is the data_root directory).')

parser.add_argument('-metafile', type=str, default=None, 
    help='Path to metadata TOML file, normally set automatically.')

parser.add_argument('-outfile', type=str, default=None, 
    help='Path to output NetCDF, normally set automatically.')

parser.add_argument('-ow', action='store_true',
    help='If provided, forces over-write of existing file.')

args = parser.parse_args()

if args.metafile is None:
    args.metafile = os.path.join(args.data_root, 
        'firn_stations/ppconfig', 
        '%s.toml' %args.site)

fs = fs_pp.fs(args.metafile, args.data_root)

# Check for existence of Level-2 file.
if not args.ow:
    if args.outfile is None:
        p = fs._get_level2_default_path()
    else:
        p = args.outfile
    check = os.path.exists(p)

    if check:
        raise IOError('The Level-2 output file for this site already exists. To overwrite, specify -ow.')


## -----------------------------------------------------------------------------

fs.load_level1_dataset(args.outfile)
fs.ds_level1.index.name = 'time'

# Convert the data to level-2 format.
fs.level1_to_level2()

# Smooth the UDG record for calculating depths
udg_median = fs.ds_level2['TCDT(m)'].rolling('3D', center=True).median()
udg_median_xr = xr.DataArray(udg_median, dims=['time'])

data_vars = {}
encoding = {}
# {"my_variable": {"dtype": "int16", "scale_factor": 0.1, "zlib": True}, ...}

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
data_vars['TDR_T'] = subsurf_DataArray('tdr', 'land_ice_temperature', 'degree_Celsius', r'TDR[0-9]\_T', active_tdrs)
data_vars['TDR_EC'] = subsurf_DataArray('tdr', 'electrical_conductivity', 'dS/m', r'TDR[0-9]\_EC', active_tdrs)
data_vars['TDR_VWC'] = subsurf_DataArray('tdr', 'volumetric_water_content', 'm^3/m^3', r'TDR[0-9]\_VWC', active_tdrs)
data_vars['TDR_Perm'] = subsurf_DataArray('tdr', 'permittivity', '', r'TDR[0-9]\_Perm', active_tdrs)
data_vars['TDR_VR'] = subsurf_DataArray('tdr', 'voltage_ratio', '', r'TDR[0-9]\_VR', active_tdrs)
data_vars['TDR_Period'] = subsurf_DataArray('tdr', 'period', 'microseconds', r'TDR[0-9]\_Period', active_tdrs)

#pdb.set_trace()
#DTC
for dtc_key, values in fs.config['level1_2']['dtc_info'].items():
    sensor_positions_f, first_sensor, depth = values
    sensor_positions = fs.load_dtc_positions(filename=os.path.join(fs.data_root,sensor_positions_f))
    dtc_depths_t0 = fs.chain_installation_depths(sensor_positions, first_sensor, depth)
    # consider mask of valid DTC sensors - this is only relevant where extra sensors
    # have been coiled at the surface.
    dtc = subsurf_DataArray('dtc%s'%dtc_key, 'land_ice_temperature', 'degree_Celsius', r'DTC%s_[0-9]+' %dtc_key, 
        dtc_depths_t0)
    data_vars['DTC%s'%dtc_key] = dtc

# EC
for ec_key, values in fs.config['level1_2']['ec_info'].items():
    sensor_positions_f, first_sensor, depth = values
    sensor_positions = pd.read_csv(os.path.join(fs.data_root,sensor_positions_f)).squeeze()
    ec_depths_t0 = fs.chain_installation_depths(sensor_positions, first_sensor, depth)
    ec = subsurf_DataArray('ec%s'%ec_key, 'electrical_conductivity', 'microSiemens', r'EC\([0-9]+\)', 
        ec_depths_t0)
    data_vars['EC%s'%dtc_key] = dtc    


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

data_vars['t_air'] = surf_DataArray('air_temperature', 'degree_Celsius', 'T107_C')
data_vars['surface_height'] = surf_DataArray('distance_to_surface_from_stake', 'm', 'TCDT(m)')
data_vars['batt'] = surf_DataArray('battery_minimum', 'volts', 'BattV_Min')


# ------------------------------------------------------------------------------
## Create xarray Dataset
attrs = {
    'creator_name': 'Andrew Tedstone',
    'creator_email': 'andrew.tedstone@unifr.ch',
    'contributors': 'Horst Machguth, Nicole Clerx, Nicolas Jullien',
    'source': 'https://www.github.com/erc-cassandra/cassandra_fs_pp/bin/fs_process_l2.py',
    'institution': 'University of Fribourg, Switzerland',
    'title':'Near-surface and sub-surface data from {site}, Greenland Ice Sheet'.format(site=fs.config['site']),
    'processing_level':'Level 2',
    'product_version': 'v1',
    'license':'Creative Commons Attribution 4.0 International (CC-BY-4.0) https://creativecommons.org/licenses/by/4.0',
    'latitude':fs.config['lat'],
    'longitude':fs.config['lon'],
    'timezone':'UTC'
}

dataset = xr.Dataset(data_vars=data_vars, attrs=attrs)

exclude = ['time', 'tdr_sensor']
for var in dataset.variables:
    if var in exclude:
        continue
    encoding[var] = {'dtype':'int16', 'scale_factor':0.001, 'zlib':False, '_FillValue':-9999}
    #dataset[var].attrs['_FillValue'] = -999

# Write to netcdf
dataset.to_netcdf(os.path.join(fs.data_root, 'firn_stations/level-2/%s.nc' %fs.config['site']),
    encoding=encoding, unlimited_dims=['time'])

pdb.set_trace()
