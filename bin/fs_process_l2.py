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
import datetime as dt

import cassandra_fs_pp as fs_pp

#######
version = 'v1.1'
#######

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
#pdb.set_trace()
# Convert the data to level-2 format.
fs.level1_to_level2()

fs.ds_level2.to_csv(os.path.join(fs.data_root, 'firn_stations/level-2/%s.csv' %fs.config['site']))

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
            '%s_install_depth' %sensor_type: ('%s_sensor' %sensor_type, list(sensors_info.values())),
        }
    )
    # if sensor_type == 'tdr':
    #     arr.coords['tdr_depth'] = data_vars['TDR_Depth']

    arr.attrs['standard_name'] = name
    arr.attrs['units'] = units
    return arr

# TDRs
tdr_info = fs.config['level1_2']['tdr_info']

active_tdrs = {}
depths = []
tdr_data = []
for tdr in tdr_info.keys():
    if 'TDR%s_T(C)' %tdr in fs.ds_level2.columns:
        # Create a dict of TDR number : depth
        active_tdrs[int(tdr)] = tdr_info[tdr][1]
        # Calculate time-varying depth of TDR
        depths.append(fs._calc_depth_tdr(tdr, udg_median))

# Create time-varying depth variable
# However, we don't attach this as a coordinate to the TDR variables as it isn't
# really valid - it probably contains NANs depending on UDG status, TDR status...
tdr_depths = pd.concat(depths, axis=1)
data_vars['tdr_depth'] = xr.DataArray(
    tdr_depths, 
    dims=('time', 'tdr_sensor'),
    coords={
        'time':tdr_depths.index,
        'tdr_sensor':list(active_tdrs.keys())
    },
    attrs={
        'standard_name':'tdr_depth_below_surface',
        'units':'m',
        'description':'Estimated depth of TDR sensor below surface at given timestamp'
    }
)

last_tdr = np.max(list(active_tdrs.keys()))
data_vars['tdr_t'] = subsurf_DataArray('tdr', 'land_ice_temperature', 'degree_Celsius', r'TDR[0-%s]\_T' %last_tdr, active_tdrs)
data_vars['tdr_ec'] = subsurf_DataArray('tdr', 'bulk_electrical_conductivity', 'dS/m', r'TDR[0-%s]\_EC' %last_tdr, active_tdrs)
#data_vars['tdr_vwc'] = subsurf_DataArray('tdr', 'volumetric_water_content', 'm^3/m^3', r'TDR[0-9]\_VWC', active_tdrs)
data_vars['tdr_perm'] = subsurf_DataArray('tdr', 'permittivity', '', r'TDR[0-%s]\_Perm' %last_tdr, active_tdrs)
data_vars['tdr_vr'] = subsurf_DataArray('tdr', 'voltage_ratio', '', r'TDR[0-%s]\_VR' %last_tdr, active_tdrs)
data_vars['tdr_period'] = subsurf_DataArray('tdr', 'period', 'micro_seconds', r'TDR[0-%s]\_Period' %last_tdr, active_tdrs)


#DTC
for dtc_key, values in fs.config['level1_2']['dtc_info'].items():
    install_date, sensor_positions_f, first_sensor, depth = values
    sensor_positions = fs.load_dtc_positions(filename=os.path.join(fs.data_root,sensor_positions_f))
    dtc_depths_t0 = fs.chain_installation_depths(sensor_positions, first_sensor, depth)
    # consider mask of valid DTC sensors - this is only relevant where extra sensors
    # have been coiled at the surface.
    dtc = subsurf_DataArray('dtc%s'%dtc_key, 'land_ice_temperature', 'degree_Celsius', r'DTC%s_[0-9]+' %dtc_key, 
        dtc_depths_t0)
    data_vars['dtc%s'%dtc_key] = dtc

# EC
for ec_key, values in fs.config['level1_2']['ec_info'].items():
    install_date, sensor_positions_f, first_sensor, depth = values
    sensor_positions = pd.read_csv(os.path.join(fs.data_root,sensor_positions_f)).squeeze()
    ec_depths_t0 = fs.chain_installation_depths(sensor_positions, first_sensor, depth)
    ec = subsurf_DataArray('ec%s'%ec_key, 'electrical_conductivity', 'microSiemens', r'EC\([0-9]+\)', 
        ec_depths_t0)
    data_vars['ec%s'%ec_key] = ec    


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
    'site_id': fs.config['site'],
    'title': 'Near-surface and sub-surface data from {site}, Greenland Ice Sheet'.format(site=fs.config['site']),
    'institution': 'University of Fribourg, Switzerland',
    'creator_name': 'Andrew Tedstone',
    'creator_email': 'andrew.tedstone@unifr.ch',
    'contributors': 'Horst Machguth, Nicole Clerx, Nicolas Jullien, Hannah Picton',
    'source': 'https://www.github.com/erc-cassandra/cassandra_fs_pp/bin/fs_process_l2.py',
    'processing_level':'Level 2',
    'product_version': version,
    'processing_date': dt.datetime.now().isoformat("T","minutes"),
    'license':'Creative Commons Attribution 4.0 International (CC-BY-4.0) https://creativecommons.org/licenses/by/4.0',
    'latitude':fs.config['lat'],
    'longitude':fs.config['lon'],
    'timezone':'UTC'
}

dataset = xr.Dataset(data_vars=data_vars, attrs=attrs)

for var in dataset.variables:
    if var in dataset.coords: 
        continue
    encoding[var] = {'dtype':'int32', 'scale_factor':0.001, 'zlib':False, '_FillValue':-9999}
    #dataset[var].attrs['_FillValue'] = -999

# Write to netcdf
dataset.to_netcdf(os.path.join(fs.data_root, 'firn_stations/level-2/%s.nc' %fs.config['site']),
    encoding=encoding, unlimited_dims=['time'])
