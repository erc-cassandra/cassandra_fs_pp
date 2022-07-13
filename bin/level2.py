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


# DTC
def chain_installation_depths(sensor_positions, first_sensor, depth):
    """
    Relevant for all chains which are fixed in position and with defined spacing.

    sensor_positions : in positive millimetres
    first_sensor : int, ID of sensor for which depth was measured
    depth : float, depth of sensor with ID first_sensor (-ve if below ground)
    """
    # Convert from +ve mm to -ve metres
    sensor_positions = sensor_positions * 1e-3 * -1
    ref_sensor_position = sensor_positions.iloc[first_sensor + 1]
    ref_sensor_depth = depth
    sensor_depths_t0 = copy.deepcopy(sensor_positions)
    #                    Negatives           Negative            Negative
    sensor_depths_t0 = sensor_depths_t0 - ref_sensor_position + depth
    sensor_depths_t0.index = np.arange(1, len(sensor_depths_t0)+1)
    return sensor_depths_t0.to_dict()


sensor_positions_f, first_sensor, depth = fs.config['level1_2']['dtc_info']['1']
sensor_positions = fs.load_dtc_positions(filename=os.path.join(fs.data_root,sensor_positions_f)) # UPDATE CODE BACKING THIS.
dtc_depths_t0 = chain_installation_depths(sensor_positions, first_sensor, depth)
# consider mask of valid DTC sensors - this is only relevant where extra sensors
# have been coiled at the surface.
dtc1 = subsurf_DataArray('dtc1', 'land_ice_temperature', 'degree_Celsius', 'DTC1_[0-9]+', 
    dtc_depths_t0)


# EC
sensor_positions_f, first_sensor, depth = fs.config['level1_2']['ec_info']['1']
sensor_positions = pd.read_csv(os.path.join(fs.data_root,sensor_positions_f)).squeeze()
ec_depths_t0 = chain_installation_depths(sensor_positions, first_sensor, depth)
# consider mask of valid DTC sensors - this is only relevant where extra sensors
# have been coiled at the surface.
ec1 = subsurf_DataArray('ec1', 'electrical_conductivity', 'milliVolts', 'EC\([0-9]+\)', 
    ec_depths_t0)


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
    'position_date':'2021-04'
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
        'TDR_VR':tdr_vr
    },
    attrs=attrs,
)


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
    offset = float(udg_median_xr.isel(time=0)) - arr.tdr_install_depth.values
    D = []
    for t in arr.time:
        udg = float(udg_median_xr.sel(time=t))
        Dt = udg - offset
        Dt = np.minimum(0, Dt)
        offset = np.where(Dt == 0, udg, offset)
        D.append(Dt)
    DD = np.array(D)
    arr.coords['depth'] = (('time', 'tdr_sensor'), DD)
    return arr


def calc_depth(
    sensor_type : str,
    arr : xr.DataArray,
    udg : xr.DataArray
    ) -> xr.DataArray:
    """
    Suitable for calculating depth of sensors whose position in space are not
    affected by melting and accumulation.
    """
    arr.coords['depth'] = arr['%s_install_depth' %sensor_type] + udg
    return arr


dtc1 = calc_depth('dtc1', dtc1, udg_median_xr)
ec1 = calc_depth('ec1', ec1, udg_median_xr)
# TDR depth indexing is slow because of sensitivity to melt-out, so 
# compute once then apply to others.
tdr_t = calc_depth_tdrs(tdr_t, udg_median_xr)
for tdrv in [tdr_ec, tdr_vr, tdr_vwc, tdr_perm, tdr_period]:
    tdrv.coords['depth'] = tdr_t.coords['depth']

# Example of plotting a coloured line rather than a mesh...
tdr_t1 = tdr_t.sel(sensor=1).to_dataframe(name='t')
plt.figure()
plt.scatter(tdr_t1.index, tdr_t1.depth, c=tdr_t1['t'], cmap='RdBu_r', vmin=-10, vmax=10)
