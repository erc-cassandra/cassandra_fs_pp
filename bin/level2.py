""" 
Level-1 csv file to Level-2 netCDF

"""

import pandas as pd
import xarray as xr
import numpy as np
import os

import cassandra_fs_pp as fs_pp

import matplotlib.pyplot as plt


data_root = '/Volumes/Science/Geosciences/Cassandra'
config_file = os.path.join(data_root, 'firn_stations/ppconfig', 'fs1.toml')

fs = fs_pp.fs(config_file, data_root)
fs.load_level1_dataset('/Users/tedstona/Desktop/fs1_nosort.csv')
fs.ds_level1.index.name = 'time'

#dtc1_positions = pd.read_csv(fs.config['level0_1'])

# Organise UDG data
udg_norm = fs._normalise_udg()
udg_norm = fs._filter_udg(udg_norm)
udg_median = udg_norm.rolling('3D', center=True).median()
udg_median_xr = xr.DataArray(udg_median, dims=['time'])


## Organise TDR data
# To do : these items need to be added from config file
tdr_labels = [1,2,3]
tdr_inst_depths = [-0.48, -1.15, -1.36]

tdr_t = xr.DataArray(
    fs.ds_level1.filter(like='_T'),
    dims=['time', 'sensor'],
    coords={'sensor':tdr_labels, 'time':fs.ds_level1.index, 'installation_depth':('sensor',tdr_inst_depths)}
)

# To do : write to netcdf


############
## LOADING FUNCS
# To provide as part of level-2 data loading suite:

def add_depth_tdrs():
    tdr_t.coords['depth'] = tdr_t.installation_depth + udg_median_xr
    tdr_t.coords['depth'] = tdr_t.coords['depth'].where(tdr_t.coords['depth'] <= 0, 0)

