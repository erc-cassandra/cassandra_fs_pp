"""
CASSANDRA Firn Station post-processing

Configuration is stored in TOML markup files conforming to v1.0.0.

Andrew Tedstone, July 2022
"""
from __future__ import annotations

import pandas as pd
import tomli
import os
import logging
import re
import copy
import numpy as np

REQUIRED_CONFIG_KEYS = ['site']
REQUIRED_CONFIG_L0_KEYS = ['header', 'skiprows', 'index_col']


def pairwise(lst):
    """ yield item i and item i+1 in lst. e.g.
        (lst[0], lst[1]), (lst[1], lst[2]), ..., (lst[-1], None)
    """
    if not lst: return
    #yield None, lst[0]
    for i in range(len(lst)-1):
        yield lst[i], lst[i+1]
    yield lst[-1], None


class fs():
    
    config = None
    data_root = None
    ds_level1 = None

    def __init__(
        self,
        config_file : str,
        data_root: str
        ) -> None:
        """
        Initialise the firn station object.

        :param config_file: the path/filename to the TOML file describing this station.
        :param data_root: the path to the root of where the level-0,1,2 data are stored.
        """
        self._load_config(config_file)
        self.data_root = data_root
        return


    def _load_config(
        self, 
        config_file : str
        ) -> None:
        """ 
        Import config file.

        :param config_file: the path/filename to TOML config file. 
        """
        with open(config_file, "rb") as f:
            conf = tomli.load(f)

        # Check required fields are present        
        for field in REQUIRED_CONFIG_KEYS:
            assert(field in conf.keys())

        for field in REQUIRED_CONFIG_L0_KEYS:
            assert(field in conf['level0_1'].keys())

        self.config = conf
        return
      

    def level0_to_level1(
        self
        ) -> pd.DataFrame:
        """ 
        stuff
        """
        store = []
        for dataset in self.config['level0']:
            sds = self.load_level0_dataset(dataset)
            store.append(sds)

        ds = pd.concat(store, axis=0)
        #ds = ds.sort_index()
        self.ds_level1 = ds
        return ds


    def load_level0_dataset(
        self,
        dataset : str
        ) -> pd.DataFrame:
        """
        Load a complete dataset (single file or bales) into memory.

        :param dataset: name of level-0 dataset as listed in TOML file.
        """
        ds_config = self.config['level0'][dataset]        
        ds_load_opts = self._setup_level0_options(dataset)

        if ds_config['type'] == 'bales':
            ds = self._concat_bale(dataset, ds_load_opts)
        elif ds_config['type'] == 'onefile':
            ds = self._load_level0_file(ds_load_opts)

        return ds


    def write_l1(
        self,
        outpath: str | None = None
        ) -> None:
        """
        Write Level-1 dataset to disk as CSV. 
        """
        assert(type(self.ds_level1) is pd.DataFrame)
        if outpath is None:
            outpath = self._get_level1_default_path()
        self.ds_level1.to_csv(outpath)
        return


    def load_level1_dataset(
        self,
        dataset : str | None=None
        ) -> pd.DataFrame():
        """
        Load a Level-1 processed file.
        Primarily useful if seeking to run only Level-2 processing.

        :param dataset: path/filename of file to load. If None then attempts
        to find from the data_root.
        """
        if dataset is None:
            dataset = self._get_level1_default_path()
        self.ds_level1 = pd.read_csv(dataset, parse_dates=True, 
            index_col=self.config['level0_1']['index_col'])
        return


    def _get_level1_default_path(self) -> None:
        """
        Default location of level-1 dataset.
        """
        return os.path.join(self.data_root, 'firn_stations/level-1', self.config['site'] + '.csv')


    def _concat_bale(
        self,
        bale_dataset : str,
        load_opts : dict
        ) -> pd.DataFrame:
        """
        Join together 'bales' of dat files into a common dataset.

        :param bale_dataset: the folder name of the dataset.
        """

        bale_config = self.config['level0'][bale_dataset]

        pth_root = os.path.join(self.data_root, bale_dataset, bale_config['subpath'])

        bs = bale_config['bales_start']
        be = bale_config['bales_stop']
        store = []
        for i in range(bs, be+1):
            pth = os.path.join(pth_root, 'MainTable%s.dat' %i)
            data = self._load_level0_file(pth, load_opts)
            store.append(data)

        bale = pd.concat(store, axis=0)
        return bale


    def _setup_level0_options(
        self,
        dataset : str | None=None
        ) -> dict:
        """
        Stuff
        """

        args = {}
        for key in ["skiprows", "header", "index_col", "na_values", "sep"]:
            args[key] = self.config['level0_1'][key]
            if dataset is not None:
                if key in self.config['level0'][dataset].keys():
                    args[key] = self.config['level0'][dataset][key]

        return args


    def _load_level0_file(
        self,
        filename : str,
        load_opts : dict
        ) -> pd.DataFrame:
        """
        Load a Campbell level-0 .dat file into a pandas DataFrame.

        :param filename: file path and name of the file to open.
        :param load_opts: dict of options to pass to pd.read_csv.
        """

        data = pd.read_csv(filename, parse_dates=True, **load_opts)
        return data


    def level1_to_level2(
        self
        ) -> None:
        """
        Process Level-1 data to Level-2
        """

        # Apply modifications to dataframe
        # Start with a copy of level1 ds
        level2 = copy.copy(self.ds_level1)

        # Delete unwanted columns
        for c in self.config['level1_2']['remove_columns']:
            level2 = level2.drop(c, axis='columns')

        # Rename
        new_col_names = self._define_l2_column_names()
        level2 = level2.rename(new_col_names, axis='columns')

        # UDG
        l2_udg = self._normalise_udg()
        l2_udg = self._filter_udg(l2_udg)
        level2['TCDT(m)'] = l2_udg

        # Overwrite mV EC with mS EC
        l2_ec = self._calibrate_ec()
        # df.assign() requires a dict of Series!
        level2 = level2.assign(**{c:l2_ec[c] for c in l2_ec.columns})

        # Set to object
        self.ds_level2 = level2
        return 


    def _define_l2_column_names(
        self,
        mapping_file : str | None=None,
        ) -> dict:
        """
        A level-2 operation.
        Creates mapping dict old:new to be supplied to df.rename().

        :param mapping_file: filename of old->new regexes. By default
        uses the file contained within the repository.

        """

        if mapping_file is None:
            _module_path = os.path.dirname(__file__)
            mapping_file = os.path.join(_module_path, 'fs_column_names.csv')

        mapping = pd.read_csv(mapping_file)
        mapping.index = mapping['level0']

        new_mapping = {}
        for ix, mapp in mapping.iterrows():
            filtered = self.ds_level1.filter(regex=ix, axis=1)
            cols = filtered.columns
            
            # 'Array'-type variables (e.g. DTC)
            if len(cols) > 1:
                for col in cols:
                    #print(col)
                    # Get sensor number
                    # First try array-type variable
                    res = re.search('\((?P<id>[0-9]+)\)$', col)
                    if res == None:
                        # If that doesn't work, try generic multiple type
                        res = re.search('[A-Za-z]+(?P<id>[0-9]+)\_', col)
                    if res == None:
                        print('Could not find sensor ID.')
                        raise ValueError

                    sensor_id = res.groupdict()['id']

                    renumber = re.compile('\*')
                    new_mapping[col] = renumber.sub(sensor_id, mapp.loc['level2'])
            elif len(cols) == 1:
                col = cols[0]
                new_mapping[col] = mapp.iloc[1]
            else:
                continue

        return new_mapping


    def load_dtc_positions(
        self,
        key : str | None=None,
        filename : str | None=None,
        check_length : bool=True
        ) -> pd.Series:
        """
        Load sensor positions reported by Recite.
        Check that number matches the number of sensors in a string.

        :param key: the key from level1_2 which contains file info
        :param filename: load this filename directly
        :param check_length: if true, check length of reported chain positions against number of data columns.
        """

        if key is not None and filename is not None:
            raise ValueError('Provide only one of `key` or `filename`')
        elif key is None and filename is None:
            raise ValueError('Provide one of `key` or `filename`.')
        elif key is not None:
            filename = self.config['level1_2']['dtc_info'][str(key)][0]
            filename = os.path.join(self.data_root, filename)

        opts = self._setup_level0_options()
        pos = pd.read_csv(filename, **opts)
        pos = pos.drop('RECORD', axis='columns')
        pos = pos.iloc[0]

        if check_length:
            dtc_id = pos.index[0][0:4]
            ndata = len(self.ds_level1.filter(like='%s' %dtc_id, axis='columns').columns)
            assert ndata == len(pos)

        return pos


    def chain_installation_depths(
        self,
        sensor_positions : pd.Series, 
        first_sensor : int,
        depth : float
        ) -> dict:
        """
        Relevant for all chains which are fixed in position and with defined spacing.
        Returns the installation depth of each sensor in the chain.

        :param sensor_positions: in positive millimetres, as per format provided by load_dtc_positions
        :param first_sensor : int, ID of sensor for which depth was measured
        :param depth : float, depth of sensor with ID first_sensor (-ve if below ground)
        """
        # Convert from +ve mm to -ve metres
        sensor_positions = sensor_positions * 1e-3 * -1
        print(sensor_positions)
        ref_sensor_position = sensor_positions.iloc[first_sensor - 1]
        ref_sensor_depth = depth
        sensor_depths_t0 = copy.deepcopy(sensor_positions)
        #                    Negatives           Negative            Negative
        sensor_depths_t0 = sensor_depths_t0 - ref_sensor_position + depth
        sensor_depths_t0.index = np.arange(1, len(sensor_depths_t0)+1)
        return sensor_depths_t0.to_dict()
      

    def _normalise_udg(
        self,
        udg : pd.Series | None=None
        ) -> pd.Series:
        """
        Stitch jumps in UDG record
        Zeroes off the series relative to the first record.

        Requires level-1 data in memory.
        """
        if udg is None:
            udg_key = self.config['level0_1']['udg_key']
            udg = copy.deepcopy(self.ds_level1[udg_key])

        changes = self.config['level1_2']['udg_to_surface']
        if len(changes) == 1:
            changes.append(-999)
        changes_it = pairwise(changes)

        for change, next_change in changes_it:
            if change == -999:
                break
            date, new_height = change
            if next_change is None or next_change == -999:
                udg.loc[date:] -= new_height
            else:
                next_date, next_height = next_change
                udg.loc[date:next_date] -= new_height

        return udg


    def _filter_udg(
        self,
        udg : pd.Series | None=None,
        med_window : str='1D',
        threshold : float=0.1
        ) -> pd.Series:
        """
        Apply filtering strategies to UDG: remove Campbell Sci bad values and
        do temporal median filtering.

        Always relies on 'Q' column of self.ds_level1.

        Note that the UDG record is likely to benefit from further smoothing,
        depending on the desired application.

        :param udg: Series of UDG values. If not provided defaults to self.ds_level1.
        :param med_window: temporal window over which to calculate rolling median.
        :param threshold: absolute difference from median to tolerate.

        """
        if udg is None:
            udg_key = self.config['level0_1']['udg_key']
            udg = copy.deepcopy(self.ds_level1[udg_key])

        # Only retain data with quality flag according to SR50A manual
        udg = udg.where(self.ds_level1['Q'] > 150).where(self.ds_level1['Q'] <= 210)
        # Remove high-frequency "problems"
        med = udg.rolling(med_window).median()
        filt = udg.where(np.abs(med-udg) < threshold)
        return filt


    def _calibrate_ec(
        self,
        cal_file : str | None=None,
        transform : bool=True
        ) -> pd.DataFrame:
        """ 
        Convert EC sensor millivolts to physical units using linear regression.

        :param cal_file: path to calibration coefficients file.
        :param transform: if true, do (1-EC values)
        """

        assert isinstance(self.ds_level1, pd.DataFrame)

        def _apply_cal(column):
            try:
                m = calibrations.loc[column.name, 'm']
                c = calibrations.loc[column.name, 'c']
            except IndexError:
                print('No cal. data for %s, using average of other sensors' %column)
                m = calibrations['m'].mean()
                c = calibrations['c'].mean()
            if transform:
                column = 1 - column
            ec = m * column + c
            return ec

        if cal_file is None:
             cal_file = os.path.join(
                self.data_root, 
                'ec_calibration', 
                'calibration_coefficients_%s_c0.csv' %self.config['site'].upper()
            )
        calibrations = pd.read_csv(cal_file, index_col=0)

        just_ec = self.ds_level1.filter(regex='EC\([0-9]+\)', axis=1)
        ec_ms = just_ec.apply(_apply_cal)

        return ec_ms
