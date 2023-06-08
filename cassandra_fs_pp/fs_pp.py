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
import glob

REQUIRED_CONFIG_KEYS = ['site']
REQUIRED_CONFIG_L0_KEYS = ['header', 'skiprows', 'index_col']


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
        self,
        add_latest_serviced : bool=True
        ) -> pd.DataFrame:
        """ 
        Transform Level-0 data to Level-1.

        Sets self.ds_level1.

        :param add_latest_serviced: if True, append the data from the first *MainTable*
        file found in the `serviced` sub-directory of the latest subdataset.
        """
        store = []
        nds = len(self.config['level0'])
        n = 1
        for dataset in self.config['level0']:
            if n == nds:
                serviced = True
            else:
                serviced = False
            sds = self.load_level0_dataset(dataset, add_serviced=serviced)
            store.append(sds)
            n += 1

        ds = pd.concat(store, axis=0)

        # Check for entire columns of NANs and remove them
        ds = ds.dropna(how='all', axis='columns')

        # Checking for duplicates...
        print('%s records before removal of duplicates' %len(ds))
        # Delete any duplicates which have slipped in due to non-cleared MainTables
        ds = ds.drop_duplicates()
        print('%s records after duplicated rows dropped' %len(ds))
        # Delete any temporal duplicates
        ds = ds[~ds.index.duplicated()]
        print('%s records after duplicated indexes dropped' %len(ds))
        self.ds_level1 = ds
        return ds


    def load_level0_dataset(
        self,
        dataset : str,
        add_serviced : bool=False
        ) -> pd.DataFrame:
        """
        Load a complete dataset (single file or bales) into memory.

        :param dataset: name of level-0 dataset as listed in TOML file.
        :param add_serviced: for "onefile" datasets this function can
        also look for a subfolder named `serviced`, located within config 
        option `subpath`. It will add data saved here, the idea being to 
        concatenate the newly-read-out data downloaded at the end of the servicing visit.
        """
        ds_config = self.config['level0'][dataset]        
        ds_load_opts = self._setup_level0_options(dataset)

        if ds_config['type'] == 'bales':
            ds = self._concat_bale(dataset, ds_load_opts)
        elif ds_config['type'] == 'onefile':
            p = os.path.join(self.data_root, dataset, ds_config['subpath'])
            ds = self._load_level0_file(p, ds_load_opts)

        if add_serviced:
            if ds_config['type'] == 'onefile':
                subpath_root = os.path.split(ds_config['subpath'])[0]
            elif ds_config['type'] == 'bales':
                # Bales have to have the subfolder containing all the bale files specified.
                split = ds_config['subpath'].split('/')
                if len(split) > 1:
                    subpath_root = os.path.join(*split[:-1])
                else:
                    subpath_root = ''

            serviced_root = os.path.join(self.data_root, dataset, subpath_root, 'serviced')
            if os.path.exists(serviced_root):
                files = glob.glob(os.path.join(serviced_root, '*MainTable*'))
                if len(files) == 1:
                    print('Found post-servicing dataset %s' %files[0])
                    ds2 = self._load_level0_file(files[0], ds_load_opts)
                    ds = pd.concat((ds, ds2), axis=0)

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

        # 'Universal' options which always need to be set
        for key in ["skiprows", "header", "index_col", "na_values", "sep"]:
            args[key] = self.config['level0_1'][key]
            if dataset is not None:
                if key in self.config['level0'][dataset].keys():
                    args[key] = self.config['level0'][dataset][key]

        # Specific options which may be set for individual datasets
        if dataset is not None:
            for key in ["nrows"]:
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
        data = data.drop_duplicates()
        return data


    def level1_to_level2(
        self
        ) -> None:
        """
        Process Level-1 data to Level-2
        """

        # Apply data ranges directly to level1 (not level2)
        self.ds_level1 = self._apply_valid_data_ranges(self.ds_level1)

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

        level2 = level2.drop_duplicates()

        # Set to object
        self.ds_level2 = level2
        return 


    def _get_level2_default_path(self) -> None:
        """
        Default location of level-2 dataset.
        """
        return os.path.join(self.data_root, 'firn_stations/level-2', self.config['site'] + '.nc')


    def _apply_valid_data_ranges(
        self,
        df : pd.DataFrame,
        spec_file : str | None=None,
        ) -> pd.DataFrame:

        if spec_file is None:
            _module_path = os.path.dirname(__file__)
            spec_file = os.path.join(_module_path, 'valid_data_ranges.toml')

        with open(spec_file, "rb") as f:
            spec = tomli.load(f)
    
        print('Restricting to valid data ranges...')
        for col in spec:
            if col[0:3].upper() == 'TDR':
                var = col[4:]
                cs = df.filter(regex='TDR[0-9]*\_%s'%var).columns
            elif col[0:2].upper() == 'EC':
                cs = df.filter(regex='EC\([0-9]*\)').columns
            else:
                cs = [col]
            vmin, vmax = spec[col]    
            for c in cs:
                print('    %s (%s, %s)'%(c, vmin, vmax))
                df.loc[:,c] = df.loc[:,c].where(df.loc[:,c] <= vmax)
                df.loc[:,c] = df.loc[:,c].where(df.loc[:,c] >= vmin)

        return df


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
                    # Get sensor number
                    # First try array-type variable
                    res = re.search(r'\((?P<id>[0-9]+)\)$', col)
                    if res == None:
                        # If that doesn't work, try generic multiple type
                        res = re.search(r'[A-Za-z]+(?P<id>[0-9]+)\_', col)
                    if res == None:
                        print('Could not find sensor ID.')
                        raise ValueError

                    sensor_id = res.groupdict()['id']

                    renumber = re.compile(r'\*')
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
            _, filename, _, _ = self.config['level1_2']['dtc_info'][str(key)]
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
        else:
            udg = copy.deepcopy(udg)

        changes = self.config['level1_2']['udg_height_change']
        if len(changes) == 1:
            changes.append(-999)

        print('Normalising UDG ...')
        first = True
        for change in changes:
            if change == -999:
                break

            if len(change) == 2:
                date, user_height_change = change
            else:
                date = change[0]
                user_height_change = np.nan

            if first:
                # This date is when UDG was installed for first time. Zero-off.
                height_change = user_height_change
                print('\t %s: Normalising to height (%s m) at first installation.' %(date, height_change))
            else:
                if np.isnan(user_height_change):
                    # These dates denote when the UDG installation height was changed.
                    # We correct for this 'automatically' using only the UDG data.
                    period_before_change_start = date - pd.Timedelta(days=1)
                    period_before_change_end = date - pd.Timedelta(hours=4)
                    udg_height_before_change = np.round(udg.loc[period_before_change_start:period_before_change_end].median(), 2)
                    udg_height_after_change = np.round(udg.loc[date.isoformat():(date+pd.Timedelta(days=1)).isoformat()].median(), 2)
                    height_change = np.round(udg_height_after_change - udg_height_before_change, 2)
                    print('\t %s: Normalised height, pre-change: %s m. New unnormalised height: %s m. Subtracting %s m.' %(date, udg_height_before_change, udg_height_after_change, height_change))
                else:
                    height_change = user_height_change
                    print('\t %s: Applying user-provided height change of %s.' %(date, height_change))
            
            # In here we need to index with 'inexact' strings rather than Timestamps,
            # which are always treated as exact, so cause this operation to fail if the precise
            # Timestamp is not an index in the DataFrame.
            udg.loc[date.isoformat():] -= height_change
            
            actual_start_date = udg.loc[date.isoformat():].index[0]
            
            first = False

        return udg


    def _filter_udg(
        self,
        udg : pd.Series | None=None,
        q : pd.Series | None=None,
        med_window : str='2D',
        threshold : float=0.5
        ) -> pd.Series:
        """
        Apply filtering strategies to UDG: remove Campbell Sci bad values and
        use temporal median filtering to identify and remove other bad values.

        No data modification!

        Note that the UDG record is likely to benefit from further smoothing,
        depending on the desired application.

        :param udg: Series of UDG values. If not provided defaults to self.ds_level1.
        :param q: Series of UDG quality values. If not provided defaults to self.ds_level1.
        :param med_window: temporal window over which to calculate rolling median.
        :param threshold: absolute difference from median to tolerate.

        """
        if udg is None:
            udg_key = self.config['level0_1']['udg_key']
            udg = copy.deepcopy(self.ds_level1[udg_key])

        if q is None:
            q_key = 'Q'
            q = copy.deepcopy(self.ds_level1[q_key])
        else:
            q = copy.deepcopy(q)

        q_nans = np.sum(q.isna())
        if q_nans > 0:
            print('WARNING: %s NaNs found in UDG Q column, indicating no Q value recorded. Quality checks will not be made on these UDG rows.' %q_nans)
            q = np.where(np.isnan(q), 150, q)

        # Only retain data with quality flag according to SR50A manual
        udg = udg.where(q >= 150).where(q <= 210)

        # Interpolate to make the Series monotonic - primarily needed due to 
        # different sampling rates in summer versus winter.
        # First calculate most likely appropriate frequency in minutes
        r = (udg.index[1:] - udg.index[0:-1])
        freq = pd.DataFrame(r).mode().iloc[0,0].total_seconds() / 60
        udg_reg = udg.resample('%smin'%freq).ffill(limit=3)

        # Remove high-frequency "problems"
        med = udg_reg.rolling(med_window).median()
        filt = udg_reg.where(np.abs(med-udg_reg) < threshold)
 
        # Revert to original sampling frequency
        filt_orig_freq = filt[udg.index]
        return filt_orig_freq


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
            except KeyError:
                print('No cal. data for %s, using average of other sensors' %column.name)
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

        just_ec = self.ds_level1.filter(regex=r'EC\([0-9]+\)', axis=1)
        ec_ms = just_ec.apply(_apply_cal)

        return ec_ms


    def _calc_depth_tdr(
        self,
        tdr : int | str,
        udg
        ) -> np.array:
        """ Calculate depth of single TDR
        """
        
        install_date, install_depth = self.config['level1_2']['tdr_info'][str(tdr)]
        
        udg_at_install = float(udg.loc[install_date.isoformat():].iloc[0])
        nearest_udg_date = udg.loc[install_date.isoformat():].index[0]
        if nearest_udg_date != pd.Timestamp(install_date):
            print('WARNING: TDR %s depth calculation: No UDG data available at \
specified installation date of %s, using next record (%s) instead.'%(tdr, install_date, nearest_udg_date))

        offset =  install_depth 

        # Select UDG data, starting at install date, and normalise the record 
        # relative to the installation reading
        udg = udg.loc[install_date:] - udg_at_install
        # Now, +ve UDG means net surface melting compared to installation
        # And -ve UDG means net surface accumulation compared to installation
        
        D = []
        for ix,udgt in udg.iteritems():
            Dt = udgt + offset
            Dt = np.minimum(0, Dt)
            offset = np.where(Dt == 0, (udgt*-1), offset)
            D.append(Dt)

        DD = np.array(D)
        DD = pd.Series(DD, index=udg.index, name='TDR%s_Depth'%tdr)
        return DD



