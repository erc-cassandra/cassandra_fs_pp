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

        print(conf)

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
        ds = ds.sort_index()
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
            outpath = os.path.join(self.data_root, 'level-1', '')
        self.ds_level1.to_csv(outpath)
        return


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
        dataset : str
        ) -> dict:
        """
        Stuff
        """

        args = {}
        for key in ["skiprows", "header", "index_col", "na_values"]:
            if key in self.config['level0'][dataset].keys():
                args[key] = self.config['level0'][dataset][key]
            else:
                args[key] = self.config['level0_1'][key]

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


    def _rename_columns(
        self,
        mapping : str | None=None,
        ) -> None:

        if mapping is None:
            _module_path = os.path.dirname(__file__)
            mapping = os.path.join(_module_path, 'fs_column_names.csv')

        mapping = pd.read_csv(mapping)
        mapping.index = mapping['level0']

        new_mapping = {}
        for mapp in mapping.iterrows():
            cols = self.ds.filter(regex=mapp.index, axis=1)
            if len(cols) > 1:
                for col in cols.columns:
                    # Get sensor number
                    res = re.search('\((?P<id>[0-9+])\)$', col)
                    sensor_id = res.groupdict()['id']
                    # Mapping to be supplied to df.rename()
                    renumber = re.compile(mapp.level1)
                    new_mapping[col] = renumber.sub('\*', sensor_id)
            elif len(matches) == 1:
                col = cols.columns[0]
                new_mapping[col] = mapp.level1
            else:
                continue

        return new_mapping



