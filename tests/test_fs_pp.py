"""
Test suite for firn stations post-processing

Coverage currently missing:
- 'onefile' use of load_level0_dataset

Andrew Tedstone, July 2022
"""

import pytest
import os
import pandas as pd

import cassandra_fs_pp as fspp

import pdb

## Only load dataset once ...
# See also:
# https://stackoverflow.com/questions/50132703/pytest-fixture-for-a-class-through-self-not-as-method-argument
@pytest.fixture(scope='module')
def data():
    return fspp.fs('test_data/example_fs1.toml', 'test_data/')

@pytest.fixture(autouse=True, scope='class')
def test_init(request, data) -> None:
    """ Loads data and provides coverage of config file loading """
    request.cls._data = data
    assert request.cls._data.config['site'] == 'FS1_example'

class TestFS:

    def test_level0_to_level1_by_bales(self) -> None:
        """
        Provides coverage against load_level0_dataset in bales config.
        """
        self._data.level0_to_level1()
        # Check against first and last dates
        assert self._data.ds_level1.index.day[0] == 30
        assert self._data.ds_level1.index.day[-1] == 3

    def test_get_level1_default_path(self) -> None:
        p = self._data._get_level1_default_path()
        assert p == 'test_data/firn_stations/level-1/FS1_example.csv'

    def test_load_dtc_positions(self) -> None:
        pos = self._data.load_dtc_positions(key=1)
        assert pos.loc['DTC1_SensorPositions(12)'] == pytest.approx(1650)

    def test_rename_columns(self) -> None:
        assert isinstance(self._data.ds_level1, pd.DataFrame)
        mapping = self._data._define_l2_column_names()
        assert mapping['DTC1(1)'] == 'DTC1_1(C)'
        assert mapping['TDR1_VWC'] == 'TDR1_VWC(m3/m3)'
        assert mapping['DTC1(10)'] == 'DTC1_10(C)'
        assert mapping['TCDT'] == 'TCDT(m)'

    def test_normalise_udg(self) -> None:        
        assert self._data.ds_level1['TCDT'].iloc[0] == pytest.approx(2.069)
        assert self._data.ds_level1['TCDT'].iloc[-1] == pytest.approx(1.81)
        udg = self._data._normalise_udg()
        assert udg.iloc[0] == pytest.approx(0, abs=1e-2)
        assert udg.iloc[-1] == pytest.approx(0, abs=1e-2)
        self._udg = udg

    def test_filter_udg(self) -> None:
        # Case of un-normalised data - use data in level1 df
        filtered = self._data._filter_udg()
        assert filtered.iloc[0] == pytest.approx(2.069, abs=1e-2)
        assert len(filtered[filtered > 210]) == 0

    def test_filter_normalised_udg(self) -> None:
        # Case of pre-normalised data - supply the series.
        normed = self._data._normalise_udg()
        filtered = self._data._filter_udg(normed)
        assert filtered.iloc[0] == pytest.approx(0, abs=1e-2)
        assert filtered.iloc[-1] == pytest.approx(0, abs=1e-2)

    def test_calibrate_ec(self) -> None:
        ec = self._data._calibrate_ec()
        assert ec['EC(1)'].iloc[0] == pytest.approx(0)

    def test_dtc_installation_depths(self) -> None:
        pos = self._data.load_dtc_positions(key=1)
        pth, sensor, depth = self._data.config['level1_2']['dtc_info']['1']
        depths = self._data.chain_installation_depths(pos, sensor, depth)
        # Sensor 1 was buried to -0.17 m.
        assert depths[1] == pytest.approx(-0.17)
        # Chain in this test has 0.15 m spacing.
        assert depths[2] == pytest.approx(-0.17 + -0.15)

    def test_ec_installation_depths(self) -> None:
        fpos, sensor, depth = self._data.config['level1_2']['ec_info']['1']
        pos = pd.read_csv(os.path.join(self._data.data_root, fpos)).squeeze()
        depths = self._data.chain_installation_depths(pos, sensor, depth)
        # Sensor 1 was buried to -0.16 m.
        assert depths[1] == pytest.approx(-0.16)
        # Chain in this test has 0.15 m spacing.
        assert depths[2] == pytest.approx(-0.16 + -0.15)

    def test_level1_to_level2(self) -> None:
        self._data.level1_to_level2()
        assert isinstance(self._data.ds_level2, pd.DataFrame)
        assert 'TCDT(m)' in self._data.ds_level2.columns

