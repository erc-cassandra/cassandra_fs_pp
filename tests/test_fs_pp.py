import pytest
import os
import pandas as pd

import cassandra_fs_pp as fspp

class TestFS:

    def test_load_l0_file(self) -> None:
        pass
 
    def test_load_bale(self) -> None:
        pass

    def test_rename_columns(self) -> None:
        fs = fspp.fs('test_data/example_fs1.toml', 'test_data/')
        fs.level0_to_level1()
        print(fs.ds_level1.columns)
        mapping = fs._define_l2_column_names()
        print(mapping)
        # Now think of some assertion!

        assert isinstance(fs.ds_level1, pd.DataFrame)
        assert mapping['DTC1(1)'] == 'DTC1_1(C)'
        assert mapping['TDR1_VWC'] == 'TDR1_VWC(m3/m3)'
        assert mapping['DTC1(10)'] == 'DTC1_10(C)'

