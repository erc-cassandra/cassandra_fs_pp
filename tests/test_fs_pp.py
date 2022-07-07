import pytest
import os
import pandas as pd

import cassandra_fs_pp as fspp

class TestFS:
    def test_rename_columns(self) -> None:
        fs = fspp.fs('test_data/example_fs1.toml', 'test_data/')
        fs.level0_to_level1()
        print(fs.ds_level1.columns)
        mapping = fs._rename_columns()
        print(mapping)
        # Now think of some assertion!

        assert isinstance(fs.ds_level1, pd.DataFrame)

