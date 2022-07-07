import pytest

import fs_pp

class TestFS:
    def test_rename_columns(self) -> None:
        fs = fs_pp('../test_data/example_fs1.toml', '../test_data/')
        fs.level0_to_level1()
        print(fs.columns)
        fs._rename_columns()
        print(fs.columns)
        # Now think of some assertion!

        assert isinstance(fs, pd.DataFrame)

