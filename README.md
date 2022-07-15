# Cassandra Firn Station Post-Processing

This repository provides a workflow for converting the raw 'engineering' datasets
recorded by Cassandra firn stations into calibrated, physical datasets suitable
for further analysis.


## Files layout

Processing is based around the concept of a `data_root`. For Cassandra, `data_root` usually corresponds to the main project folder on `bigdata`.

The `data_root` must contain the following directories:

- `ec_calibration` (required if processing EC chain measurements)
- `firn_stations`
    + `level-1` -> level-1 outputs are saved to here.
    + `level-2` -> level-2 outputs are saved to here.
- Folders containing batches of field-collected data - "subdatasets".

Site-specific metadata files do not need to be kept in the `data_root`, but
this is usually sensible. Find the up-to-date metadata files in `firn_stations/ppconfig/`.


## Metadata files

Post-processing essentially uses metadata about each firn station to bring its
measurements into the physical domain. A complete and self-documenting example can be found in `test_data`. These files are in [TOML format v1.0.0](https://toml.io/en/v1.0.0.).

Note that the schema of the metadata files is currently not enforced before the workflow
begins. If a key is missing, the first you will know about this is when the 
workflow fails to find that key in the metadata file!


## Check-list for running the workflow

1. Check that files are in-place according to information above.
2. Update the TOML file of each station visited: 
    - add the latest level-0 dataset(s)
    - add new UDG position if it was changed
    - add any new TDRs
    - if a new TDR replaces an old one, note the new installation date and depth
3. Run `bin/fs_process_l1.py`
4. Run `bin/fs_process_l2.py`


## Data levels

### Level-0

Refers to raw files straight from the station loggers.

At the moment only directly-downloaded data are supported, not transmitted.

Depending on the logger setup, level-0 data may consist of either a single file, 
or of multiple files. This workflow refers to the latter as 'bales' of files, 
generally one bale per station visit.


### Level-1

Data from station has been concatentated into a single continuous file. Column
names have been renamed.

These data are output to csv files.


### Level-2

Quantities such as TDR burial depth are derived and added to this level of data.

These data are output to NetCDF files. Note that various metadata are appended
to the NetCDF files; to change these settings make edits directly to `bin/fs_process_l2.py`.

Helper utilities for working with Level-2 data are available (`l2utils`). The main functionality at the moment is to assist in deriving 'instantaneous' burial depth as a function of UDG-derived surface height change.


## Development

Clone and install in-place:

    pip install -e .

Branch before making changes.

A test suite is set up for this package. Run `pytest` in the main directory, it
will find the tests automatically. Make sure that all the tests pass. Also make
sure to add any tests needed to provide coverage of your development.

Once the tests pass, merge your branch into `main`.


## Credits

Some parts of workflow inspired by GEUS PROMICE AWS processing (https://github.com/GEUS-Glaciology-and-Climate/PROMICE-AWS-processing).


## Various notes

In practical terms, depth is perhaps another variable rather than a coordinate ...
(even if not the case in theory)

can UDG-fixed depths be discretised onto a coordinate that is still time-varying?

Could fix by setting 'depth' to 'sensor_depth' (matching 'sensor_install_depth'),
then it just wouldn't be possible to select on depth key across multiple
data arrays. (e.g. T where chainEC > 1)
depth of each sensor varies by the same amount at each time step, 
it's just their spacing which is different - but not that different
between EC and DTC. It's only TDR where this would be a problem. So how about
just ensuring that non-valid depths are set to nan? (then look up nearest?)

Problem is that the measurements are not on a regular grid once they've been corrected to UDG.