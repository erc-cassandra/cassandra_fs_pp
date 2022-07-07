# Cassandra Firn Station Post-Processing

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


### Level-2

Quantities such as TDR burial depth are derived and added to this level of data.


## Check-list for running the workflow

1. Update the TOML file of each station visited: 
    - add the latest level-0 dataset(s)
    - add new UDG position if it was changed
    - add any new TDRs
    - if a new TDR replaces an old one, note the new installation date and depth
2. Run `fs_process_l1.py`
3. Run `fs_process_l2.py`



## Credits

Some parts of workflow inspired by GEUS PROMICE AWS processing (https://github.com/GEUS-Glaciology-and-Climate/PROMICE-AWS-processing).