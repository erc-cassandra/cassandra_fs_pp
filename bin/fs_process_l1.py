import fs_pp

if __name__ == '__main__':

    config_file = '/Users/tedstona/scripts/cassandra_fs/site.toml/fs1.toml'
    data_root = '/Volumes/Science/Geosciences/Cassandra'
    fs = fs_pp.fs(config_file, data_root)
    fs.level0_to_level1()
    fs.write_l1(outpath='~/Desktop/fs1.csv')

    #addArg('-reprocess') # if selected, reprocess all data in config file. otherwise, append to main store.
# 
# add option to append transmitted data set, perhaps with a TX column to indicate this?

    #addArg('l0path')
    #addArg('siteconfigpath')



    # consider npdh needed for melt to occur - see old scripts - will help with WV planning

    