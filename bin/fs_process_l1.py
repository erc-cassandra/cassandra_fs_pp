#!/usr/bin/env python
"""
Process level-0 data up to level-1 status.

Andrew Tedstone, July 2022.
"""
import os
import argparse

import cassandra_fs_pp as fs_pp

if __name__ == '__main__':

    parser = argparse.ArgumentParser('Process level-0 data up to level-1 status.')

    parser.add_argument('site', type=str, help='Name of site, normally corresponding to TOML metadata file.')

    cwd = os.getcwd()
    parser.add_argument('-data_root', type=str, default=cwd,
        help='Path to root of data (see README), defaults to current directory.')

    parser.add_argument('-metafile', type=str, default=None, 
        help='Path to metadata TOML file, normally set automatically.')

    parser.add_argument('-outfile', type=str, default=None, 
        help='Path to output CSV, normally set automatically.')

    parser.add_argument('-ow', action='store_true',
        help='If provided, forces over-write of existing file.')
    
    args = parser.parse_args()

    if args.metafile is None:
        args.metafile = os.path.join(args.data_root, 
            'firn_stations/ppconfig', 
            '%s.toml' %args.site)

    fs = fs_pp.fs(args.metafile, args.data_root)

    # Check for existence of Level-1 file.
    if not args.ow:
        if args.outfile is None:
            p = fs._get_level1_default_path()
        else:
            p = args.outfile
        check = os.path.exists(p)

        if check:
            raise IOError('The Level-1 output file for this site already exists. To overwrite, specify -ow.')

    fs.level0_to_level1()

    fs.write_l1(outpath=args.outfile)

    