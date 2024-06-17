#!/usr/bin/env python
"""
Plot all variables in firn station Level-2 NetCDF files.

For more information, run: 

    plot_L2.py -h

Andrew Tedstone, August 2022
"""
import xarray as xr
import matplotlib.pyplot as plt
import seaborn as sns
import argparse
import os
import pandas as pd
import datetime as dt

if __name__ == '__main__':

    p = argparse.ArgumentParser('Plot Level-2 data for site.')
    p.add_argument('site', type=str, help='Name of site, e.g. FS1.')
    p.add_argument('-inpath', type=str, default='.', help='Path to where dataset is stored.')
    p.add_argument('-infilename', type=str, default=None, help='Filename of the xarray dataset, defaults to <site>.nc.')
    p.add_argument('-outpath', type=str, default='.', help='Path to store the figures to, defaults to the current working directory.')

    p.add_argument('-start', default=None,
        type=lambda v: dt.datetime.strptime(v, '%Y-%m-%d'),
        help='Constrain the time range of each figure to the specified start and finish dates. Use the year-first format, e.g. 2022-06-30. Must be used in conjunction with -finish.'
    )

    p.add_argument('-finish', default=None,
        type=lambda v: dt.datetime.strptime(v, '%Y-%m-%d'),
        help='Constrain the time range of each figure to the specified start and finish dates. Use the year-first format, e.g. 2022-06-30. Must be used in conjunction with -start.'
    )

    args = p.parse_args()

    figsize = (9.0, 5.5)


    if args.infilename is None:
        args.infilename = args.site.upper() + '.nc'

    data = xr.open_dataset(os.path.join(args.inpath, args.infilename))
    if args.start is not None:
        data = data.sel(time=slice(args.start, args.finish))

    sns.set_style('whitegrid')

    def style_common(ax, main_title, sub_title):
        sns.despine(ax=ax)
        #if args.time is not None:
        #    ax.set_xlim(args.time[0], args.time[1])
        plt.legend()
        plt.title(r"$\bf{" + main_title + "}$ " + " {sname}\n{site} ".format(sname=sub_title, site=args.site),
            loc='left', fontdict={'fontsize':10})
        return

    # Set figures filename time suffix.
    if args.start is not None:
        tstr = '_' + args.start.strftime('%Y%m%d') + '_' + args.finish.strftime('%Y%m%d')
    else:
        tstr = ''

    # TDRs
    # Bounds based (at least initially) on FS1 2021-22 data.
    tdr_vars = {
        'vwc': dict(vmin=0, vmax=1.0),
        'ec': dict(vmin=0, vmax=0.01),
        't': dict(cmap='RdBu_r', vmin=-10, vmax=10),
        'perm': dict(vmin=0, vmax=50),
        'period': dict(vmin=0, vmax=3000),
        'vr': dict(vmin=0.99, vmax=1.01)
    }
    for item in ['ec', 't', 'perm', 'period', 'vr']: #vwc removed 2024-06-17 AJT

        print('TDRs: %s ...' %item)

        var_name = 'tdr_%s' %item
        sname = data[var_name].attrs['standard_name']

        # Plot coloured scatter by depth
        fig = plt.figure(figsize=figsize)
        ax = plt.subplot(111)

        for sensor in data.tdr_sensor:
            plt.scatter(
                data.time.to_index(), 
                data['tdr_depth'].sel(tdr_sensor=sensor), 
                c=data[var_name].sel(tdr_sensor=sensor).values, 
                **tdr_vars[item], label=int(sensor.values))
        
        plt.colorbar()
        plt.ylabel(data['tdr_depth'].attrs['units'])

        style_common(ax, 'Time Domain Reflectometry', sname)
        plt.savefig(os.path.join(args.outpath, '{site}_tdr_{item}_depth{t}.png'.format(site=args.site, item=item, t=tstr)), dpi=300)


        # Plot line of each sensor instead, without depth dimension.
        fig = plt.figure(figsize=figsize)
        ax = plt.subplot(111)

        for sensor in data.tdr_sensor:
            plt.plot(
                data.time.to_index(), 
                data[var_name].sel(tdr_sensor=sensor),
                label=int(sensor.values))
        try:
            plt.ylabel(data[var_name].attrs['units'])
        except KeyError:
            pass
        style_common(ax, 'Time Domain Reflectometry', sname)
        plt.savefig(os.path.join(args.outpath, '{site}_tdr_{item}{t}.png'.format(site=args.site, item=item, t=tstr)), dpi=300)

        #all_variables = data.data_vars.keys()

    # Smooth surface height so that we can add it to DTC and EC records.
    surf_loc = data['surface_height'].to_pandas()
    surf_loc = surf_loc.interpolate().rolling('24H').mean() * -1

    # Plot DTC(s)   
    def plot_dtc(dtc):
        fig = plt.figure(figsize=figsize)
        ax = plt.subplot(111)
        dtc.plot(x='time', y='%s_install_depth'%dtc.name, ax=ax)
        item = dtc.name
        sname = dtc.attrs['standard_name']
        surf_loc.plot(label='Surface location', color='black', ax=ax)
        style_common(ax, item, dtc.attrs['standard_name'])
        plt.savefig(os.path.join(args.outpath, '{site}_{n}{t}.png'.format(site=args.site, n=dtc.name, t=tstr)), dpi=300)
        return (fig, ax)

    print('DTCs ...')
    plot_dtc(data['dtc1'])
    if 'dtc2' in data.data_vars.keys():
        plot_dtc(data['dtc2'])

    # In future would also be worth exporting ~monthly average DTC plots, we just don't have the data for this yet.

    # Plot EC
    print('EC ...')
    plt.figure(figsize=figsize)
    ax = plt.subplot(111)
    data['ec1'].plot(x='time', y='ec1_install_depth', vmin=0, vmax=15, ax=ax)
    surf_loc.plot(label='Surface location', color='white', ax=ax)
    
    item ='EC1'
    style_common(ax, 'EC', data['ec1'].attrs['standard_name'])
    plt.savefig(os.path.join(args.outpath, '{site}_ec1{t}.png'.format(site=args.site, t=tstr)), dpi=300)

    # Other variables
    print('Other ...')
    for item in ['batt', 't_air', 'surface_height']:

        plt.figure(figsize=figsize)
        ax = plt.subplot(111)
        item_title = item.replace('_', r'\_')
        sname = data[item].standard_name

        data[item].plot(ax=ax, marker='.', linestyle='-', markersize=3, color='lightblue', alpha=0.5)
        as_pd = data[item].to_pandas()
        # there was an interpolate() in here
        as_pd.rolling('24H', min_periods=10, center=True).median().plot(label='24H rolling', color='tab:blue', linewidth=2, alpha=0.9)

        style_common(ax, item_title, sname)

        plt.savefig(os.path.join(args.outpath, '{site}_{item}{t}.png').format(site=args.site, item=item, t=tstr), dpi=300)

