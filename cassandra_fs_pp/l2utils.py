"""
Utilities for working with level-2 data.

Andrew Tedstone, July 2022
"""

def calc_depth_tdrs(
    arr : xr.DataArray, 
    udg : xr.DataArray
    ) -> xr.DataArray:
    """
    Consider this problem in terms of the total distance between the UDG
    and the installed TDR.
    This total distance changes whenever the TDR is exposed at the surface and 
    then gets lowered with that surface, increasing the offset.
    We need to account for periods of surface lowering before we can calculate
    the new burial depth of the TDR.
    """
    offset = float(udg_median_xr.isel(time=0)) - arr.tdr_install_depth.values
    D = []
    for t in arr.time:
        udg = float(udg_median_xr.sel(time=t))
        Dt = udg - offset
        Dt = np.minimum(0, Dt)
        offset = np.where(Dt == 0, udg, offset)
        D.append(Dt)
    DD = np.array(D)
    arr.coords['tdr_depth'] = (('time', 'tdr_sensor'), DD)
    return arr


def calc_depth(
    sensor_type : str,
    arr : xr.DataArray,
    udg : xr.DataArray
    ) -> xr.DataArray:
    """
    Suitable for calculating depth of sensors whose position in space are not
    affected by melting and accumulation.
    """
    arr.coords['%s_depth' %sensor_type] = arr['%s_install_depth' %sensor_type] + udg
    return arr

if __name__ == '__main__':

    import matplotlib.pyplot as plt
    
    dataset['DTC1'] = calc_depth('dtc1', dataset['DTC1'], udg_median_xr)
    dataset['EC1'] = calc_depth('ec1', dataset['EC1'], udg_median_xr)
    # TDR depth indexing is slow because of sensitivity to melt-out, so 
    # compute once then apply to others.
    dataset['TDR_T'] = calc_depth_tdrs(dataset['TDR_T'], udg_median_xr)
    for tdrv in ['TDR_EC', 'TDR_VR', 'TDR_VWC', 'TDR_Perm', 'TDR_Period']:
        dataset[tdrv].coords['tdr_depth'] = dataset['TDR_T'].coords['tdr_depth']

    # Example of plotting a coloured line rather than a mesh...
    tdr_t1 = dataset.TDR_T.sel(tdr_sensor=1).to_dataframe(name='t')
    plt.figure()
    plt.scatter(tdr_t1.index, tdr_t1.tdr_depth, c=tdr_t1['t'], cmap='RdBu_r', vmin=-10, vmax=10)
