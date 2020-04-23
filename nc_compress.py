#!/usr/bin/env python
import os
from glob import glob
from subprocess import Popen, PIPE
import click

import xarray as xr
import dask

dask.config.set({'distributed.dashboard.link': '/proxy/{port}/status'})

def _nco(cmd):
    """Interface to NCO"""
    p = Popen(
        ' && '.join(['module load nco', ' '.join(cmd)]),
        stdout=PIPE,
        stderr=PIPE,
        shell=True
    )

    stdout, stderr = p.communicate()
    if p.returncode != 0:
        print(stdout.decode('UTF-8'))
        print(stderr.decode('UTF-8'))
        raise


def nc_compress(ncfile):
    _nco(['ncks', '-O', '-4', '-L', '1', ncfile, ncfile])

def _not_compressed(ncfile):
    l_not_compressed = True
    with xr.open_dataset(ncfile) as ds:
        for v in ds.variables:
            if ds[v].encoding['zlib']:
                l_not_compressed = False

    print(f'{ncfile}: {l_not_compressed}')

    return l_not_compressed

def _sum_file_size(files):
    if not isinstance(files, list):
        files = [files]

    size = 0
    for f in files:
        statinfo = os.stat(f)
        size += statinfo.st_size
    return '%0.2fT'%(size/1024**4)

def dask_cluster(njobs=8):
    from ncar_jobqueue import NCARCluster
    from dask.distributed import Client

    cluster = NCARCluster()
    cluster.scale(njobs)
    client = Client(cluster) # Connect this local process to remote workers
    return cluster, client

@click.command()
@click.option('-r', '--recursive', default=False, is_flag=True)
@click.option('--dask-jobs', 'dask_jobs', default=0)
@click.option('--pre-check', 'pre_check_compression', default=True, is_flag=True)
@click.argument('directory')

def main(directory, recursive=False, dask_jobs=0, pre_check_compression=True):
    """compress netcdf files in a directory"""

    directory = os.path.abspath(directory)
    if recursive:
        w = os.walk(directory)
        ncfiles = []
        for root, dirs, files in w:
            for f in files:
                if os.path.splitext(f)[1] == '.nc':
                    ncfiles.append(os.path.join(root,f))
    else:
        ncfiles = sorted(glob(os.path.join(directory, '*.nc')))

    if not ncfiles:
        print('No netCDF files found.')
        return

    if dask_jobs > 0:
        print('spinning up cluster')
        cluster, client = dask_cluster(njobs=dask_jobs)
        print('-'*10)
        print('cluster')
        print(cluster)
        print('-'*10)

        print('-'*10)
        print('client')
        print(client)
        print(client.cluster.dashboard_link)
        print('-'*10)
        compress_func = dask.delayed(nc_compress)
        pre_check_func = dask.delayed(_not_compressed)
    else:
        compress_func = nc_compress
        pre_check_func = _not_compressed

    if pre_check_compression:
        print('performing pre-check')
        l_not_compressed = []
        for f in ncfiles:
            l_not_compressed.append(pre_check_func(f))
        if dask_jobs > 0:
            l_not_compressed = dask.compute(*l_not_compressed)

        ncfiles = [f for f, l in zip(ncfiles, l_not_compressed) if l]

    print(f'compressing {len(ncfiles)} files')

    size_i = _sum_file_size(ncfiles)
    print('Total file size: '+size_i)

    res = []
    for f in ncfiles:
        res.append(compress_func(f))

    if dask_jobs > 0:
        res = dask.compute(*res)
        cluster.close()
        client.close()

    print('done.')
    size_f = _sum_file_size(ncfiles)
    print('Total file size initial: '+size_i)
    print('Total file size final: '+size_f)


if __name__ == '__main__':
    main()
