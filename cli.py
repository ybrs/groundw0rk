import datetime
import pandas as pd
import pandas.io.common
import time
import os
from math import floor
import time
#
import click
from utils import u
from worker import prepare_dirs, find_metrics
from config import DATA_DIR
import numpy as np

import logging
logger = logging.getLogger(__name__)

def date_parser(ts):
    return datetime.datetime.fromtimestamp(float(ts))

def secs_for_m(m):
    return m * 60

def secs_for_h(m):
    return m * 60 * 60

def secs_for_d(m):
    return m * 60 * 60 * 24

secs_for = {
    'd': secs_for_d,
    'h': secs_for_h,
    'm': secs_for_m
}

def relative_or_absolute_ts(ts):
    from app import u
    ts_end = time.time()
    if isinstance(ts, str) or isinstance(ts, bytes):
        ts_start = u(ts)
        if ts_start[0] == '-':
            last = ts_start[-1] # -1d, -2h, -1m etc.
            r = secs_for[last](int(ts_start[1:-1]))
            ts = ts_end - r
    return ts


def is_datafile(fname):
    import re
    return re.match('^[0-9\-]+\.csv', u(fname))


def in_range(min_max_tuples, wanted_min, wanted_max):
    """
    say we have these times

    1,2,3,4,5 => 1-5.csv
    6,7,8,9,10 => 6-10.csv
    11,12,13,14,15 => 11-15.csv
    16,17,18,19,20 => 16-20.csv

    and the client wants

        wants => min=2, max=12

    we return [1-5.csv, 6-10.csv, 11-15.csv]

    :param min_max_tuples list of tuples(min, max, index)
    :return list of indexes

    """
    lmin = 0
    lmax = len(min_max_tuples)
    for i, (min, max, idx) in enumerate(min_max_tuples):
        if wanted_min >= min and wanted_min <= max:
            lmin = i
        if wanted_max >= min and wanted_max <= max:
            lmax = i
    return min_max_tuples[lmin:lmax+1]


def any_(*args):
    return any(args)

def files_for_metrics(metric_name, ts_start_ts, ts_end_ts):
    from app import b

    if not ts_end_ts:
        # TODO: utc baby
        ts_end_ts = int(time.time()) + 1

    mdir = prepare_dirs(metric_name)
    # locate files
    cvsfiles = os.listdir(mdir)

    flist = []
    # round the ts to get the filenames
    t1 = floor(ts_start_ts/3600) * 3600
    t2 = floor(ts_end_ts/3600) * 3600
    ranged_files = []
    for c in cvsfiles:
        if not any_(c.endswith(b'.csv'), c.endswith(b'.csv.gz')):
            continue

        if c.endswith(b'.csv'):
            file_ext = b'.csv'

        if c.endswith(b'.csv.gz'):
            file_ext = b'.csv.gz'

        if b'-' in c:
            nmin, nmax = b(c).replace(file_ext, b'').split(b'-')
            ranged_files.append((int(nmin), int(nmax), c))
        else:
            n = int(b(c).split(file_ext)[0])
            if n >= t1 and n <= t2:
                flist.append(c)

    include_files = in_range(ranged_files, ts_start_ts, ts_end_ts)
    flist += [f for _, _, f in include_files]

    return mdir, sorted(flist)

def summarize_metric_files(metric_name, ts_start_ts, ts_end_ts):
    mdir, flist = files_for_metrics(metric_name, ts_start_ts, ts_end_ts)
    dfs = []
    for f in u(flist):
        if '-' in f:
            continue
        f = os.path.join(u(mdir), u(f))
        df = pd.read_csv(f, header=None, parse_dates=[0], index_col=0, date_parser=date_parser)
        print(">>>", f, df.index[0], df.index[-1], len(df))
        dfs.append(df)
    return dfs

def concat_files(metric_name, mdir, files):
    dfs = []
    for f in u(files):
        df = pd.read_csv(f, header=None, parse_dates=[0], index_col=0, date_parser=date_parser)
        print(">>>", f, df.index[0], df.index[-1], len(df))
        dfs.append(df)

    df = pd.concat(dfs)
    df = df.sort_index()
    df.index = df.index.astype(np.int64) / 10**9
    path = os.path.join(u(mdir), '{}-{}.csv'.format(int(df.index[0]), int(df.index[-1])))
    df.to_csv(path, header=False)
    # now we can delete files.
    for f in u(files):
        os.unlink(f)

    return df


def concat_metric_files(metric_name):
    chunk_size = 64 * 1024 # 64 kbs
    mdir, flist = files_for_metrics(metric_name, 0, 0)
    flist = sorted([f for f in u(flist) if '-' not in f])
    flist = flist[0:-1] # pop the last file
    total = 0
    chunk_files = []

    for f in u(flist):
        if '-' in f:
            continue
        f = os.path.join(u(mdir), u(f))

        fsize = os.path.getsize(os.path.join(f))
        total += fsize

        if total >= chunk_size:
            concat_files(metric_name, mdir, chunk_files)
            total = fsize
            chunk_files = []

        chunk_files.append(f)

def file_loader_csv(f):
    with open(f, 'rb') as f:
        file_content = f.read()
        for ln in file_content.split(b'\n'):
            if ln:
                ts, val = ln.split(b',')
                yield ts, val

def file_loader_csv_gz(f):
    import gzip
    with gzip.open(f, 'rb') as f:
        file_content = f.read()
        for ln in file_content.split(b'\n'):
            if ln:
                ts, val = ln.split(b',')
                yield ts, val

extension_loader = {
    'csv.gz': file_loader_csv_gz,
    'csv': file_loader_csv
}

def get_loadable_file_ext_name(f):
    for i in extension_loader.keys():
        if f.endswith(i):
            return i

def load_files(metric_name, ts_start, ts_end=None, step=None):
    """
    we pass step here just in case we try something smart when
    loading files. its not used for now.

    :param metric_name:
    :param ts_start:
    :param ts_end:
    :param step:
    :return:
    """

    if not ts_end:
        # TODO: utc baby
        ts_end = int(time.time()) + 1

    ts_start_ts = relative_or_absolute_ts(ts_start)
    ts_end_ts = relative_or_absolute_ts(ts_end)

    ts_start_dt = datetime.datetime.fromtimestamp(ts_start_ts)
    ts_end_dt = datetime.datetime.fromtimestamp(ts_end_ts)

    mdir, flist = files_for_metrics(metric_name, ts_start_ts, ts_end_ts)

    logger.info("need to analyze %s ts between %s - %s files %s", ts_start_dt, ts_end_dt, len(flist), flist)
    if not flist:
        return None

    lt = time.time()

    l = []
    for i in flist:
        if not is_datafile(i):
            continue
        f = os.path.join(u(mdir), u(i))
        fn = extension_loader[get_loadable_file_ext_name(f)]
        for ts, val in fn(f):
            l.append((ts, val))

    logger.info("loading took - %s", time.time() - lt)
    # df = pd.concat(l)
    # df = []

    lt = time.time()
    df = pd.DataFrame.from_records(l, index='index', columns=['index', 'C1'])
    df.index = df.index.astype('float64').astype('int64').astype('datetime64[s]')
    df.C1 = df.C1.astype('float64')
    logger.info("converting took - %s", time.time() - lt)

    # logger.info("loaded %s datapoints", len(df))

    # print(df.describe())
    # print(df)
    # print("---" * 5)
    # # df.sort_index()
    # print(df.index)
    # print("--- duplicates ----")
    # # print(df.index.get_duplicates())
    # print("// ----------------")
    # df2 = df.groupby(level=0).resample('1m').ffill()
    # print("--------")
    # print(df2.T)
    # print("//------")
    # return df2
    # print("0> returning - ", df, type(df))

    # TODO: we can load data and then convert dateindex.
    # df = pd.read_csv(io.StringIO(t), header=None, sep=';', index_col=[0])
    # df.index = pd.to_datetime(df.index, unit='s')
    # df = pd.read_csv(u(os.path.join(mdir, i)), header=None,
    #                  parse_dates=[0],
    #                  prefix='C',
    #                  index_col=0,
    #                  date_parser=date_parser,
    #                  # engine='c',
    #                  # memory_map=True
    #                  )

    return df


def get_column_name_from_list_or_function(list_or_fn, fname):
    if list_or_fn is None:
        return fname

    if isinstance(list_or_fn, list) or isinstance(list_or_fn, set):
        return list_or_fn.pop()

    if callable(list_or_fn):
        return list_or_fn(fname)

def load_files_m(*args, ts_start=0, ts_end=0, step=None, column_names=None):
    c = []
    for fname in args:
        ts = load_files(fname, ts_start, ts_end, step)
        ts.columns = [get_column_name_from_list_or_function(column_names, fname)]
        c.append(ts)
    return pd.concat(c, axis=1)


@click.group()
def cli():
    pass


def _metrics(customer=None):
    """
    if customer is None return everything

    :param customer:
    :return:
    """
    if not customer:
        customers = os.listdir(DATA_DIR)
    else:
        customers = []

    m = []
    for customer in customers:
        d = os.path.join(DATA_DIR, customer)
        for root, dirs, files in os.walk(d):
            if not dirs:
                m.append([customer, root.replace(d, b'')[1:].replace(b'/', b'.')])
    return m

@cli.command()
def metrics():
    for m in _metrics():
        print('{} | {}'.format(*u(m)))

@cli.command()
def metric_dirs():
    m = []
    for root, dirs, files in os.walk(DATA_DIR):
        if not dirs:
            m.append(root)
    for i in m:
        print(i.decode())

@cli.command()
def metric_stats():
    for customer, metric in _metrics():
        files = _files(metric, customer=customer)


def _files(mname, customer):
    d = prepare_dirs(mname, customer=customer)
    return os.listdir(d)


@cli.command()
def files():
    for customer, metric in _metrics():
        print('{} | {} '.format(*u(customer, metric)))
        for f in _files(metric, customer=customer):
            print(f)



if __name__ == '__main__':
    # cli.add_command(metrics)
    # cli.add_command(files)
    cli()
    # t1 = time.time()
    # load_files(b'aws.regions.frankfurt.zone1.server9.load_avg_5', ts_start=0)
    # print(">>> loading took>>>", time.time() - t1)