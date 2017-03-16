import datetime
import pandas as pd
import pandas.io.common
import time
import os
from math import floor

from app import DATA_DIR, prepare_dirs

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


def load_files(metric_name, ts_start, ts_end=None):
    from app import b
    if not ts_end:
        # TODO: utc baby
        ts_end = int(time.time())

    ts_start_ts = relative_or_absolute_ts(ts_start)
    ts_end_ts = relative_or_absolute_ts(ts_end)

    ts_start_dt = datetime.datetime.fromtimestamp(ts_start_ts)
    ts_end_dt = datetime.datetime.fromtimestamp(ts_end_ts)

    print("start", ts_start_dt)
    print("end", ts_end_dt)

    dir = prepare_dirs(metric_name)
    # locate files
    cvsfiles = os.listdir(dir)

    flist = []
    t1 = floor(ts_start_ts/3600) * 3600
    t2 = floor(ts_end_ts/3600) * 3600
    for c in cvsfiles:
        n = int(b(c).split(b'.csv')[0])
        if n >= t1 and n <= t2:
            flist.append(c)

    print(flist)

    if not flist:
        return

    l = []
    for i in flist:
        f = open(os.path.join(dir, i), 'r')
        try:
            # TODO: we can load data and then convert dateindex.
            # df = pd.read_csv(io.StringIO(t), header=None, sep=';', index_col=[0])
            # df.index = pd.to_datetime(df.index, unit='s')
            df = pd.read_csv(f, header=None, parse_dates=[0], index_col=0, date_parser=date_parser)
            l.append(df)
        except pandas.io.common.EmptyDataError:
            pass
    df = pd.concat(l)
    print(df.describe())
    print(df)
    print("---" * 5)
    # df.sort_index()
    print(df.index)
    print("--- duplicates ----")
    # print(df.index.get_duplicates())
    print("// ----------------")
    return df

    # df2 = df.groupby(level=0).resample('1m').ffill()
    # print("--------")
    # print(df2.T)
    # print("//------")
    # return df2


if __name__ == '__main__':
    import time
    t1 = time.time()
    load_files(b'aws.regions.frankfurt.zone1.server9.load_avg_5', ts_start=0)
    print(">>> loading took>>>", time.time() - t1)