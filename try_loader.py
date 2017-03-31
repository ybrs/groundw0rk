"""
this is just to see if things work
"""
from cli import load_files
import time
t1 = time.time()
df = load_files('aws.regions.amsterdam.zone9.server1.load_avg_5',
                ts_start=0, step=None)
# print(df.describe())
print(len(df), "took - ", time.time() - t1)

"""
with gzip
(env3) $ du -hs ../data/customer_1/aws/regions/amsterdam/zone9/server1/load_avg_15
 94M	../data/customer_1/aws/regions/amsterdam/zone9/server1/load_avg_15

(env3) $ du -hs ../data/customer_1/aws/regions/amsterdam/zone9/server1/load_avg_5/
 24M	../data/customer_1/aws/regions/amsterdam/zone9/server1/load_avg_5/

with gz:
    3230445 took -  11.010967016220093

without gz:
    3230445 took -  10.704976797103882

# simple csv/gz read
took 3.5175700187683105
# without gz
took 3.2008490562438965

parquet
- with gzip writes
In [17]: %time write('outfile.parq', df, compression='gzip', write_index=True)
CPU times: user 51.8 s, sys: 669 ms, total: 52.5 s
Wall time: 58.4 s
size: 22M

- with snappy writes
In [19]: %time write('outfile.parq', df, compression='snappy', write_index=True)
CPU times: user 355 ms, sys: 65.4 ms, total: 420 ms
Wall time: 434 ms
size: 38M

- with snappy read
In [28]: %time df1 = p.to_pandas(); df1.set_index('index')
CPU times: user 172 ms, sys: 108 ms, total: 281 ms
Wall time: 321 ms

- pickles
In [32]: %time df.to_pickle('data.pickle')
CPU times: user 25.2 ms, sys: 50 ms, total: 75.2 ms
Wall time: 97.1 ms
- pickle load
In [33]: %time pd.read_pickle('data.pickle')
CPU times: user 13.2 ms, sys: 39.4 ms, total: 52.6 ms
Wall time: 50.4 ms

-- with dask csv.gz
%time df = dd.read_csv('../data/customer_1/aws/regions/amsterdam/zone9/server1/load_avg_5/*.csv.gz', compression='gzip', header=None, date_parser=date_parser, parse_dates=[0], blocksize=None)
CPU times: user 739 ms, sys: 45.3 ms, total: 784 ms
Wall time: 806 ms
In [131]: %time df = df.set_index(0)
CPU times: user 14.9 s, sys: 1.94 s, total: 16.8 s
Wall time: 13.4 s

"""