import asyncio
from sanic import Sanic
from sanic.response import json, text
import os
import sqlite3
import numpy as np
import pandas as pd
import math

DATA_DIR = b'./data'
DB_DIR = b'./db'

file_handles = {}

INDEX_DB = b'/'.join([DB_DIR, b'index.db'])

db_conn = sqlite3.connect(INDEX_DB.decode())

def prepare_db():
    c = db_conn.cursor()
    try:
        c.execute('''CREATE TABLE metrics(name text, last_ts int, last_val real)''')
        c.execute('''CREATE TABLE metrics_files(metric_name text, min_ts int, max_ts int, file_path text)''')
    except:
        # TODO: check metrics already exists
        pass
    db_conn.commit()

prepare_db()

app = Sanic()

def update_db_index(metric_name, ts, val, file_path):
    c = db_conn.cursor()
    c.execute("select name from metrics where name=?", (metric_name,) )
    row = c.fetchone()
    if not row:
        c.execute('''
        insert into metrics(name, last_ts, last_val)
        values (?, ?, ?)
        ''', (metric_name, ts, val))
    else:
        # update maybe
        pass

    c.execute('select metric_name from metrics_files where metric_name=? and file_path=?', (metric_name, file_path))
    row = c.fetchone()
    if not row:
        c.execute('''
          insert into metrics_files(metric_name, file_path)
          values (?, ?)
        ''', (metric_name, file_path))

    db_conn.commit()

@app.route("/")
async def main(request):
    return text("ok")

def prepare_dirs(metric_name):
    dir = b'/'.join([DATA_DIR, b('customer_1'), b(metric_name).replace(b'.', b'/')])
    if not os.path.exists(dir):
        os.makedirs(dir, exist_ok=True)
    return dir

def get_fhandle(fname):
    if file_handles.get(fname):
        return file_handles.get(fname)
    file_handles[fname] = open(fname, 'a+')
    print("new file handle -", fname)
    return file_handles[fname]

def b(v):
    if isinstance(v, str):
        return v.encode()
    if isinstance(v, bytes):
        return v
    return str(v).encode()


def u(v):
    """
    always returns a string.

    if v is a list of somethings, returns list of strings

    :param v:
    :return:
    """
    if isinstance(v, str):
        return v

    if isinstance(v, bytes):
        return v.decode()

    if isinstance(v, list):
        return [u(vt) for vt in v]

    return str(v)

def save_record(metric_name, ts, val):
    dir = prepare_dirs(metric_name)
    hour_ts = math.floor(ts/3600)*3600
    fname = os.path.join(u(dir), u(hour_ts))
    fname = '{}.csv'.format(u(fname))

    fhandle = get_fhandle(fname)
    fhandle.write(','.join(u([ts, val])))
    fhandle.write('\n')
    #
    update_db_index(metric_name, ts, val, fname)

def process_line(ln):
    if not ln:
        return
    vals = [v.strip() for v in ln.split(b',')]
    if len(vals) < 3:
        return
    metric_name, ts, val = vals[0:3]
    # print(metric_name, ts, val)
    save_record(metric_name, float(ts), val)

@app.route("/metrics", methods=['POST'])
async def metrics(request):
    try:
        lines = request.body.split(b'\n')
        for ln in lines:
            process_line(ln)
    except:
        raise
        # return text('-err')
    return text("+ok")


@app.route('/collectd-post', methods=['POST'])
async def collectd_post(request):
    import json as jsn
    data = jsn.loads(request.body)
    for ln in data:
        ts = ln['time']

        m = []
        for k in ('host', 'plugin', 'plugin_instance', 'plugin_type', 'type_instance'):
            tv = ln.get(k, '')
            if tv:
                m.append(tv)

        for t in zip(ln['dstypes'], ln['dsnames'], ln['values']):
            metric_name = '.'.join(m + list(t[0:2]))
            v = t[-1]
            if 'cpu.' in metric_name:
                print(ln)
            save_record(metric_name.encode(), ts, v)

    return text("+ok\r\n")

# these are for mimicking prometheus api
@app.route('/api/v1/label/<name>/values')
async def api_name(request, name):

    if name == '__name__':
        c = db_conn.cursor()
        c.execute('''
          select name from metrics order by name
        ''')

        names = [row[0] for row in c.fetchall()]
        return json({
           "status" : "success",
           "data" : names
        })

    return text('-err:Unsupported')

@app.route('/api/v1/query_range')
async def query_range(request):
    from cli import load_files
    metric_name = request.args['query'][0].encode()
    ds = load_files(metric_name, ts_start=0, ts_end=0)

    # print(request.args)

    # convert to epoch
    ds.index = ds.index.astype(np.int64) // 10 ** 9

    vals = []
    for i, v in ds.itertuples():
        vals.append((float(i), float(v)))

    return json({
           "status" : "success",
           "data" : {
              "resultType" : "matrix",
              "result" : [
                 {
                    "metric" : {
                       "__name__" : metric_name,
                    },
                    "values" : vals
                }
              ]
           }
        })

@asyncio.coroutine
def periodic(app, loop):
    while True:
        for k, f in file_handles.items():
            f.flush()
        yield from asyncio.sleep(3)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8001, debug=False, after_start=periodic)


"""
curl -v -X POST --data-binary @post_values.txt http://localhost:8001/metrics/
"""