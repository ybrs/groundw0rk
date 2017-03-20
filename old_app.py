import asyncio
from sanic import Sanic
from sanic.response import json, text
import os
import sqlite3
import numpy as np
import pandas as pd
import math
from utils import u, b
import redis

import logging
logging.basicConfig(level=logging.DEBUG)

rds = redis.StrictRedis()

DATA_DIR = b'./data'
DB_DIR = b'./db'

file_handles = {}

INDEX_DB = b'/'.join([DB_DIR, b'index.db'])

db_conn = sqlite3.connect(INDEX_DB.decode())

def prepare_db():
    c = db_conn.cursor()
    try:
        c.execute('''CREATE TABLE metrics(name text, last_ts int, last_val real)''')
        c.execute('''CREATE TABLE metric_props(metric_name text, name text, value text)''')
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

def prepare_dirs(metric_name, customer=b'customer_1'):
    dir = b'/'.join([DATA_DIR, customer, b(metric_name).replace(b'.', b'/')])
    if not os.path.exists(dir):
        os.makedirs(dir, exist_ok=True)
    return dir

def get_fhandle(fname):
    if file_handles.get(fname):
        return file_handles.get(fname)
    file_handles[fname] = open(fname, 'a+')
    print("new file handle -", fname)
    return file_handles[fname]

cnt = 0

def save_record(metric_name, ts, val):
    global cnt
    dir = prepare_dirs(metric_name)
    hour_ts = math.floor(ts/3600)*3600
    fname = os.path.join(u(dir), u(hour_ts))
    fname = '{}.csv'.format(u(fname))
    fhandle = get_fhandle(fname)
    fhandle.write(','.join(u([ts, val])))
    fhandle.write('\n')

    rds.lpush('metrics_queue', ','.join(u([metric_name, ts, val])))

    #
    # update_db_index(metric_name, ts, val, fname)

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

def int_or_none(i):
    if i is None:
        return i
    return int(i)

from sanic.exceptions import SanicException

class NotAuthenticated(SanicException):
    status_code = 401

class NotAllowed(SanicException):
    status_code = 403

import base64

def auth(request, authenticator):
    auths = request.headers.get('authorization', '')
    if not auths:
        raise NotAuthenticated('not authenticated')

    t = auths.split('Basic ')[1]
    decoded = base64.b64decode(t)
    user, passwd = decoded.split(b':')
    authenticated = authenticator(user, passwd)
    if not authenticated:
        raise NotAllowed('credentials wrong, or not allowed')

    return user, passwd

@app.route('/api/v1/query_range')
async def query_range(request):
    from cli import load_files
    auth(request, authenticator=lambda u, p: True)

    start = int(request.args.get('start', 0))
    end = int_or_none(request.args.get('end', None))
    metric_name = request.args['query'][0].encode()
    ds = load_files(metric_name, ts_start=start, ts_end=end)

    # print(request.args)
    if ds is None:
        vals = []
    else:
        # resample now.
        step = request.args.get('step', None)
        if step:
            ds = ds.resample('{}s'.format(step)).mean()

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