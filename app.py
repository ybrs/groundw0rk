from functools import wraps

from flask import Flask, request, jsonify
import os
import sqlite3
import numpy as np
import pandas as pd
import math

from flask import Response

DATA_DIR = b'./data'
DB_DIR = b'./db'
INDEX_DB = b'/'.join([DB_DIR, b'index.db'])

db_conn = sqlite3.connect(INDEX_DB.decode())

from utils import u, b
import redis

import logging
logging.basicConfig(level=logging.DEBUG)

app = Flask(__name__)

rds = redis.StrictRedis()



def check_auth(username, password):
    """This function is called to check if a username /
    password combination is valid.
    """
    return True # username == 'admin' and password == 'secret'

def authenticate():
    """Sends a 401 response that enables basic auth"""
    return Response(
    'Could not verify your access level for that URL.\n'
    'You have to login with proper credentials', 401,
    {'WWW-Authenticate': 'Basic realm="Login Required"'})

def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.authorization
        if not auth or not check_auth(auth.username, auth.password):
            return authenticate()
        return f('customer_1', *args, **kwargs)
    return decorated

def queue_record(tenant, metric_name, ts, val, props=None):
    p = ''
    if props:
        prop_str = []
        for k, v in props.items():
            prop_str.append('='.join([k, v]))
        p = ','.join(prop_str)

    rds.lpush('metrics_queue', ','.join(u([tenant, metric_name, ts, val, p])))

def process_line(tenant, ln):
    if not ln:
        return
    vals = [v.strip() for v in ln.split(b',')]
    if len(vals) < 3:
        return
    metric_name, ts, val = vals[0:3]
    # print(metric_name, ts, val)
    queue_record(tenant, metric_name, float(ts), val)

def process_line_graphite(tenant, ln):
    """
    this is coming from httpPostHandler of Diamond
    :param tenant:
    :param ln:
    :return:
    """
    if not ln:
        return
    vals = [v.strip() for v in ln.split(b' ')]
    if len(vals) < 3:
        return
    metric_name, val, ts = vals[0:3]
    # print(metric_name, ts, val)
    queue_record(tenant, metric_name, float(ts), val)


@app.route("/")
def main():
    return "ok+"

@app.route("/gw0/metrics", methods=['POST'])
def gw0_metrics(tenant='customer_1'):
    data = request.json
    print(">>>", data)
    # for ln in lines:
    #     process_line_graphite(tenant, ln)
    for ln in data:
        queue_record(tenant, ln['name'], float(ln['ts']), ln['value'], props=ln.get('meta', {}))

    return "+ok"

@app.route("/metrics", methods=['POST'])
def metrics(tenant='customer_1'):
    data = request.get_data()
    print(">>>", data)
    lines = request.data.split(b'\n')
    print("------")
    print(lines)
    print('//----')
    for ln in lines:
        process_line_graphite(tenant, ln)
    return "+ok"


@app.route('/collectd-post', methods=['POST'])
@requires_auth
def collectd_post(tenant):
    import json as jsn
    data = jsn.loads(request.data)
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
            queue_record(tenant, metric_name.encode(), ts, v)

    return "+ok\r\n"

# these are for mimicking prometheus api
@app.route('/api/v1/label/<name>/values')
@requires_auth
async def api_name(tenant, name):

    if name == '__name__':
        c = db_conn.cursor()
        c.execute('''
          select name from metrics order by name
        ''')

        names = [row[0] for row in c.fetchall()]
        return jsonify({
           "status" : "success",
           "data" : names
        })

    return '-err:Unsupported'


def int_or_none(i):
    if i is None:
        return i
    return int(i)

@app.route('/api/v1/query_range')
@requires_auth
async def query_range(tenant):
    from cli import load_files

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

    return jsonify({
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


if __name__ == '__main__':
    app.run(port=8001, debug=True)