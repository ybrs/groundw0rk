
import asyncio
from sanic import Sanic
from sanic.response import json, text
import os
import sqlite3
import numpy as np
import pandas as pd
import math
from utils import u, b, strip_all
import redis

import logging
logging.basicConfig(level=logging.DEBUG)

rds = redis.StrictRedis()

DATA_DIR = b(os.getenv('DATA_DIR', b'./data'))
print("->>>", DATA_DIR)
DB_DIR = b(os.getenv('DB_DIR', b'./db'))

file_handles = {}

INDEX_DB = b'/'.join([DB_DIR, b'index.db'])

db_conn = sqlite3.connect(INDEX_DB.decode())

def prepare_db():
    c = db_conn.cursor()
    try:
        c.execute('''CREATE TABLE metrics(tenant text, name text, last_ts int, last_val real)''')
        c.execute('''CREATE TABLE metric_props(tenant text, metric_name text, name text,
                                               value_text text,
                                               value_int integer,
                                               value_float float
                                               )''')
        c.execute('''CREATE TABLE metric_files(tenant text, metric_name text, min_ts int, max_ts int, file_path text)''')
    except:
        # TODO: check metrics already exists
        pass
    db_conn.commit()

prepare_db()


def metric_prop_type(val):
    if isinstance(val, int):
        return 'value_int'
    if isinstance(val, float):
        return 'value_float'
    return 'value_text'

def any_(*args):
    for arg in args:
        if arg is not None:
            return arg

def metric_type_or_none(val):
    if isinstance(val, int):
        return [None, val, None]
    if isinstance(val, float):
        return [None, None, val]
    return [val, None, None]


def get_metric_props_as_dict(tenant, metric_name):
    c = db_conn.cursor()
    c.execute('''select name, value_text, value_int, value_float
                 from metric_props
                 where tenant=? and metric_name=?''', (tenant, metric_name))

    rows = c.fetchall()


    db_metric_props = {}
    for row in rows:
        db_metric_props[row[0]] = any_(row[1], row[2], row[3])

    return db_metric_props

def save_metric_prop(tenant, metric_name, prop_name, value):
    c = db_conn.cursor()
    c.execute('''select name, value_text, value_int, value_float
                 from metric_props
                 where tenant=? and metric_name=? and name=?''', (tenant, metric_name, prop_name))
    row = c.fetchone()
    if row:
        print("do update")
        return

    value_text, value_int, value_float = metric_type_or_none(value)

    # do insert
    c.execute('''
      insert into metric_props(tenant, metric_name, name, value_text, value_int, value_float)
                       values (?, ?, ?, ?, ?, ?)
                         ''', (tenant, metric_name, prop_name, value_text, value_int, value_float))
    db_conn.commit()

def update_db_index(tenant, metric_name, ts, val, file_path, metric_props):
    c = db_conn.cursor()
    c.execute("select name from metrics where tenant=? and name=?", (tenant, metric_name,) )
    row = c.fetchone()
    if not row:
        c.execute('''
        insert into metrics(tenant, name, last_ts, last_val)
        values (?, ?, ?, ?)
        ''', (tenant, metric_name, ts, val))
    else:
        # update maybe
        pass

    if not metric_props:
        metric_props = {}

    db_metric_props = get_metric_props_as_dict(tenant, metric_name)

    for k, v in metric_props.items():
        if db_metric_props.get(k, None) != v:
            save_metric_prop(tenant, metric_name, k, v)

    # c.execute('select metric_name from metrics_files where metric_name=? and file_path=?', (metric_name, file_path))
    # row = c.fetchone()
    # if not row:
    #     c.execute('''
    #       insert into metrics_files(metric_name, file_path)
    #       values (?, ?)
    #     ''', (metric_name, file_path))

    db_conn.commit()


def prepare_dirs(metric_name, customer=b'customer_1'):
    dir = b'/'.join([DATA_DIR, customer, b(metric_name).replace(b'.', b'/')])
    if not os.path.exists(dir):
        os.makedirs(dir, exist_ok=True)
    return dir

def get_fhandle(fname):
    if file_handles.get(fname):
        return file_handles.get(fname)
    file_handles[fname] = open(fname, 'a+')
    logging.info("new file handle - %s", fname)
    return file_handles[fname]

def save_record(tenant, metric_name, ts, val):
    dir = prepare_dirs(metric_name)
    hour_ts = math.floor(ts/3600)*3600
    fname = os.path.join(u(dir), u(hour_ts))
    fname = '{}.csv'.format(u(fname))
    fhandle = get_fhandle(fname)
    fhandle.write(','.join(u([ts, val])))
    fhandle.write('\n')
    #
    update_db_index(tenant, metric_name, ts, val, fname)

def str_int_or_float_value(s):
    if '.' in s:
        return float(s)
    return int(s)

def process_line(valln):
    val = u(valln)
    try:
        tenant, metric_name, ts, val = val.split(',')
    except:
        logging.error("format error - %s", val)
        return
    ts = str_int_or_float_value(ts)
    val = str_int_or_float_value(val)
    save_record(tenant, metric_name, ts, val)

async def go():
    import time
    t1 = time.time()
    counter = 0

    conn = await aioredis.create_connection(('localhost', 6379), loop=loop)
    while True:
        val = await conn.execute('brpop', 'metrics_queue', 10)
        if not val:
            if counter:
                logging.info("{} in {} seconds, {} val/sec".format(counter, (ltime - t1), counter/(ltime-t1)))
            counter = 0
            continue

        logging.debug("received %s", val)
        counter += 1
        process_line(val[1])
        ltime = time.time()

    conn.close()
    await conn.wait_closed()


@asyncio.coroutine
async def periodic(loop):
    while True:
        for k, f in file_handles.items():
            f.flush()
        # print("sleeping")
        await asyncio.sleep(3)


if __name__ == '__main__':
    import asyncio
    import aioredis
    loop = asyncio.get_event_loop()

    # asyncio.ensure_future(periodic)
    loop.create_task(periodic(loop))
    loop.run_until_complete(go())