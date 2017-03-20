
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

def save_record(metric_name, ts, val):
    dir = prepare_dirs(metric_name)
    hour_ts = math.floor(ts/3600)*3600
    fname = os.path.join(u(dir), u(hour_ts))
    fname = '{}.csv'.format(u(fname))
    fhandle = get_fhandle(fname)
    fhandle.write(','.join(u([ts, val])))
    fhandle.write('\n')
    #
    # update_db_index(metric_name, ts, val, fname)



@asyncio.coroutine
def periodic(app, loop):
    while True:
        for k, f in file_handles.items():
            f.flush()
        yield from asyncio.sleep(3)


def str_int_or_float_value(s):
    if '.' in s:
        return float(s)
    return int(s)

def process_line(valln):
    val = u(valln)
    tenant, metric_name, ts, val = val.split(',')
    ts = str_int_or_float_value(ts)
    val = str_int_or_float_value(val)
    save_record(metric_name, ts, val)

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