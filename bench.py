"""
truely scientific benchmarks for gw0

this dumps numbers, to give you an idea about the speed.

hint: its not fast

"""
import requests
import random
import time

buffer = []
buffer_size = 20
total_time = 2

def post_random_data(ts, val):
    """

    :return:
    """
    global buffer
    buffer.append((ts, val))
    if len(buffer) % buffer_size == 0:
        t = []
        for ts, val in buffer:
            s = 'aws.regions.amsterdam.zone1.server{i}.load_avg_10,{ts},{val}'.format(i=12, ts=int(ts), val=val)
            t.append(s)
        s = '\n'.join(t)
        r = requests.post('http://127.0.0.1:8001/metrics', data=s)
        assert r.status_code == 200
        buffer = []

if __name__ == '__main__':
    ts = time.time()
    cnt = 0
    while True:
        now = time.time()
        elapsed = now - ts
        # we only do this for 20 seconds
        if elapsed > total_time:
            try:
                print("exiting - ", (cnt / elapsed))
            except:
                pass
            break

        post_random_data(now, cnt)
        cnt += 1

        if int(elapsed) % 2 == 0:
            try:
                print(">>> per second", elapsed, cnt/elapsed)
            except:
                pass
