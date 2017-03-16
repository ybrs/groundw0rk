"""
this just generates random metrics
"""
import requests
import random
import time

def post_random_data(ts, i):
    """
    we generate data for a month

    :return:
    """
    val = random.randint(1, 1000) / (random.randint(1, 100))
    s = 'aws.regions.amsterdam.zone1.server{i}.load_avg_5,{ts},{val}'.format(i=i, ts=int(ts), val=val)
    requests.post('http://localhost:8001/metrics', data=s)
    # print(s)

if __name__ == '__main__':
    t1 = time.time() - (30 * 24 * 60 * 60)
    ts = int(t1)
    now = time.time()
    while ts < now:
        ts = ts + 1

        if ts % 10 == 0:
            for i in range(1,2):
                post_random_data(ts, i)

        if ts % 100 == 0:
            print(">>>", ts, now - ts)
            # time.sleep(0.01)
