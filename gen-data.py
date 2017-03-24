"""
this just generates random metrics
"""
import requests
import random
import time

buffer = []

def post_random_data(prefix, ts, i, buffer_len=100):
    """
    we generate data for a month

    :return:
    """
    global buffer
    val = random.randint(1, 1000) / (random.randint(1, 100))
    s = '{prefix}{i}.load_avg_5 {val} {ts}'.format(prefix=prefix, i=i, ts=int(ts), val=val)
    buffer.append(s)
    if len(buffer) > buffer_len:
        s = '\n'.join(buffer)
        requests.post('http://localhost:8001/metrics', data=s)
        buffer = []
    # print(s)

def main(days=30,
         metric_count=1,
         prefix='aws.regions.amsterdam.zone1.server',
         buffer_len=100):

    t1 = time.time() - (days * 24 * 60 * 60)
    ts = int(t1)
    now = time.time()
    while ts < now:
        ts += 1

        if ts % 10 == 0:
            for i in range(1, 1+metric_count):
                post_random_data(prefix, ts, i, buffer_len=buffer_len)

        if ts % 100 == 0:
            print(">>>", ts, now - ts)
            # time.sleep(0.01)

if __name__ == '__main__':
    main(365, metric_count=1, prefix='aws.regions.amsterdam.zone9.server', buffer_len=500)