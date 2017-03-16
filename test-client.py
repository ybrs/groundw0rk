"""
this just generates random metrics
"""
import requests
import random
import time

def post_random_data():
    i = random.randint(1, 10)
    val = random.randint(1, 1000) / (random.randint(1, 100))
    s = 'aws.regions.frankfurt.zone1.server{i}.load_avg_5,{ts},{val}'.format(i=i, ts=int(time.time()), val=val)
    requests.post('http://localhost:8001/metrics', data=s)
    print(s)

if __name__ == '__main__':
    while True:
        for i in range(1, 10):
            post_random_data()
        time.sleep(1)