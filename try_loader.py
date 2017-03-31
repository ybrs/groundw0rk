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

"""