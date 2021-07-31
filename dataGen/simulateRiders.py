import requests
import numpy as np
import pandas as pd
import time

taxi = pd.read_csv('../original_data/taxi_2019/taxi_2019_01.txt', sep='|')
taxi = taxi.loc[~(pd.isna(taxi['ORIGIN_BLOCK_LATITUDE']))].reset_index(drop=True)

while True:
    try:
        requests.get('http://localhost:5432/driver/all')
        break
    except:
        print('Waiting Connection...')
        time.sleep(0.5)
        continue

def requestRide(i):
    lat, lon = taxi.sample()[['ORIGIN_BLOCK_LATITUDE', 'ORIGIN_BLOCK_LONGITUDE']].values[0]
    riderObj = {
        'riderid': i,
        'lat': float(lat),
        'lon': float(lon)
    }
    x = requests.post("http://localhost:5432/rider/request", data=riderObj)

for i in range(4):
    requestRide(i)    

k = 4
while True:
    k += 1
    time.sleep(np.random.exponential(6))
    requestRide(k)