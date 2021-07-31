import requests
import numpy as np
import pandas as pd
import time

while True:
    try:
        requests.get('http://localhost:5432/driver/all')
        break
    except:
        print('Waiting Connection...')
        time.sleep(0.5)
        continue

# carga inicial
def activate(i):
    url = "http://localhost:5432/driver/activate"
    obj = {
        'driverid': i
    }
    x = requests.post(url, data=obj)

def deactivate(i):
    url = "http://localhost:5432/driver/deactivate"
    obj = {
        'driverid': i
    }
    x = requests.post(url, data=obj)

[activate(i) for i in range(15)]

currentDrivers = list(range(15))

k = 0
while True:
    k += 1
    time.sleep(np.random.exponential(2))

    if np.random.rand() < len(currentDrivers) / 1000:
        driver = np.random.choice(currentDrivers)
        deactivate(driver)
        currentDrivers.remove(driver)
    else:
        driver = np.random.randint(0, 100000)
        if driver not in currentDrivers:
            activate(driver)
            currentDrivers.append(driver)

    if k % 200 == 0:
        aux = np.array(requests.get('http://localhost:5432/driver/all').json()['drivers'])
        currentDrivers = aux[aux[:, 1], 0].tolist()