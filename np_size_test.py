import ray
import numpy as np
import pandas as pd
import pyarrow as pa
import time
import os

print("PID", os.getpid())

M = int(1e6)
N = 10 * M

RAY = True
PA = False

class Country(object):
    """docstring for Country"""
    def __init__(self, oid, arr=None):
        self.oid = oid
        self.arr = ray.get(oid) if arr is None else arr
    
    def access_values(self):
        for i in range(M):
            v = self.arr[i]

        return v

if RAY:
    ray.init()
    Country = ray.remote(Country)

if __name__ == '__main__':
    

    time.sleep(0)
    print("memory allocating")

    # TEST 1: Array access
    arr = np.random.rand(N)
    if RAY:
        oid = ray.put(arr)
    elif PA:
        arr = pa.array(arr)

    if RAY:
        arr = ray.get(oid)

    t = time.time()


    for i in range(N):
        v = arr[i]

    print(time.time() - t, oid)
    time.sleep(3)
    # TEST 2: Memory footprint across workers
    print("TEST 2")
    if RAY:
        countries = [Country.remote(oid) for _ in range(10)]
        t = time.time()
        res = [c.access_values.remote() for c in countries]
        time.sleep(5)
        print(res)

        print(time.time() - t)
    time.sleep(100)