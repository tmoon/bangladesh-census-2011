import ray
import numpy as np
import pandas as pd
# import pyarrow as pa
import time
import os

print("PID", os.getpid())

RAY = True


class District(object):
    """docstring for District"""
    def __init__(self, id):
        self.id = id
        self.persons_dict = {}
    
    def register_persons(self, persons):
        for person in persons:
            self.persons_dict[person.uid] = person
        time.sleep(10)
    

    def num_members(self):
        return len(self.persons_dict)

if RAY:
    ray.init()
    District = ray.remote(District)

print(ray.get_gpu_ids())
class Person(object):
    """docstring for Person"""
    def __init__(self, uid, features = None):
        self.uid = uid
        self.features = np.random.rand(100) if features is None else features
        
if __name__ == '__main__':
    M = 10000

    NUM_DIST = 4
    p_dict = {i: [] for i in range(NUM_DIST)}

    if RAY:
        dists = [District.options(resources={"Custom1": i}).remote(i) for i in range(NUM_DIST)]
    else:
        dists = [District(i) for i in range(NUM_DIST)]

    for i in range(M):
        p = Person(i)
        p_dict[i % NUM_DIST].append(p)

    t = time.time()
    for id in range(NUM_DIST):
        dist = dists[id]
        if RAY:
            dist.register_persons.remote(p_dict.get(id))
        else:
            dist.register_persons(p_dict.get(id))

    if RAY:
        results = ray.get([d.num_members.remote() for d in dists])
    else:
        results = [d.num_members() for d in dists]

    print(time.time() - t)

    print(results)
    print("DONE")
    time.sleep(100)