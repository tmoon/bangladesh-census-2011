import ray
import numpy as np
import pandas as pd
# import pyarrow as pa
import time
import os

print("PID", os.getpid())
ray.init()
        
@ray.remote
class District(object):
    """docstring for District"""
    def __init__(self, id):
        self.id = id
        self.persons_dict = {}
    
    def register_person(self, person):
        self.persons_dict[person.uid] = person
    

    def num_members(self):
        return len(self.persons_dict)


class Person(object):
    """docstring for Person"""
    def __init__(self, uid, features = None):
        self.uid = uid
        self.features = np.random.rand(100) if features is None else features
        
if __name__ == '__main__':
    p_arr = []
    M = 1000000

    NUM_DIST = 4

    dists = [District.remote(i) for i in range(NUM_DIST)]

    for i in range(M):
        p = Person(i)

        dists[i % NUM_DIST].register_person.remote(p)

        if i % M == 1:
            print(i, " ")

    results = ray.get([d.num_members.remote() for d in dists])


    print(results)
    print("DONE")
    time.sleep(100)