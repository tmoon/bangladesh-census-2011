import ray
import numpy as np
import pandas as pd
# import pyarrow as pa
import time
import os

print("PID", os.getpid())
ray.init()


# @ray.remote
# class Counter(object):
#     def __init__(self):
#         self.value = 0

#     def increment(self):
#         self.value += 1
#         return self.value

        
# @ray.remote
class Person(object):
    """docstring for Person"""
    def __init__(self, id):
        # super(Person, self).__init__()
        self.id = id
        self.arr = np.random.rand(1000)
    
    def get_arr(self, i):
        return self.arr[i]

if __name__ == '__main__':
    p_arr = []
    p_val_arr = []
    M = 1000000

    # for i in range(10 * M):
    #     p = Person(i)
    #     p_val_arr.append(p.arr[i % 100])
    #     if i % M == 1:
    #         print(i, " ")

    # del(p_val_arr)
    # # Create ten < Counter actors.
    counters = [Person(id) for id in range(1000)]

    # # Increment each Counter once and get the results. These tasks all happen in
    # # parallel.
    results = ray.get([c.get_arr.remote(13) for c in counters])

    # print(results)  # prints [1, 1, 1, 1, 1, 1, 1, 1, 1, 1]
    print("DONE")
    time.sleep(100)