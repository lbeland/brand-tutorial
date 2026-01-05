import gc
import sys
import numpy as np
import time
import logging
from brand import BRANDNode

class FirstNode(BRANDNode):
    def __init__(self):
        super().__init__()
        self.i = np.uint32(0)
        self.sample = {
            # time at which the output is written
            'ts': np.uint64(time.time_ns()).tobytes(),
            'i_in': 0.0,
            'status': b'running'
        }
        print("FirstNode initialized")

    def run(self):
        self.start_time = time.time_ns()
        for i in range(5000):
            # send samples to Redis
            self.sample['i_in'] = np.uint64(time.time_ns()).tobytes()
            # self.sample['ts'] = np.uint64(time.time_ns()).tobytes()
            self.r.xadd('random', self.sample, maxlen=100, approximate=True)
            self.i += np.uint32(1)
        elapsed_time = time.time_ns() - self.start_time
        print(f"Elapsed time for sending 1000 samples: {elapsed_time/1e9} seconds")
        self.sample['status'] = b'done'
        self.r.xadd('random',  self.sample, maxlen=1, approximate=True)

    # def work(self):
        # print("Generating random number")



if __name__ == "__main__":
    gc.disable()

    # setup
    firstnode = FirstNode()

    # main
    firstnode.run()

    gc.collect()