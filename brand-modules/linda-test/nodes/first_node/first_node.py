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
            'i':0,
            'status': b'running'
        }
        print("FirstNode initialized")

    def run(self):
        self.start_time = time.time_ns()
        n_samples = 50
        for i in range(n_samples):
            # send samples to Redis
            # print(f"Sending sample {i}, Redis stream length: {self.r.xlen('random')}")
            self.sample['i'] = self.i.tobytes()
            self.sample['i_in'] = np.uint64(time.time_ns()).tobytes()
            # self.sample['i_in'] = (np.ones((100,), dtype=np.uint64)*time.time_ns()).tobytes()
            # self.sample['ts'] = np.uint64(time.time_ns()).tobytes()
            self.r.xadd('random', self.sample, maxlen=10, approximate=True)
            self.i += np.uint32(1)
        elapsed_time = time.time_ns() - self.start_time
        print(f"Elapsed time for sending {n_samples} samples: {elapsed_time/1e9} seconds")
        self.sample['status'] = b'done'
        self.r.xadd('random',  self.sample, maxlen=1, approximate=True)



if __name__ == "__main__":
    gc.disable()

    # setup
    firstnode = FirstNode()

    # main
    firstnode.run()

    gc.collect()