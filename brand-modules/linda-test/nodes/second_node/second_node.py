import gc
import logging
import sys
import numpy as np
import time
from brand import BRANDNode

class SecondNode(BRANDNode):
    def __init__(self):
        super().__init__()
        self.last_id = b'0-0'   # read always oldest entry (from beginning), '$' for latest entry
        self.sample = {}
        self.first_timestamp = None
        print("SecondNode initialized")

    def work(self):
        self.streams = self.r.xread(streams={'random':self.last_id}, count=1, block=0)
        self.stream_name, self.stream_entries = self.streams[0]
        self.entry_id, self.entry_dict = self.stream_entries[0]
        self.last_id = self.entry_id

        self.sample['i_in'] = time.time_ns() - np.frombuffer(self.entry_dict[b'i_in'], dtype=np.uint64)[0]
        self.sample['ts'] = np.frombuffer(self.entry_dict[b'ts'], dtype=np.uint64)[0]
        self.sample['i'] = np.frombuffer(self.entry_dict[b'i'], dtype=np.uint32)[0]

        if self.first_timestamp is None:
            self.first_timestamp = self.sample['ts']

        print(self.sample['i'], self.sample['i_in'], self.sample['ts'])

    def terminate(self, sig, frame):
        print("Total time in s" ,(self.sample['ts'] - self.first_timestamp)/1e9)
        logging.info('SIGINT received, Exiting')
        self.r.close()

if __name__ == "__main__":
    gc.disable()

    # setup
    secondnode = SecondNode()

    # main
    secondnode.run()

    gc.collect()