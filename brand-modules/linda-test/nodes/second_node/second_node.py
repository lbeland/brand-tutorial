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
        self.process_times = []
        print("SecondNode initialized")

    def work(self):
        
        self.streams = self.r.xread(streams={'random':self.last_id}, count=1, block=0)
        self.stream_name, self.stream_entries = self.streams[0]
        self.entry_id, self.entry_dict = self.stream_entries[0]
        self.last_id = self.entry_id

        if self.entry_dict[b'status'] == b'done':
            self.terminate(None, None)

        # self.sample['ts'] = np.frombuffer(self.entry_dict[b'ts'], dtype=np.uint64)[0]
        self.sample['i_in'] = np.frombuffer(self.entry_dict[b'i_in'], dtype=np.uint64)[0]

        if self.first_timestamp is None:
            self.first_timestamp = np.frombuffer(self.entry_dict[b'i_in'], dtype=np.uint64)[0]

        self.process_times.append( time.time_ns() - np.frombuffer(self.entry_dict[b'i_in'], dtype=np.uint64)[0])
        # print(self.sample['i'], self.sample['i_in'], self.sample['ts'])

    def terminate(self, sig, frame):
        print("Total time in s" ,(time.time_ns() - self.first_timestamp)/1e9)
        print("Average processing time in s:", np.mean(self.process_times)/1e9)
        print("Max processing time in s:", np.max(self.process_times)/1e9, np.argmax(self.process_times))
        print("Std of processing time in s:", np.std(self.process_times)/1e9)
        logging.info('SIGINT received, Exiting')
        self.r.close()
        sys.exit(0)

if __name__ == "__main__":
    gc.disable()

    # setup
    secondnode = SecondNode()

    # main
    secondnode.run()

    gc.collect()