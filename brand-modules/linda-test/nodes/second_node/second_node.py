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
        self.max_len = 0
        print("SecondNode initialized")

    def work(self):
        
        self.streams = self.r.xread(streams={'random':self.last_id}, count=5, block=0) # reads up to 5 entries if available, else blocks
        self.max_len = max(self.max_len, len(self.streams))
        for stream_name, entries in self.streams:
            for entry_id, entry_dict in entries:
                self.last_id = entry_id

                if entry_dict[b'status'] == b'done':
                    self.terminate(None, None)

                # self.sample['ts'] = np.frombuffer(entry_dict[b'ts'], dtype=np.uint64)[0]
                self.sample['i_in'] = np.frombuffer(entry_dict[b'i_in'], dtype=np.uint64)
                print(self.sample['i_in'].shape)
                self.sample['i'] = np.frombuffer(entry_dict[b'i'], dtype=np.uint32)
                if self.first_timestamp is None:
                    self.first_timestamp = np.frombuffer(entry_dict[b'i_in'], dtype=np.uint64)[0]

                self.process_times.append( time.time_ns() - np.frombuffer(entry_dict[b'i_in'], dtype=np.uint64)[0])
                print(self.sample['i'], self.sample['i_in'])

    def terminate(self, sig, frame):
        print("Max Redis stream length observed:", self.max_len)
        # print("Total time in s" ,(time.time_ns() - self.first_timestamp)/1e9)
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