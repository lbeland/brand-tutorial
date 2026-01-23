import gc
import logging
import sys
import numpy as np
import time
from brand import BRANDNode

class Consumer(BRANDNode):
    def __init__(self):
        super().__init__()
        self.last_id = b'0-0'   # read always oldest entry (from beginning), '$' for latest entry
        self.recv_times = []
        self.process_times = []
        self.n_messages = self.parameters["n_messages"]
        self.output_file = self.parameters["output_file"]
        self.counter = 0

    def work(self):
        
        self.streams = self.r.xread(streams={'random':self.last_id}, count=1, block=0) # reads up to 5 entries if available, else blocks
        now = time.monotonic()

        for stream_name, entries in self.streams:
            for entry_id, entry_dict in entries:
                self.last_id = entry_id
                self.recv_times.append(now)

                self.process_times.append(now - np.frombuffer(entry_dict[b'time'], dtype=np.float64)[0])
        self.counter += 1

        if self.counter >= self.n_messages + 1:
            self.terminate(None, None)

    def terminate(self, sig, frame):
        print(f"Total messages processed: {len(self.process_times)}")
        process_times = self.process_times[1:]     # exclude first measuremnt because of startup delay
        # print(f"Average processing time (s): {np.mean(process_times)/1e9:.6f}")
        # print(f"Max processing time (s): {np.max(process_times)/1e9:.6f}, idx: {np.argmax(process_times)}")
        # print(f"Std processing time (s): {np.std(process_times)/1e9:.6f}")
        # print('\n')
        recv_times = self.recv_times[1:]
        recv_times_diff = np.diff(recv_times)
        print(f"Average receive period (s): {np.mean(recv_times_diff):.6f}")
        print(f"Max receive period (s): {np.max(recv_times_diff):.6f}, idx: {np.argmax(recv_times_diff)}")
        print(f"Std receive period (s): {np.std(recv_times_diff):.6f}")

        save = np.vstack((recv_times, process_times, np.append(recv_times_diff, 0)))
        np.savetxt(self.output_file, save.T, delimiter=',', header='recv_time,latency,recv_period')

        print("Consumer stopped.")
        
        # Signal completion
        self.r.xadd('test_completion', {'consumer': 'done'})
        self.r.close()
        sys.exit(0)

if __name__ == "__main__":
    gc.disable()

    # setup
    consumer = Consumer()

    # main
    consumer.run()

    gc.collect()