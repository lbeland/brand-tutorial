import gc
import sys
import os
import site

# Add virtual environment site-packages to sys.path if VIRTUAL_ENV is set.
# Two things are needed, in this order, not just a plain sys.path.insert():
#  1. Insert at the FRONT of sys.path. This binary's default sys.path (from
#     the linked system libpython3.8) already includes system dist-packages,
#     which can shadow the venv's pinned versions of packages that also
#     happen to exist system-wide (e.g. a newer system pytz requiring
#     zoneinfo, Python 3.9+ stdlib, breaking on this 3.8 venv) if the venv
#     dir only gets appended at the end.
#  2. Call site.addsitedir(), not just sys.path.insert(): brand is
#     pip-installed editable (PEP 660), which relies on a .pth file that
#     registers a custom import finder — that .pth only gets executed by
#     site.addsitedir()'s processing, not by merely adding the directory to
#     sys.path. This binary is a Cython --embed executable, which skips the
#     interpreter's normal startup site-processing, so it has to be done here.
if 'VIRTUAL_ENV' in os.environ:
    venv_path = os.environ['VIRTUAL_ENV']
    site_packages = os.path.join(venv_path, 'lib', 'python3.8', 'site-packages')
    if site_packages not in sys.path:
        sys.path.insert(0, site_packages)
    site.addsitedir(site_packages)

import logging
import numpy as np
import pandas as pd
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
        self.warmup_counter = 0
        self.first_timestamp = None

    def work(self):
        
        self.streams = self.r.xread(streams={'random':self.last_id}, count=1, block=0) # reads 1 entry, else blocks
        now = time.monotonic()

        for stream_name, entries in self.streams:
            for entry_id, entry_dict in entries:
                self.last_id = entry_id
                
                # Warmup phase: receive 500 samples before measurements
                if self.warmup_counter < 500:
                    self.warmup_counter += 1
                    self.r.xdel('random', entry_id)
                    continue
                
                timestamp = np.frombuffer(entry_dict[b'time'], dtype=np.float64)[0]
                if self.counter == 0:
                    self.first_timestamp = timestamp
                self.recv_times.append(now)
                self.process_times.append(now - timestamp)
                self.counter += 1
                self.r.xdel('random', entry_id)

        if self.counter >= self.n_messages:
            self.terminate(None, None)

    def terminate(self, sig, frame):
        print(f"Total messages processed: {len(self.process_times)}")
        recv_times_diff = np.diff(self.recv_times)
        mean_recv_period = np.mean(recv_times_diff)*1e6
        std_recv_period = np.std(recv_times_diff)*1e6
        max_recv_period = np.max(recv_times_diff)*1e6

        print(f"Average receive period (us): {mean_recv_period:.4f}")
        print(f"Max receive period (us): {max_recv_period:.4f}, idx: {np.argmax(recv_times_diff)}")
        print(f"Std receive period (us): {std_recv_period:.4f}")

        mean_latency = np.mean(self.process_times)*1e6
        std_latency = np.std(self.process_times)*1e6
        max_latency = np.max(self.process_times)*1e6

        print(f"Average latency (us): {mean_latency:.4f}")
        print(f"Max latency (us): {max_latency:.4f}, idx: {np.argmax(self.process_times)}")
        print(f"Std latency (us): {std_latency:.4f}")

        throughput = self.n_messages / (self.recv_times[-1] - self.first_timestamp)/1e3
        print(f"Throughput (msg/ms): {self.n_messages}/{(self.recv_times[-1] - self.first_timestamp)*1e3:.4f}ms={throughput:.2f}")

        results = pd.DataFrame({"Metric":["mean", "std", "max"],
                                "Recv_period": [mean_recv_period, std_recv_period, max_recv_period], 
                                "Latency": [mean_latency, std_latency, max_latency], 
                                "Throughput": [throughput, None, None]})

        results.to_csv(self.output_file, index=False)
        print("Consumer stopped.")
        
        # Persist per-packet process_times for post-processing
        try:
            base = self.output_file[:-4] if self.output_file.lower().endswith('.csv') else self.output_file
            proc_path = f"{base}_process_times.csv"
            with open(proc_path, "w") as f:
                for v in self.process_times:
                    f.write(f"{v}\n")
        except Exception:
            pass

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