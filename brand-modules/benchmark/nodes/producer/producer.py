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

import numpy as np
import logging
import time
import pandas as pd
from brand import BRANDNode

class Producer(BRANDNode):
    def __init__(self):
        super().__init__()
        self.n_messages =  self.parameters["n_messages"]
        self.msg_size = self.parameters["msg_size"]
        self.t_wait = self.parameters.get("t_wait", 0.0001)
        self.num_channels = self.parameters["num_channels"]
        self.output_file = self.parameters["output_file"]
        self.max_buffer_size = self.parameters.get("max_buffer_size", 10)
        
        self.send_times = []
        self.sample = {
            'time': 0.0,
        }

    def _wait_for_buffer_slot(self):
        while self.r.xlen('random') >= self.max_buffer_size:
            pass

    def run(self):
        try:
            default_data = np.ones((self.num_channels,self.msg_size), dtype=np.float64)

            # warmup loop to stabilize the stream before measurements
            counter = 0
            for i in range(500):
                now = time.monotonic()
                self._wait_for_buffer_slot()
                self.sample['time'] = (default_data * now).tobytes()
                self.r.xadd('random', self.sample, maxlen=None, approximate=True)
                counter += 1

            for i in range(self.n_messages):
                if self.t_wait > 0:
                    time.sleep(self.t_wait)  # Let Consumer process last message
                self._wait_for_buffer_slot()
                now = time.monotonic()
                data = default_data * now
                self.sample['time'] = data.tobytes()
                self.r.xadd('random', self.sample, maxlen=None, approximate=True)
                self.send_times.append(now)
        finally:   
            self.terminate(None, None)

    def terminate(self, sig, frame):
        print(f"Total messages sent: {len(self.send_times)}")

        send_times_diff = np.diff(self.send_times)
        mean_send_period = np.mean(send_times_diff)*1e6
        std_send_period = np.std(send_times_diff)*1e6
        max_send_period = np.max(send_times_diff)*1e6
        print(f"Average send period (us): {mean_send_period:.4f}")
        print(f"Max send period (us): {max_send_period:.4f}, idx: {np.argmax(send_times_diff)}")
        print(f"Std send period (us): {std_send_period:.4f}")

        results = pd.DataFrame({"Metric":["mean", "std", "max"],"Send_period": [mean_send_period, std_send_period, max_send_period]})
        results.to_csv(self.output_file, index=False)
        print("Producer stopped.")


if __name__ == "__main__":
    gc.disable()

    # setup
    producer = Producer()

    # main
    producer.run()

    gc.collect()