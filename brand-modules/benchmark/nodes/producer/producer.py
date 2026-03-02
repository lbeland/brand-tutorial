import gc
import sys
import os

# Add virtual environment site-packages to sys.path if VIRTUAL_ENV is set
if 'VIRTUAL_ENV' in os.environ:
    venv_path = os.environ['VIRTUAL_ENV']
    site_packages = os.path.join(venv_path, 'lib', 'python3.8', 'site-packages')
    if site_packages not in sys.path:
        sys.path.insert(0, site_packages)

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
        self.num_channels = self.parameters["num_channels"]
        self.output_file = self.parameters["output_file"]
        
        self.send_times = []
        self.sample = {
            'time': 0.0,
        }

    def run(self):
        try:
            default_data = np.ones((self.num_channels,self.msg_size), dtype=np.float64)

            # warmup loop to stabilize the stream before measurements
            counter = 0
            for i in range(500):
                now = time.monotonic()
                self.sample['time'] = (default_data * now).tobytes()
                self.r.xadd('random', self.sample, maxlen=None, approximate=True)
                counter += 1

            for i in range(self.n_messages):
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