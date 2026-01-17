import gc
import numpy as np
import sys
import logging
import time
from brand import BRANDNode

class Producer(BRANDNode):
    def __init__(self):
        super().__init__()
        self.n_messages =  self.parameters["n_messages"]
        self.freq = self.parameters["freq"]
        self.msg_size = self.parameters["msg_size"]
        self.num_channels = self.parameters["num_channels"]
        self.output_file = self.parameters["output_file"]
        
        self.send_times = []
        self.PERIOD_S = 1/self.freq
        self.sample = {
            'time': 0.0,
        }

    def run(self):
        try:
            next_deadline = time.monotonic()
            default_data = np.ones((self.num_channels,self.msg_size), dtype=np.float64)

            for i in range(self.n_messages):
                while True:
                    now = time.monotonic()
                    if now >= next_deadline:
                        self.sample['time'] = (default_data * now).tobytes()
                        # print(data)
                        self.r.xadd('random', self.sample, maxlen=None, approximate=True)
                        self.send_times.append(now)
                        next_deadline += self.PERIOD_S
                        break
        finally:   
            self.terminate(None, None)

    def terminate(self, sig, frame):
        print(f"Total messages sent: {len(self.send_times)}")
        send_times = self.send_times[1:]
        send_times_diff = np.diff(send_times)
        print(f"Average send period (s): {np.mean(send_times_diff):.6f}")
        print(f"Max send period (s): {np.max(send_times_diff):.6f}, idx: {np.argmax(send_times_diff)}")
        print(f"Std send period (s): {np.std(send_times_diff):.6f}")
        save = np.vstack((send_times, np.append(send_times_diff, 0)))
        np.savetxt(self.output_file, save.T, delimiter=',', header='send_time,send_period')
        print("Producer stopped.")


if __name__ == "__main__":
    gc.disable()

    # setup
    producer = Producer()

    # main
    producer.run()

    gc.collect()