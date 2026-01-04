import json
import os
import pickle
import time
import numpy as np
from datetime import datetime

import redis
import yaml

DURATION = None  # seconds
GRAPH = 'first_graph/first_graph.yaml'
REDIS_IP = '127.0.0.1'
REDIS_PORT = 6379
test_dir = 'brand-modules/linda-test'

with open(os.path.join(test_dir, 'graphs', GRAPH), 'r') as f:
    graph = yaml.safe_load(f)

r = redis.Redis(host=REDIS_IP, port=REDIS_PORT)

curs, start_streams = r.scan(0, _type='stream')
while curs != 0:
    curs, streams = r.scan(curs, _type='stream')
    start_streams += streams

# get the most recent ID from each stream
start_id = {}
for stream in start_streams:
    replies = r.xrevrange(stream, count=1)
    if replies:
        start_id[stream] = replies[0][0]

print(f'Starting graph from {GRAPH}')
r.xadd('supervisor_ipstream', {
    'commands': 'startGraph',
    'graph': json.dumps(graph)
})

if DURATION:
    print(f'Waiting {DURATION} seconds')
    time.sleep(DURATION)
else:
    input('Hit ENTER to stop graph...')

# Stop the graph
print('Stopping graph')
r.xadd('supervisor_ipstream', {'commands': 'stopGraph'})

curs, stop_streams = r.scan(0, _type='stream')
while curs != 0:
    curs, streams = r.scan(curs, _type='stream')
    stop_streams += streams

new_streams = [
    stream for stream in stop_streams if stream not in start_streams
]

for stream in new_streams:
    start_id[stream] = 0
