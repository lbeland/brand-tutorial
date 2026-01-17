import json
import os
import time

import redis
import yaml


def main():
    GRAPH = 'first_graph/first_graph.yaml'
    REDIS_IP = '127.0.0.1'
    REDIS_PORT = 6379
    test_dir = 'brand-modules/linda-test'

    with open(os.path.join(test_dir, 'graphs', GRAPH), 'r') as f:
        graph = yaml.safe_load(f)
    
    # Get parameters from environment or use defaults
    n_messages = int(os.environ.get('BENCHMARK_N_MESSAGES', '1000'))
    freq = float(os.environ.get('BENCHMARK_FREQ', '1000'))
    msg_size = int(os.environ.get('BENCHMARK_MSG_SIZE', '1'))
    num_channels = int(os.environ.get('BENCHMARK_NUM_CHANNELS', '1'))
    output_file = os.environ.get('BENCHMARK_OUTPUT_FILE', 'send_times.csv')
    
    # Add parameters to graph nodes
    for node in graph['nodes']:
        if node['name'] == 'producer':
            node['parameters']['n_messages'] = n_messages
            node['parameters']['freq'] = freq
            node['parameters']['msg_size'] = msg_size
            node['parameters']['num_channels'] = num_channels
            node['parameters']['output_file'] = output_file
        elif node['name'] == 'consumer':
            node['parameters']['n_messages'] = n_messages
            node['parameters']['output_file'] = output_file

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
    print(f'Parameters: n_messages={n_messages}, freq={freq}, msg_size={msg_size}, num_channels={num_channels}')
    r.xadd('supervisor_ipstream', {
        'commands': 'startGraph',
        'graph': json.dumps(graph)
    })

    # Wait for the producer to signal completion
    print('Waiting for test to complete...')
    # Calculate timeout based on expected runtime
    expected_runtime = (n_messages / freq) * 1000
    
    last_id = '0-0'
    
    while True:
        # Check for completion signal from producer
        messages = r.xread({'test_completion': last_id}, count=1, block=int(expected_runtime))
        if messages:
            for stream_name, entries in messages:
                for entry_id, entry_dict in entries:
                    if entry_dict.get(b'producer') == b'done':
                        return
                    last_id = entry_id


if __name__ == "__main__":
    main()
