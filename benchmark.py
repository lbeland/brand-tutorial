import subprocess
import time
import os
import signal
import argparse
import json
import redis


def start_graph(args):
    """Start the BRAND graph and wait for completion"""
    REDIS_IP = '127.0.0.1'
    REDIS_PORT = 6379

    os.makedirs("results", exist_ok=True)

    # Define graph structure directly in Python
    graph = {
        'graph_name': 'first_graph',
        'nodes': [
            {
                'name': 'c-producer',
                'nickname': 'node_1',
                'module': '../brand-modules/benchmark',
                'redis-inputs': [],
                'parameters': {
                    'log': 'INFO',
                    'n_messages': args.n_messages,
                    'freq': args.freq,
                    'msg_size': args.msg_size,
                    'num_channels': args.num_channels,
                    'output_file': f"../results/{args.freq}_{args.msg_size}_{args.num_channels}_send_times.csv"
                }
            },
            {
                'name': 'c-consumer',
                'nickname': 'node_2',
                'module': '../brand-modules/benchmark',
                'parameters': {
                    'log': 'INFO',
                    'n_messages': args.n_messages,
                    'output_file': f"../results/{args.freq}_{args.msg_size}_{args.num_channels}_recv_times.csv"
                }
            }
        ]
    }
    
    r = redis.Redis(host=REDIS_IP, port=REDIS_PORT)

    print(f'Starting graph: {graph["graph_name"]}')
    print(f'Parameters: n_messages={args.n_messages}, freq={args.freq}, msg_size={args.msg_size}, num_channels={args.num_channels}')
    r.xadd('supervisor_ipstream', {
        'commands': 'startGraph',
        'graph': json.dumps(graph)
    })

    # Wait for both producer and consumer to signal completion
    print('Waiting for test to complete...')
    # Calculate timeout based on expected runtime
    expected_runtime = (args.n_messages / args.freq) + 10
    
    last_id = '0-0'
    consumer_done = False
    
    while not consumer_done:
        # Check for completion signals from both nodes
        messages = r.xread({'test_completion': last_id}, count=1, block=int(expected_runtime * 1000))
        if messages:
            for stream_name, entries in messages:
                for entry_id, entry_dict in entries:
                    if entry_dict.get(b'consumer') == b'done':
                        consumer_done = True
                        print("Consumer completed")
                    last_id = entry_id
        else:
            # Timeout - no message received
            print("Timeout waiting for completion")
            break
    
    if consumer_done:
        print("Test completed successfully")
    else:
        print("Test incomplete - timeout occurred")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--n_messages", type=int, default=1000)
    parser.add_argument("--freq", type=int, default=1000)
    parser.add_argument("--msg_size", type=int, default=100)
    parser.add_argument("--num_channels", type=int, default=10)
    parser.add_argument("--output_file", type=str, default="send_times.csv")
    args = parser.parse_args()

    os.makedirs("results", exist_ok=True)
    
    # Set up environment with venv binaries in PATH
    env = os.environ.copy()
    env['PATH'] = f"/home/linda/brand-tutorial/brand/bin:{env.get('PATH', '')}"
    
    # 1. Start supervisor with venv activated
    supervisor_cmd = """
    source rt/bin/activate && \
    cd brand && \
    python3 supervisor/supervisor.py
    """
    
    supervisor = subprocess.Popen(
        supervisor_cmd,
        shell=True,
        executable="/bin/bash",
        env=env,
    )

    # Give supervisor time to start Redis and initialize
    time.sleep(2)
    
    try:
        # 2. Start the graph and wait for completion
        start_graph(args)

    except KeyboardInterrupt:
        print("Interrupted by user")
    except Exception as e:
        print(f"Error during benchmark: {e}")
    finally:
        terminate(supervisor)
        print(f"Benchmark complete")


def terminate(proc):
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    except Exception:
        pass


if __name__ == "__main__":
    main()
