import subprocess
import time
import os
import signal
import argparse
import json
import redis


def start_graph(args, results_folder="results"):
    """Start the BRAND graph and wait for completion"""
    REDIS_IP = '127.0.0.1'
    REDIS_PORT = 6379

    # Define graph structure directly in Python
    graph = {
        'graph_name': 'first_graph',
        'nodes': [
            {
                'name': 'producer',
                'nickname': 'node_1',
                'module': '../brand-modules/benchmark',
                'redis-inputs': [],
                'run_priority': 99,
                'cpu_affinity': 5,
                'parameters': {
                    'log': 'INFO',
                    'n_messages': args.n_messages,
                    'msg_size': args.msg_size,
                    'num_channels': args.num_channels,
                    'max_buffer_size': args.max_buffer_size,
                    'output_file': f"../{results_folder}/{args.msg_size}_{args.num_channels}_Producer.csv"
                }
            },
            {
                'name': 'consumer',
                'nickname': 'node_2',
                'module': '../brand-modules/benchmark',
                'run_priority': 99,
                'cpu_affinity': 6,
                'parameters': {
                    'log': 'INFO',
                    'n_messages': args.n_messages,
                    'output_file': f"../{results_folder}/{args.msg_size}_{args.num_channels}_Consumer.csv"
                }
            }
        ]
    }
    
    r = redis.Redis(host=REDIS_IP, port=REDIS_PORT)

    print(f'Starting graph: {graph["graph_name"]}')
    print(f'Parameters: n_messages={args.n_messages}, msg_size={args.msg_size}, num_channels={args.num_channels}, max_buffer_size={args.max_buffer_size}')
    r.xadd('supervisor_ipstream', {
        'commands': 'startGraph',
        'graph': json.dumps(graph)
    })

    # Wait for both producer and consumer to signal completion
    print('Waiting for test to complete...')
    # Calculate timeout based on expected runtime
    expected_runtime = 10
    
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
    parser.add_argument("--max_buffer_size", type=int, default=10)
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--output_file", type=str, default="send_times.csv")
    args = parser.parse_args()

    results_folder = "rt_results"

    os.makedirs(results_folder, exist_ok=True)

    if os.path.exists(f"{results_folder}/{args.msg_size}_{args.num_channels}_Producer.csv") and not args.overwrite:
        return  # Skip if already exists
    
    # Set up environment with venv binaries in PATH
    env = os.environ.copy()
    env['PATH'] = f"/home/linda/Documents/Benchmark_IPC/brand-tutorial/brand/bin:{env.get('PATH', '')}"
    
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
        start_graph(args, results_folder)

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
