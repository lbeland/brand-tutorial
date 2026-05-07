import subprocess
import time
import os
import signal
import argparse
import json
import redis

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent


def start_graph(args, results_folder="c_results"):
    """Start the BRAND graph and wait for completion"""
    REDIS_IP = '127.0.0.1'
    REDIS_PORT = 6379

    os.makedirs(results_folder, exist_ok=True)

    # Define graph structure directly in Python
    graph = {
        'graph_name': 'first_graph',
        'nodes': [
            {
                'name': 'c-producer',
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
                    't_wait': str(args.t_wait),
                    'output_file': f"{results_folder}/{args.msg_size}_{args.num_channels}_Producer.csv"
                }
            },
            {
                'name': 'c-consumer',
                'nickname': 'node_2',
                'module': '../brand-modules/benchmark',
                'run_priority': 99,
                'cpu_affinity': 6,
                'parameters': {
                    'log': 'INFO',
                    'n_messages': args.n_messages,
                    'output_file': f"{results_folder}/{args.msg_size}_{args.num_channels}_Consumer.csv"
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
    
    last_id = '0-0'
    consumer_done = False
    
    while not consumer_done:
        # Check for completion signals from both nodes
        messages = r.xread({'test_completion': last_id}, count=1, block=5*1000)
        if messages:
            for stream_name, entries in messages:
                for entry_id, entry_dict in entries:
                    if entry_dict.get(b'consumer') == b'done':
                        consumer_done = True
                        break
                    last_id = entry_id
                else:
                    continue
                break
            else:
                continue
            break
        else:
            # Timeout - no message received
            print("Waiting for completion")
    
    if consumer_done:
        print("Test completed successfully")
    else:
        print("Test incomplete - timeout occurred")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--n_messages", type=int, default=100000)
    parser.add_argument("--msg_size", type=int, default=1)
    parser.add_argument("--num_channels", type=int, default=1)
    parser.add_argument("--max_buffer_size", type=int, default=100)
    parser.add_argument("--t_wait", type=float, default=0.0001)
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--output_file", type=str, default="Producer.csv")
    args = parser.parse_args()

    results_folder = f"{REPO_ROOT}/rt_c_results"
    os.makedirs(results_folder, exist_ok=True)

    if os.path.exists(f"{results_folder}/{args.msg_size}_{args.num_channels}_Consumer.csv") and not args.overwrite:
        return
    
    # Set up environment with venv binaries in PATH
    env = os.environ.copy()
    env['PATH'] = f"{REPO_ROOT}/brand/bin:{env.get('PATH', '')}"
    
    # 1. Start supervisor with venv activated
    supervisor = subprocess.Popen(
        [f"{REPO_ROOT}/rt/bin/python3", "supervisor/supervisor.py"],
        cwd=f"{REPO_ROOT}/brand",
        env=env,
        preexec_fn=os.setsid,
    )

    try:
        # Give supervisor time to start Redis and initialize
        time.sleep(2)
        # 2. Start the graph and wait for completion
        start_graph(args, results_folder=results_folder)

    except KeyboardInterrupt:
        print("Interrupted by user")
    except Exception as e:
        print(f"Error during benchmark: {e}")
    finally:
        terminate(supervisor)
        print(f"Benchmark complete")


def terminate(proc):
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGINT)
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        os.killpg(os.getpgid(proc.pid), signal.SIGINT)


if __name__ == "__main__":
    main()
