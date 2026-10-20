# brand-tutorial — IPC benchmark

Origin: https://github.com/brandbci/brand-tutorial/tree/main

[BRAND](https://github.com/brandbci/brand-tutorial) is a BCI framework
where nodes communicate over Redis streams, orchestrated by a supervisor
process. Two benchmark variants live under `brand-modules/benchmark/nodes/`:

- `benchmark.py` — starts the supervisor + pure-Python producer/consumer nodes.
- `c_benchmark.py` — starts the supervisor + C producer/consumer nodes.

## Install

From the repo root:

```bash
./bootstrap.sh --only brand-tutorial --with-builds
```

This creates `brand-tutorial/rt` (Python 3.8, since upstream BRAND pins
`numpy==1.18`/`torch==1.12`) and, with `--with-builds`, compiles
`redis-server`/`redis-cli`, `hiredis`, and the four benchmark nodes via
`brand/Makefile`. This step is not parallelized (`make -j`) — redis's own
Makefile doesn't correctly order its `jemalloc` dependency before compiling
`src/`, so parallel jobs race.

## Run the benchmark

Python variant:

```bash
cd brand-tutorial
rt/bin/python3 benchmark.py \
  --n_messages 1000 \
  --num_channels 10 \
  --msg_size 100 \
  --t_wait 0.0001 \
  --overwrite
```

C variant:

```bash
cd brand-tutorial
rt/bin/python3 c_benchmark.py \
  --n_messages 100000 \
  --num_channels 1 \
  --msg_size 1 \
  --t_wait 0.0001 \
  --overwrite
```

Both start their own `redis-server` via the BRAND supervisor — no separate
Redis setup needed.

- `--n_messages`: number of timed messages sent
- `--num_channels` / `--msg_size`: shape of each message
- `--t_wait`: seconds the producer sleeps between sends (0 = no wait)
- `--overwrite`: rerun even if results for this channel/size combination
  already exist

Python-variant results are written to
`rt_results/<msg_size>_<num_channels>_{Producer,Consumer}.csv`, C-variant
results to `rt_c_results/` in the same naming scheme.
