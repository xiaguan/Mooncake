# Mooncake Store Benchmark

This directory contains benchmarking tools for the Mooncake Store project.

## Automated Benchmark Script

The `run_benchmark.sh` script automates the process of running benchmarks with different engines (Redis or Mooncake) and handles the setup and teardown of required services.

### Prerequisites

The following executables should be available in your system:

- Redis server and client (`/usr/bin/redis-server` and `/usr/bin/redis-cli`)
- etcd (`/usr/bin/etcd`) - Required for Mooncake engine
- Mooncake Master (`/usr/bin/mooncake_master`) - Required for Mooncake engine
- The benchmark worker executable (`./worker`)

### Usage

```bash
./run_benchmark.sh [OPTIONS]
```

#### Options

- `--engine=ENGINE`: Specify the engine to use (redis or mooncake)
- `--value-size=SIZE`: Size of values in bytes (default: 1048576)
- `--num-ops=OPS`: Number of operations to perform (default: 1000)
- `--num-threads=THREADS`: Number of concurrent threads (default: 2)
- `--help`: Display help message

### Examples

Run benchmark with Redis engine (default):

```bash
./run_benchmark.sh
```

Run benchmark with Mooncake engine:

```bash
./run_benchmark.sh --engine=mooncake
```

Run benchmark with custom parameters:

```bash
./run_benchmark.sh --engine=redis --value-size=4096 --num-ops=10000 --num-threads=4
```

### How It Works

The script performs the following steps:

1. Starts a Redis server on port 6380 for message queue
2. Based on the engine type:
   - If Redis: Starts another Redis server on port 6379, then starts prefill and decode nodes
   - If Mooncake: Starts a mooncake_master and etcd
3. Runs the worker in prefill mode
4. Waits for 1 second
5. Runs the worker in decode mode
6. Prints benchmark results
7. Cleans up resources (stops all services)

### Logs and Temporary Files

The script creates the following files in `/tmp/mooncake_benchmark/`:

- Log files for each service
- PID files for tracking processes
- Benchmark output logs

These files are useful for debugging if something goes wrong.

## Manual Benchmarking

If you prefer to run benchmarks manually, you can use the worker executable directly:

```bash
samply record ./mooncake-store/bench/worker --mode=prefill --value_size=16777200 --num_ops=2000 --num_threads=8 --redis_host=localhost --engine=mooncake
```

Available options:

- `--mode`: Benchmark mode (prefill or decode)
- `--engine`: KV engine to benchmark (redis or mooncake)
- `--value_size`: Size of values in bytes
- `--num_ops`: Number of operations to perform
- `--num_threads`: Number of concurrent threads
- `--redis_host`: Redis server hostname
- `--redis_port`: Redis server port
- `--redis_password`: Redis server password
- `--queue_name`: Redis queue name for message passing

Note that when running manually, you need to set up the required services (Redis, etcd, Mooncake Master) yourself.
