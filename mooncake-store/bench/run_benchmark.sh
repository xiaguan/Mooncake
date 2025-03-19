#!/bin/bash
#
# Automated benchmark script for mooncake-store
# This script automates the process of running benchmarks with different engines
# (redis or mooncake) and handles the setup and teardown of required services.

set -e  # Exit immediately if a command exits with a non-zero status

# ====================== Configuration Variables ======================
# Paths to executables
REDIS_SERVER="/usr/local/bin/redis-server"
REDIS_CLI="/usr/local/bin/redis-cli"
ETCD="/usr/local/bin/etcd"
MOONCAKE_MASTER="/home/ubuntu/Mooncake/build/mooncake-store/src/mooncake_master"
WORKER="/home/ubuntu/Mooncake/build/mooncake-store/bench/worker"

# Redis configuration
REDIS_MQ_PORT=6380  # Message queue Redis port
REDIS_ENGINE_PORT=6379  # Redis engine port
REDIS_HOST="localhost"

# Benchmark configuration
ENGINE="mooncake"  # Options: redis, mooncake
# 8mb
VALUE_SIZE=16777200
NUM_OPS=3000
NUM_THREADS=8

# Temporary files for process IDs
TEMP_DIR="/tmp/mooncake_benchmark"
REDIS_MQ_PID_FILE="${TEMP_DIR}/redis_mq.pid"
REDIS_ENGINE_PID_FILE="${TEMP_DIR}/redis_engine.pid"
ETCD_PID_FILE="${TEMP_DIR}/etcd.pid"
MOONCAKE_MASTER_PID_FILE="${TEMP_DIR}/mooncake_master.pid"
PREFILL_PID_FILE="${TEMP_DIR}/prefill.pid"
DECODE_PID_FILE="${TEMP_DIR}/decode.pid"

# Log files
REDIS_MQ_LOG="${TEMP_DIR}/redis_mq.log"
REDIS_ENGINE_LOG="${TEMP_DIR}/redis_engine.log"
ETCD_LOG="${TEMP_DIR}/etcd.log"
MOONCAKE_MASTER_LOG="${TEMP_DIR}/mooncake_master.log"
PREFILL_LOG="${TEMP_DIR}/prefill.log"
DECODE_LOG="${TEMP_DIR}/decode.log"

# ====================== Helper Functions ======================

# Print usage information
usage() {
    echo "Usage: $0 [OPTIONS]"
    echo "Options:"
    echo "  --engine=ENGINE       Specify the engine to use (redis or mooncake)"
    echo "  --value-size=SIZE     Size of values in bytes (default: 1048576)"
    echo "  --num-ops=OPS         Number of operations to perform (default: 1000)"
    echo "  --num-threads=THREADS Number of concurrent threads (default: 2)"
    echo "  --help                Display this help message"
    exit 1
}

# Print a message with timestamp
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

# Check if a command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Check if a process is running
is_process_running() {
    local pid=$1
    if [ -z "$pid" ]; then
        return 1
    fi
    if ps -p "$pid" > /dev/null; then
        return 0
    else
        return 1
    fi
}

# Kill a process and wait for it to terminate
kill_process() {
    local pid=$1
    local name=$2
    if is_process_running "$pid"; then
        log "Stopping $name (PID: $pid)..."
        kill "$pid" 2>/dev/null || true
        # Wait for the process to terminate
        for i in {1..10}; do
            if ! is_process_running "$pid"; then
                break
            fi
            sleep 0.5
        done
        # Force kill if still running
        if is_process_running "$pid"; then
            log "Force killing $name (PID: $pid)..."
            kill -9 "$pid" 2>/dev/null || true
        fi
    fi
}

# Clean up all resources
cleanup() {
    log "Cleaning up resources..."
    
    # Read PIDs from files
    [ -f "$REDIS_MQ_PID_FILE" ] && REDIS_MQ_PID=$(cat "$REDIS_MQ_PID_FILE")
    [ -f "$REDIS_ENGINE_PID_FILE" ] && REDIS_ENGINE_PID=$(cat "$REDIS_ENGINE_PID_FILE")
    [ -f "$ETCD_PID_FILE" ] && ETCD_PID=$(cat "$ETCD_PID_FILE")
    [ -f "$MOONCAKE_MASTER_PID_FILE" ] && MOONCAKE_MASTER_PID=$(cat "$MOONCAKE_MASTER_PID_FILE")
    [ -f "$PREFILL_PID_FILE" ] && PREFILL_PID=$(cat "$PREFILL_PID_FILE")
    [ -f "$DECODE_PID_FILE" ] && DECODE_PID=$(cat "$DECODE_PID_FILE")
    
    # Kill processes
    kill_process "$REDIS_MQ_PID" "Redis Message Queue"
    kill_process "$REDIS_ENGINE_PID" "Redis Engine"
    kill_process "$ETCD_PID" "etcd"
    kill_process "$MOONCAKE_MASTER_PID" "Mooncake Master"
    kill_process "$PREFILL_PID" "Prefill Worker"
    kill_process "$DECODE_PID" "Decode Worker"
    
    # Clean up Redis databases
    if command_exists "$REDIS_CLI"; then
        log "Flushing Redis databases..."
        "$REDIS_CLI" -p "$REDIS_MQ_PORT" flushall > /dev/null 2>&1 || true
        "$REDIS_CLI" -p "$REDIS_ENGINE_PORT" flushall > /dev/null 2>&1 || true
    fi
    
    # Remove temporary files
    rm -f "$REDIS_MQ_PID_FILE" "$REDIS_ENGINE_PID_FILE" "$ETCD_PID_FILE" "$MOONCAKE_MASTER_PID_FILE" "$PREFILL_PID_FILE" "$DECODE_PID_FILE"
    
    log "Cleanup completed"
}

# Start Redis server
start_redis() {
    local port=$1
    local pid_file=$2
    local log_file=$3
    local name=$4
    
    log "Starting $name on port $port..."
    
    # Check if Redis is already running on this port
    if nc -z localhost "$port" 2>/dev/null; then
        log "Warning: Port $port is already in use. Stopping existing Redis instance..."
        "$REDIS_CLI" -p "$port" shutdown || true
        sleep 1
    fi
    
    # Start Redis server
    "$REDIS_SERVER" --port "$port" --daemonize yes --pidfile "$pid_file" --logfile "$log_file"
    
    # Wait for Redis to start
    for i in {1..10}; do
        if nc -z localhost "$port" 2>/dev/null; then
            log "$name started successfully"
            return 0
        fi
        sleep 0.5
    done
    
    log "Error: Failed to start $name on port $port"
    return 1
}

# Start etcd
start_etcd() {
    log "Starting etcd..."
    
    # Start etcd in the background
    "$ETCD" > "$ETCD_LOG" 2>&1 &
    ETCD_PID=$!
    echo "$ETCD_PID" > "$ETCD_PID_FILE"
    
    # Wait for etcd to start
    for i in {1..10}; do
        if nc -z localhost 2379 2>/dev/null; then
            log "etcd started successfully"
            return 0
        fi
        sleep 0.5
    done
    
    log "Error: Failed to start etcd"
    return 1
}

# Start Mooncake Master
start_mooncake_master() {
    log "Starting Mooncake Master..."
    
    # Start Mooncake Master in the background
    "$MOONCAKE_MASTER" --enable_gc=false > "$MOONCAKE_MASTER_LOG" 2>&1 &
    MOONCAKE_MASTER_PID=$!
    echo "$MOONCAKE_MASTER_PID" > "$MOONCAKE_MASTER_PID_FILE"
    
    # Wait for Mooncake Master to start (assuming it listens on port 50051)
    for i in {1..10}; do
        if nc -z localhost 50051 2>/dev/null; then
            log "Mooncake Master started successfully"
            return 0
        fi
        sleep 0.5
    done
    
    log "Error: Failed to start Mooncake Master"
    return 1
}

# Run worker in prefill mode
run_prefill() {
    log "Starting worker in prefill mode..."
    
    "$WORKER" \
        --mode=prefill \
        --engine="$ENGINE" \
        --value_size="$VALUE_SIZE" \
        --num_ops="$NUM_OPS" \
        --num_threads="$NUM_THREADS" \
        --redis_host="$REDIS_HOST" \
        --redis_port="$REDIS_ENGINE_PORT" > "$PREFILL_LOG" 2>&1 &
    
    PREFILL_PID=$!
    echo "$PREFILL_PID" > "$PREFILL_PID_FILE"
    log "Prefill worker started in background (PID: $PREFILL_PID)"
    
    # Wait for 1 seconds instead of waiting for completion
    log "Waiting for 1 seconds to consider prefill as successful..."
    sleep 1
    
    log "Prefill considered successful"
    return 0
}

# Run worker in decode mode
run_decode() {
    log "Starting worker in decode mode..."
    
    "$WORKER" \
        --mode=decode \
        --engine="$ENGINE" \
        --value_size="$VALUE_SIZE" \
        --num_ops="$NUM_OPS" \
        --num_threads="$NUM_THREADS" \
        --redis_host="$REDIS_HOST" \
        --redis_port="$REDIS_ENGINE_PORT" > "$DECODE_LOG" 2>&1 &
    
    DECODE_PID=$!
    echo "$DECODE_PID" > "$DECODE_PID_FILE"
    log "Decode worker started in background (PID: $DECODE_PID)"
    
    # Wait for 1 seconds instead of waiting for completion
    log "Waiting for 1 seconds to consider decode as successful..."
    sleep 1
    
    log "Decode considered successful"
    return 0
}

# Print benchmark results
print_results() {
    log "Benchmark Results:"
    echo "----------------------------------------"
    echo "Engine: $ENGINE"
    echo "Value Size: $VALUE_SIZE bytes"
    echo "Operations: $NUM_OPS"
    echo "Threads: $NUM_THREADS"
    echo "----------------------------------------"
    
    echo "Prefill Results:"
    if [ -f "$PREFILL_LOG" ] && grep -q "Throughput:" "$PREFILL_LOG"; then
        grep -A 1 "Benchmark Results:" "$PREFILL_LOG" | tail -n +2
        grep "Throughput:" "$PREFILL_LOG"
    else
        echo "Prefill is still running in background (PID: $(cat "$PREFILL_PID_FILE" 2>/dev/null || echo "unknown"))"
        echo "Check $PREFILL_LOG for results when completed"
    fi
    
    echo "----------------------------------------"
    
    echo "Decode Results:"
    if [ -f "$DECODE_LOG" ] && grep -q "Throughput:" "$DECODE_LOG"; then
        grep -A 1 "Benchmark Results:" "$DECODE_LOG" | tail -n +2
        grep "Throughput:" "$DECODE_LOG"
    else
        echo "Decode is still running in background (PID: $(cat "$DECODE_PID_FILE" 2>/dev/null || echo "unknown"))"
        echo "Check $DECODE_LOG for results when completed"
    fi
    
    echo "----------------------------------------"
}

# ====================== Main Script ======================

# Parse command line arguments
for arg in "$@"; do
    case $arg in
        --engine=*)
            ENGINE="${arg#*=}"
            ;;
        --value-size=*)
            VALUE_SIZE="${arg#*=}"
            ;;
        --num-ops=*)
            NUM_OPS="${arg#*=}"
            ;;
        --num-threads=*)
            NUM_THREADS="${arg#*=}"
            ;;
        --help)
            usage
            ;;
        *)
            echo "Unknown option: $arg"
            usage
            ;;
    esac
done

# Validate engine type
if [ "$ENGINE" != "redis" ] && [ "$ENGINE" != "mooncake" ]; then
    echo "Error: Invalid engine type. Must be 'redis' or 'mooncake'."
    usage
fi

# Check for required executables
if ! command_exists "$REDIS_SERVER"; then
    echo "Error: Redis server not found at $REDIS_SERVER"
    exit 1
fi

if ! command_exists "$REDIS_CLI"; then
    echo "Error: Redis CLI not found at $REDIS_CLI"
    exit 1
fi

if [ "$ENGINE" = "mooncake" ]; then
    if ! command_exists "$ETCD"; then
        echo "Error: etcd not found at $ETCD"
        exit 1
    fi
    
    if ! command_exists "$MOONCAKE_MASTER"; then
        echo "Error: Mooncake Master not found at $MOONCAKE_MASTER"
        exit 1
    fi
fi

if [ ! -x "$WORKER" ]; then
    echo "Error: Worker executable not found or not executable at $WORKER"
    exit 1
fi

# Clean up temporary directory
rm -rf "$TEMP_DIR"
# Clean up .rdb filles
rm -f ./*.rdb
# Create temporary directory
mkdir -p "$TEMP_DIR"
# Cealnup etcd
rm -rf default.etcd

# Set up trap to clean up on exit
trap cleanup EXIT INT TERM

# Print benchmark configuration
log "Starting benchmark with configuration:"
log "  Engine: $ENGINE"
log "  Value Size: $VALUE_SIZE bytes"
log "  Operations: $NUM_OPS"
log "  Threads: $NUM_THREADS"

# Start Redis for message queue
start_redis "$REDIS_MQ_PORT" "$REDIS_MQ_PID_FILE" "$REDIS_MQ_LOG" "Redis Message Queue"

# Start services based on engine type
if [ "$ENGINE" = "redis" ]; then
    # Start Redis for Redis engine
    start_redis "$REDIS_ENGINE_PORT" "$REDIS_ENGINE_PID_FILE" "$REDIS_ENGINE_LOG" "Redis Engine"
    
    # Run prefill worker
    run_prefill
    
    # Wait a bit for the queue to stabilize
    log "Waiting for 1 second before starting decode..."
    sleep 1
    
    # Run decode worker
    run_decode

    # wait prefill and decode to finish
    wait $PREFILL_PID $DECODE_PID
elif [ "$ENGINE" = "mooncake" ]; then
    # Start etcd
    start_etcd
    
    # Start Mooncake Master
    start_mooncake_master
    
    # Run prefill worker
    run_prefill
    
    # Wait a bit for the queue to stabilize
    log "Waiting for 1 seconds before starting decode..."
    sleep 1
    
    # Run decode worker
    run_decode

    # wait prefill and decode to finish
    wait $PREFILL_PID $DECODE_PID
fi

# Print benchmark results
print_results

log "Benchmark completed successfully"
exit 0
