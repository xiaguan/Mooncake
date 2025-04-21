#include <gflags/gflags.h>
#include <glog/logging.h>

#include <algorithm>  // For std::sort
#include <iostream>
#include <string>

#include "benchmark.h"

// Define command line flags
DEFINE_string(mode, "prefill", "Benchmark mode: prefill or decode");
DEFINE_string(engine, "redis", "KV engine to benchmark: redis");
DEFINE_int32(value_size, 128, "Size of values in bytes");
DEFINE_int32(num_ops, 1000, "Number of operations to perform");
DEFINE_int32(num_threads, 1, "Number of concurrent threads");
DEFINE_string(redis_host, "localhost", "Redis server hostname");
DEFINE_int32(redis_port, 6379, "Redis server port");
DEFINE_string(redis_password, "", "Redis server password");
DEFINE_string(queue_name, "bench_queue",
              "Redis queue name for message passing");

int main(int argc, char* argv[]) {
    // Initialize Google's logging library.
    google::InitGoogleLogging(argv[0]);

    // Set VLOG level to 1 for detailed logs
    google::SetVLOGLevel("*", 1);

    // log to stderr
    FLAGS_logtostderr = true;

    // Parse command line flags
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    // Configure benchmark
    mooncake::store::bench::BenchmarkConfig config;

    // Set benchmark mode
    if (FLAGS_mode == "prefill") {
        config.mode = mooncake::store::bench::BenchmarkMode::kPrefill;
    } else if (FLAGS_mode == "decode") {
        config.mode = mooncake::store::bench::BenchmarkMode::kDecode;
    } else {
        LOG(FATAL) << "Invalid mode: " << FLAGS_mode
                   << ". Must be 'prefill' or 'decode'";
    }

    // Set engine type
    if (FLAGS_engine == "redis") {
        config.engine_type = "redis";
    } else if (FLAGS_engine == "mooncake") {
        config.engine_type = "mooncake";
    } else {
        LOG(FATAL) << "Invalid engine: " << FLAGS_engine
                   << ". Must be 'redis' or 'mooncake'";
    }

    // Set other configuration parameters
    config.value_size_bytes = FLAGS_value_size;
    config.num_operations = FLAGS_num_ops;
    config.num_threads = FLAGS_num_threads;
    config.redis_host = FLAGS_redis_host;
    config.redis_port = FLAGS_redis_port;
    config.redis_password = FLAGS_redis_password;
    config.queue_name = FLAGS_queue_name;

    // Create and run benchmark
    LOG(INFO) << "Starting benchmark...";
    mooncake::store::bench::Benchmark benchmark(config);
    mooncake::store::bench::BenchmarkResult result = benchmark.Run();

    // Print results
    LOG(INFO) << "Benchmark Results:";
    LOG(INFO) << "  Mode: " << FLAGS_mode;
    LOG(INFO) << "  Engine: " << FLAGS_engine;
    LOG(INFO) << "  Value Size: " << FLAGS_value_size << " bytes";
    LOG(INFO) << "  Throughput: " << result.throughput_gb_per_second << " GB/s";

    return 0;
}
