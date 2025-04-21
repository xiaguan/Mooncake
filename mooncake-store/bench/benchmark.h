#ifndef MOONCAKE_STORE_BENCH_BENCHMARK_H_
#define MOONCAKE_STORE_BENCH_BENCHMARK_H_

#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "kv_engine.h"
#include "redis_message_queue.h"

namespace mooncake {
namespace store {
namespace bench {

// Benchmark modes
enum class BenchmarkMode {
    kPrefill,  // Prefill data into the KV store
    kDecode    // Read data from the KV store
};

// Benchmark results
struct BenchmarkResult {
    double throughput_gb_per_second;  // Throughput in GB/s
};

// Benchmark configuration
struct BenchmarkConfig {
    BenchmarkMode mode;
    std::string engine_type;  // "redis" or "mooncake-store"
    size_t value_size_bytes;
    int num_operations;
    int num_threads;
    std::string redis_host;
    int redis_port;
    std::string redis_password;
    std::string queue_name;  // Redis queue name for message passing
};

// Benchmark class for running performance tests
class Benchmark {
   public:
    explicit Benchmark(const BenchmarkConfig& config);
    ~Benchmark() = default;

    // Run the benchmark
    BenchmarkResult Run();

   private:
    BenchmarkConfig config_;
    std::atomic<bool> should_exit_{false};  // Signal for all threads to exit

    // Create a KV engine based on the configuration
    std::unique_ptr<KVEngine> CreateEngine();

    // Generate a random key
    std::string GenerateKey(int index);

    // Generate a random value of specified size
    std::string GenerateValue(int size_bytes);

    // Worker function for benchmark threads
    void WorkerThread(int thread_id, int start_idx, int end_idx,
                      double* thread_throughput, std::atomic<int>* completed);

    // Prefill data into the KV store
    void PrefillData(int thread_id, int start_idx, int end_idx,
                     double* thread_throughput, std::atomic<int>* completed,
                     KVEngine* engine, RedisMessageQueue* message_queue);

    // Decode (read) data from the KV store
    void DecodeData(int thread_id, int start_idx, int end_idx,
                    double* thread_throughput, std::atomic<int>* completed,
                    KVEngine* engine, RedisMessageQueue* message_queue);

    // Push a message to the Redis queue
    bool PushToQueue(const std::string& key, RedisMessageQueue* message_queue);

    // Pop a message from the Redis queue
    bool PopFromQueue(std::string* key, RedisMessageQueue* message_queue);
};

}  // namespace bench
}  // namespace store
}  // namespace mooncake

#endif  // MOONCAKE_STORE_BENCH_BENCHMARK_H_
