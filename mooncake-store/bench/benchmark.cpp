#include "benchmark.h"

#include <glog/logging.h>

#include <cassert>
#include <chrono>
#include <cstdlib>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <random>
#include <sstream>

#include "mooncake_engine.h"
#include "redis_engine.h"

namespace mooncake {
namespace store {
namespace bench {

namespace {
// Helper function to generate random string
std::string RandomString(int length) {
    static const char alphanum[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, sizeof(alphanum) - 2);

    std::string result;
    result.reserve(length);
    for (int i = 0; i < length; ++i) {
        result += alphanum[dis(gen)];
    }

    return result;
}
}  // namespace

Benchmark::Benchmark(const BenchmarkConfig& config) : config_(config) {
    // Engine and message queue will be created per worker thread
}

std::unique_ptr<KVEngine> Benchmark::CreateEngine() {
    if (config_.engine_type == "redis") {
        return std::make_unique<RedisEngine>(
            config_.redis_host, config_.redis_port, config_.redis_password);
    } else if (config_.engine_type == "mooncake") {
        return std::make_unique<MooncakeEngine>();
    } else {
        LOG(FATAL) << "Unsupported engine type: " << config_.engine_type;
        return nullptr;
    }
}

std::string Benchmark::GenerateKey(int index) {
    std::stringstream ss;
    ss << "bench_key_" << std::setw(10) << std::setfill('0') << index;
    return ss.str();
}

std::string Benchmark::GenerateValue(int size_bytes) {
    return RandomString(size_bytes);
}

BenchmarkResult Benchmark::Run() {
    LOG(INFO) << "Starting benchmark with configuration:";
    LOG(INFO) << "  Mode: "
              << (config_.mode == BenchmarkMode::kPrefill ? "Prefill"
                                                          : "Decode");
    LOG(INFO) << "  Engine: " << config_.engine_type;
    LOG(INFO) << "  Value size: " << config_.value_size_bytes << " bytes";
    LOG(INFO) << "  Operations: " << config_.num_operations;
    LOG(INFO) << "  Threads: " << config_.num_threads;
    VLOG(1) << "  Each worker will use its own RedisEngine and message queue";

    // Prepare threads and result vectors
    std::vector<std::thread> threads;
    std::vector<double> thread_throughputs(config_.num_threads, 0.0);
    std::atomic<int> completed_ops(0);

    int ops_per_thread = config_.num_operations / config_.num_threads;
    int remainder = config_.num_operations % config_.num_threads;

    // Reset the exit signal
    should_exit_.store(false, std::memory_order_release);

    // Launch worker threads
    for (int i = 0; i < config_.num_threads; ++i) {
        int start_idx = i * ops_per_thread;
        int end_idx = start_idx + ops_per_thread;
        if (i == config_.num_threads - 1) {
            end_idx += remainder;  // Add remainder to the last thread
        }

        threads.emplace_back(&Benchmark::WorkerThread, this, i, start_idx,
                             end_idx, &thread_throughputs[i], &completed_ops);
    }

    // Wait for all operations to complete
    if (config_.mode == BenchmarkMode::kDecode) {
        // For decode mode, just wait for all operations to complete
        while (completed_ops.load(std::memory_order_acquire) <
               config_.num_operations) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            LOG(INFO) << "Main thread waiting for operations to complete: "
                      << completed_ops.load(std::memory_order_acquire) << "/"
                      << config_.num_operations;
        }
    } else {
        // For prefill mode, first wait for all operations to complete
        while (completed_ops.load(std::memory_order_acquire) <
               config_.num_operations) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            LOG(INFO) << "Main thread waiting for operations to complete: "
                      << completed_ops.load(std::memory_order_acquire) << "/"
                      << config_.num_operations;
        }

        // Then wait for completion messages from all decode threads
        auto message_queue = std::make_unique<RedisMessageQueue>();
        if (!message_queue->Init()) {
            LOG(ERROR)
                << "Main thread failed to initialize Redis message queue";
        } else {
            VLOG(1)
                << "Main thread Redis message queue initialized successfully";
        }

        LOG(INFO) << "Main thread waiting for " << config_.num_threads
                  << " completion messages from decode threads";

        int received_messages = 0;
        while (received_messages < config_.num_threads) {
            std::string completion_message;
            if (message_queue->Pop("decode", &completion_message, 60)) {
                received_messages++;
                LOG(INFO) << "Main thread received completion message "
                          << received_messages << "/" << config_.num_threads;
            } else {
                LOG(ERROR)
                    << "Main thread timed out waiting for completion message "
                    << (received_messages + 1) << "/" << config_.num_threads;
                break;  // Continue even if we don't receive all messages
            }
        }

        // Clean up message queue
        message_queue->Close();
    }

    // Signal all threads to exit
    LOG(INFO) << "Main thread signaling all threads to exit";
    should_exit_.store(true, std::memory_order_release);

    // Wait for all threads to complete
    for (auto& thread : threads) {
        thread.join();
    }

    // Combine results from all threads
    BenchmarkResult result;

    // Sum up throughputs from all threads
    result.throughput_gb_per_second = 0.0;
    for (const auto& throughput : thread_throughputs) {
        result.throughput_gb_per_second += throughput;
    }

    // Log results
    LOG(INFO) << "Benchmark completed:";
    LOG(INFO) << "  Total throughput: " << result.throughput_gb_per_second
              << " GB/s";

    return result;
}

void Benchmark::WorkerThread(int thread_id, int start_idx, int end_idx,
                             double* thread_throughput,
                             std::atomic<int>* completed) {
    // Create thread-local engine and message queue
    auto engine = CreateEngine();
    if (!engine->Init()) {
        LOG(FATAL) << "Thread " << thread_id
                   << " failed to initialize KV engine";
    }

    // Initialize the message queue with Redis host and port 6380
    auto message_queue = std::make_unique<RedisMessageQueue>();
    if (!message_queue->Init()) {
        LOG(ERROR) << "Thread " << thread_id
                   << " failed to initialize Redis message queue";
    } else {
        VLOG(1) << "Thread " << thread_id
                << " Redis message queue initialized successfully";
    }

    // Reset thread throughput
    *thread_throughput = 0.0;

    if (config_.mode == BenchmarkMode::kPrefill) {
        PrefillData(thread_id, start_idx, end_idx, thread_throughput, completed,
                    engine.get(), message_queue.get());
    } else {
        DecodeData(thread_id, start_idx, end_idx, thread_throughput, completed,
                   engine.get(), message_queue.get());
    }

    // Wait for the main thread to signal that all threads should exit
    LOG(INFO) << "Thread " << thread_id
              << " waiting for exit signal from main thread";

    while (!should_exit_.load(std::memory_order_acquire)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    LOG(INFO) << "Thread " << thread_id
              << " received exit signal from main thread";

    // Clean up thread-local resources
    engine->Close();
    message_queue->Close();
}

void Benchmark::PrefillData(int thread_id, int start_idx, int end_idx,
                            double* thread_throughput,
                            std::atomic<int>* completed, KVEngine* engine,
                            RedisMessageQueue* message_queue) {
    LOG(INFO) << "Thread " << thread_id << " prefilling data from " << start_idx
              << " to " << end_idx;

    // Pre-generate values to avoid measuring value generation time
    std::string value = GenerateValue(config_.value_size_bytes);

    // Calculate total data size in GB for this thread
    double total_data_gb = (end_idx - start_idx) * config_.value_size_bytes /
                           (1024.0 * 1024.0 * 1024.0);

    auto thread_start_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> engine_time(0);

    for (int i = start_idx; i < end_idx; ++i) {
        std::string key = GenerateKey(i);

        auto engine_start_time = std::chrono::high_resolution_clock::now();
        bool success = engine->Put(key, value);
        assert(success);
        auto engine_end_time = std::chrono::high_resolution_clock::now();
        engine_time += engine_end_time - engine_start_time;

        if (success) {
            VLOG(1) << "Thread " << thread_id
                    << " pushing key to queue: " << key;
            // Push key to queue for decode phase
            bool queue_success = PushToQueue(key, message_queue);
            if (!queue_success) {
                LOG(ERROR) << "Thread " << thread_id
                           << " failed to push key to queue: " << key;
                // Continue even if queue push fails
            }
            VLOG(1) << "Thread " << thread_id << " pushed key to queue end";
            completed->fetch_add(1, std::memory_order_relaxed);
        } else {
            LOG(ERROR) << "Thread " << thread_id
                       << " failed to put key: " << key;
        }

        // Log progress periodically
        int current_completed = completed->load(std::memory_order_relaxed);
        if (current_completed % 1000 == 0 || i == start_idx ||
            i == end_idx - 1) {
            VLOG(1) << "Thread " << thread_id
                    << " progress: " << (i - start_idx + 1) << "/"
                    << (end_idx - start_idx) << " operations completed";
            LOG(INFO) << "Total progress: " << current_completed << "/"
                      << config_.num_operations << " operations completed";
        }
    }

    auto thread_end_time = std::chrono::high_resolution_clock::now();
    double thread_time_seconds =
        std::chrono::duration<double>(thread_end_time - thread_start_time)
            .count();
    double engine_time_seconds = engine_time.count();

    // Calculate throughput in GB/s for this thread based only on KV engine time
    *thread_throughput = total_data_gb / engine_time_seconds;

    LOG(INFO) << "Thread " << thread_id << " completed prefilling data from "
              << start_idx << " to " << end_idx
              << " with throughput: " << *thread_throughput << " GB/s"
              << " (KV engine time only: " << engine_time_seconds << "s, "
              << "total time: " << thread_time_seconds << "s)";
}
void Benchmark::DecodeData(int thread_id, int start_idx, int end_idx,
                           double* thread_throughput,
                           std::atomic<int>* completed, KVEngine* engine,
                           RedisMessageQueue* message_queue) {
    LOG(INFO) << "Thread " << thread_id << " decoding data from " << start_idx
              << " to " << end_idx;

    // Calculate total data size in GB for this thread
    double total_data_gb = (end_idx - start_idx) * config_.value_size_bytes /
                           (1024.0 * 1024.0 * 1024.0);

    auto thread_start_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> engine_time(0);

    for (int i = start_idx; i < end_idx; ++i) {
        std::string key;
        if (!PopFromQueue(&key, message_queue)) {
            LOG(ERROR) << "Thread " << thread_id
                       << " failed to pop key from queue";
            continue;
        }

        std::string value;
        auto engine_start_time = std::chrono::high_resolution_clock::now();
        bool success = engine->Get(key, &value);
        auto engine_end_time = std::chrono::high_resolution_clock::now();
        engine_time += engine_end_time - engine_start_time;

        if (value.size() != config_.value_size_bytes) {
            LOG(ERROR) << "Thread " << thread_id
                       << " got key from engine with wrong value size: "
                       << value.size() << " != " << config_.value_size_bytes;
            continue;
        }

        VLOG(1) << "Thread " << thread_id << " got key from engine end";

        if (success) {
            completed->fetch_add(1, std::memory_order_relaxed);
        } else {
            LOG(ERROR) << "Thread " << thread_id
                       << " failed to get key: " << key;
        }

        // Log progress periodically
        int current_completed = completed->load(std::memory_order_relaxed);
        if (current_completed % 1000 == 0 || i == start_idx ||
            i == end_idx - 1) {
            VLOG(1) << "Thread " << thread_id
                    << " progress: " << (i - start_idx + 1) << "/"
                    << (end_idx - start_idx) << " operations completed";
            LOG(INFO) << "Total progress: " << current_completed << "/"
                      << config_.num_operations << " operations completed";
        }
    }

    auto thread_end_time = std::chrono::high_resolution_clock::now();
    double thread_time_seconds =
        std::chrono::duration<double>(thread_end_time - thread_start_time)
            .count();
    double engine_time_seconds = engine_time.count();

    // Calculate throughput in GB/s for this thread based only on KV engine time
    *thread_throughput = total_data_gb / engine_time_seconds;

    LOG(INFO) << "Thread " << thread_id << " completed decoding data from "
              << start_idx << " to " << end_idx
              << " with throughput: " << *thread_throughput << " GB/s"
              << " (KV engine time only: " << engine_time_seconds << "s, "
              << "total time: " << thread_time_seconds << "s)";

    // Push a completion message after each successful decode task
    bool queue_success = message_queue->Push("decode", "nothing");
    if (!queue_success) {
        LOG(ERROR) << "Thread " << thread_id
                   << " failed to push completion message";
    }
}

bool Benchmark::PushToQueue(const std::string& key,
                            RedisMessageQueue* message_queue) {
    if (!message_queue) {
        LOG(ERROR) << "Message queue not initialized";
        return false;
    }

    bool result = message_queue->Push(config_.queue_name, key);
    if (!result) {
        LOG(ERROR) << "Failed to push key to queue: " << key;
    }
    return result;
}

bool Benchmark::PopFromQueue(std::string* key,
                             RedisMessageQueue* message_queue) {
    if (key == nullptr) {
        return false;
    }

    if (!message_queue) {
        LOG(ERROR) << "Message queue not initialized";
        return false;
    }

    return message_queue->Pop(config_.queue_name, key);
}

}  // namespace bench
}  // namespace store
}  // namespace mooncake
