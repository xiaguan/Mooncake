#include <gflags/gflags.h>
#include <glog/logging.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <memory>
#include <string>
#include <vector>

#include "client.h"
#include "client_buffer.hpp"
#include "types.h"
#include "utils.h"

// Configuration flags
DEFINE_string(protocol, "rdma", "Transfer protocol: rdma|tcp");
DEFINE_string(device_name, "erdma_0",
              "Device name to use, valid if protocol=rdma");
DEFINE_string(master_address, "localhost:50051", "Address of master server");
DEFINE_int32(batch_size, 4, "Number of keys per batch");
DEFINE_int32(num_batches, 1000, "Number of batches to test");
DEFINE_int32(value_size, 1048576, "Size of values in bytes (default: 1MB)");
DEFINE_string(local_hostname, "localhost:12345", "Local hostname for client");
DEFINE_string(metadata_connection_string, "http://localhost:8080/metadata",
              "Metadata connection string");

namespace mooncake {
namespace benchmark {

// Global client and allocator instances
std::shared_ptr<Client> g_client = nullptr;
std::shared_ptr<ClientBufferAllocator> g_client_buffer_allocator = nullptr;
void* g_segment_ptr = nullptr;
size_t g_ram_buffer_size = 0;

// Performance measurement structures
struct BatchOperationResult {
    double latency_us;
    bool is_put;
    bool success;
    int batch_size;
};

bool initialize_segment() {
    // Calculate RAM buffer size based on total data to write + 10% overhead
    const size_t total_data_size = static_cast<size_t>(FLAGS_batch_size) *
                                   static_cast<size_t>(FLAGS_num_batches) *
                                   static_cast<size_t>(FLAGS_value_size);
    g_ram_buffer_size = static_cast<size_t>(total_data_size * 1.1);

    LOG(INFO) << "Allocating segment memory of size " << g_ram_buffer_size;

    g_segment_ptr = allocate_buffer_allocator_memory(g_ram_buffer_size);
    if (!g_segment_ptr) {
        LOG(ERROR) << "Failed to allocate segment memory of size "
                   << g_ram_buffer_size;
        return false;
    }

    auto result = g_client->MountSegment(g_segment_ptr, g_ram_buffer_size);
    if (!result.has_value()) {
        LOG(ERROR) << "Failed to mount segment: " << toString(result.error());
        return false;
    }

    LOG(INFO) << "Segment initialized successfully with "
              << g_ram_buffer_size / (1024 * 1024) << "MB RAM buffer";
    return true;
}

void cleanup_segment() {
    if (g_segment_ptr && g_client) {
        auto result =
            g_client->UnmountSegment(g_segment_ptr, g_ram_buffer_size);
        if (!result.has_value()) {
            LOG(ERROR) << "Failed to unmount segment: "
                       << toString(result.error());
        }
    }
}

bool initialize_client() {
    void** args =
        (FLAGS_protocol == "rdma") ? rdma_args(FLAGS_device_name) : nullptr;

    auto client_opt =
        Client::Create(FLAGS_local_hostname, FLAGS_metadata_connection_string,
                       FLAGS_protocol, args, FLAGS_master_address);

    if (!client_opt.has_value()) {
        LOG(ERROR) << "Failed to create client";
        return false;
    }

    LOG(INFO) << "Create client successfully";
    g_client = *client_opt;

    // Calculate client buffer size for batch operations + 10% overhead
    size_t batch_buffer_size = FLAGS_batch_size * FLAGS_value_size;
    size_t client_buffer_allocator_size =
        static_cast<size_t>(batch_buffer_size * 1.1);

    g_client_buffer_allocator =
        ClientBufferAllocator::create(client_buffer_allocator_size);

    auto result = g_client->RegisterLocalMemory(
        g_client_buffer_allocator->getBase(), client_buffer_allocator_size,
        "cpu:0", false, false);

    if (!result.has_value()) {
        LOG(ERROR) << "Failed to register local memory: "
                   << toString(result.error());
        return false;
    }

    LOG(INFO) << "Client initialized successfully with "
              << client_buffer_allocator_size / (1024 * 1024)
              << "MB buffer allocator";
    return true;
}

void cleanup_client() {
    if (g_client) {
        g_client.reset();
    }
    g_client_buffer_allocator.reset();
}

void run_batch_benchmark(std::vector<BatchOperationResult>& results) {
    ReplicateConfig config;
    config.replica_num = 1;

    std::vector<std::string> all_keys;

    // Generate all keys upfront
    for (int batch_idx = 0; batch_idx < FLAGS_num_batches; ++batch_idx) {
        for (int key_idx = 0; key_idx < FLAGS_batch_size; ++key_idx) {
            all_keys.push_back("batch_" + std::to_string(batch_idx) + "_key_" +
                               std::to_string(key_idx));
        }
    }

    // Phase 1: BatchPut operations
    LOG(INFO) << "Starting BatchPut phase...";

    for (int batch_idx = 0; batch_idx < FLAGS_num_batches; ++batch_idx) {
        std::vector<std::string> batch_keys;
        std::vector<std::vector<Slice>> batch_slices;
        std::vector<BufferHandle> batch_buffers;

        // Prepare batch data
        for (int key_idx = 0; key_idx < FLAGS_batch_size; ++key_idx) {
            int global_idx = batch_idx * FLAGS_batch_size + key_idx;
            batch_keys.push_back(all_keys[global_idx]);

            auto buffer = g_client_buffer_allocator->allocate(FLAGS_value_size);

            // Fill with simple pattern
            memset(buffer->ptr(), 'A' + (batch_idx % 26), FLAGS_value_size);

            std::vector<Slice> slices;
            slices.emplace_back(
                Slice{buffer->ptr(), static_cast<size_t>(FLAGS_value_size)});
            batch_slices.push_back(std::move(slices));
            batch_buffers.push_back(std::move(*buffer));
        }

        // Execute BatchPut
        auto start_time = std::chrono::high_resolution_clock::now();
        auto batch_put_results =
            g_client->BatchPut(batch_keys, batch_slices, config);
        auto end_time = std::chrono::high_resolution_clock::now();

        auto latency_us = std::chrono::duration_cast<std::chrono::microseconds>(
                              end_time - start_time)
                              .count();

        // Check results
        bool all_success = true;
        for (const auto& result : batch_put_results) {
            if (!result.has_value()) {
                all_success = false;
                break;
            }
        }

        results.push_back({static_cast<double>(latency_us), true, all_success,
                           FLAGS_batch_size});
    }

    // Phase 2: BatchGet operations
    LOG(INFO) << "Starting BatchGet phase...";

    for (int batch_idx = 0; batch_idx < FLAGS_num_batches; ++batch_idx) {
        std::vector<std::string> batch_keys;
        std::unordered_map<std::string, std::vector<Slice>> batch_slices;
        std::vector<BufferHandle> batch_buffers;

        // Prepare batch for reading
        for (int key_idx = 0; key_idx < FLAGS_batch_size; ++key_idx) {
            int global_idx = batch_idx * FLAGS_batch_size + key_idx;
            std::string key = all_keys[global_idx];
            batch_keys.push_back(key);

            auto buffer = g_client_buffer_allocator->allocate(FLAGS_value_size);

            std::vector<Slice> slices;
            slices.emplace_back(
                Slice{buffer->ptr(), static_cast<size_t>(FLAGS_value_size)});
            batch_slices[key] = slices;
            batch_buffers.push_back(std::move(*buffer));
        }

        // Execute BatchGet
        auto start_time = std::chrono::high_resolution_clock::now();
        auto batch_get_results = g_client->BatchGet(batch_keys, batch_slices);
        auto end_time = std::chrono::high_resolution_clock::now();

        auto latency_us = std::chrono::duration_cast<std::chrono::microseconds>(
                              end_time - start_time)
                              .count();

        // Check results
        bool all_success = true;
        for (const auto& result : batch_get_results) {
            if (!result.has_value()) {
                all_success = false;
                break;
            }
        }

        results.push_back({static_cast<double>(latency_us), false, all_success,
                           FLAGS_batch_size});
    }
}

void calculate_percentiles(std::vector<double>& latencies, double& p50,
                           double& p90, double& p95, double& p99) {
    if (latencies.empty()) {
        p50 = p90 = p95 = p99 = 0.0;
        return;
    }

    std::sort(latencies.begin(), latencies.end());
    size_t size = latencies.size();

    p50 = latencies[static_cast<size_t>(std::ceil((size * 0.50) - 1))];
    p90 = latencies[static_cast<size_t>(std::ceil((size * 0.90) - 1))];
    p95 = latencies[static_cast<size_t>(std::ceil((size * 0.95) - 1))];
    p99 = latencies[static_cast<size_t>(std::ceil((size * 0.99) - 1))];
}

void print_results(const std::vector<BatchOperationResult>& results,
                   double duration_s) {
    // Aggregate statistics
    uint64_t total_batches = 0;
    uint64_t successful_batches = 0;
    uint64_t total_put_batches = 0;
    uint64_t total_get_batches = 0;
    uint64_t total_keys_processed = 0;

    std::vector<double> all_latencies;
    std::vector<double> put_latencies;
    std::vector<double> get_latencies;

    for (const auto& result : results) {
        total_batches++;
        total_keys_processed += result.batch_size;

        if (result.success) {
            successful_batches++;
            all_latencies.push_back(result.latency_us);

            if (result.is_put) {
                total_put_batches++;
                put_latencies.push_back(result.latency_us);
            } else {
                total_get_batches++;
                get_latencies.push_back(result.latency_us);
            }
        }
    }

    // Calculate percentiles
    double all_p50, all_p90, all_p95, all_p99;
    double put_p50, put_p90, put_p95, put_p99;
    double get_p50, get_p90, get_p95, get_p99;

    calculate_percentiles(all_latencies, all_p50, all_p90, all_p95, all_p99);
    calculate_percentiles(put_latencies, put_p50, put_p90, put_p95, put_p99);
    calculate_percentiles(get_latencies, get_p50, get_p90, get_p95, get_p99);

    // Calculate throughput
    double batches_per_second = successful_batches / duration_s;
    double keys_per_second = total_keys_processed / duration_s;
    double data_throughput_mb_s =
        (total_keys_processed * FLAGS_value_size) / (duration_s * 1024 * 1024);

    // Print results
    LOG(INFO) << "=== Batch Benchmark Results ===";
    LOG(INFO) << "Test Duration: " << duration_s << " seconds";
    LOG(INFO) << "Batch Size: " << FLAGS_batch_size << " keys";
    LOG(INFO) << "Value Size: " << FLAGS_value_size << " bytes";
    LOG(INFO) << "Number of Batches: " << FLAGS_num_batches;
    LOG(INFO) << "";
    LOG(INFO) << "=== Batch Operation Statistics ===";
    LOG(INFO) << "Total Batches: " << total_batches;
    LOG(INFO) << "Successful Batches: " << successful_batches;
    LOG(INFO) << "BatchPut Operations: " << total_put_batches;
    LOG(INFO) << "BatchGet Operations: " << total_get_batches;
    LOG(INFO) << "Total Keys Processed: " << total_keys_processed;
    LOG(INFO) << "Success Rate: "
              << (100.0 * successful_batches / total_batches) << "%";
    LOG(INFO) << "";
    LOG(INFO) << "=== Throughput ===";
    LOG(INFO) << "Batches/sec: " << batches_per_second;
    LOG(INFO) << "Keys/sec: " << keys_per_second;
    LOG(INFO) << "Data Throughput (MB/s): " << data_throughput_mb_s;
    LOG(INFO) << "";
    LOG(INFO) << "=== Batch Latency (microseconds) ===";
    LOG(INFO) << "All Operations - P50: " << all_p50 << ", P90: " << all_p90
              << ", P95: " << all_p95 << ", P99: " << all_p99;

    if (!put_latencies.empty()) {
        LOG(INFO) << "BatchPut Operations - P50: " << put_p50
                  << ", P90: " << put_p90 << ", P95: " << put_p95
                  << ", P99: " << put_p99;
    }

    if (!get_latencies.empty()) {
        LOG(INFO) << "BatchGet Operations - P50: " << get_p50
                  << ", P90: " << get_p90 << ", P95: " << get_p95
                  << ", P99: " << get_p99;
    }
}

}  // namespace benchmark
}  // namespace mooncake

int main(int argc, char** argv) {
    // Initialize gflags and glog
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = 1;

    using namespace mooncake::benchmark;

    LOG(INFO) << "Starting Mooncake Store Batch Benchmark";
    LOG(INFO) << "Protocol: " << FLAGS_protocol
              << ", Device: " << FLAGS_device_name;
    LOG(INFO) << "Batch size: " << FLAGS_batch_size;
    LOG(INFO) << "Number of batches: " << FLAGS_num_batches;
    LOG(INFO) << "Value size: " << FLAGS_value_size << " bytes";

    // Initialize client
    if (!initialize_client()) {
        LOG(ERROR) << "Failed to initialize client";
        return 1;
    }

    // Initialize segment
    if (!initialize_segment()) {
        LOG(ERROR) << "Failed to initialize segment";
        cleanup_client();
        return 1;
    }

    // Run benchmark
    std::vector<BatchOperationResult> results;

    auto start_time = std::chrono::high_resolution_clock::now();
    run_batch_benchmark(results);
    auto end_time = std::chrono::high_resolution_clock::now();

    double actual_duration_s =
        std::chrono::duration_cast<std::chrono::milliseconds>(end_time -
                                                              start_time)
            .count() /
        1000.0;

    // Print results
    print_results(results, actual_duration_s);

    // Cleanup
    cleanup_segment();
    cleanup_client();
    google::ShutdownGoogleLogging();

    LOG(INFO) << "Batch benchmark completed successfully";
    return 0;
}