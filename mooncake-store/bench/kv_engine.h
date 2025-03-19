#ifndef MOONCAKE_STORE_BENCH_KV_ENGINE_H_
#define MOONCAKE_STORE_BENCH_KV_ENGINE_H_

#include <string>

namespace mooncake {
namespace store {
namespace bench {

// KVEngine defines the interface for key-value storage engines.
// This is used for benchmarking different KV storage systems.
class KVEngine {
   public:
    virtual ~KVEngine() = default;

    // Initialize the KV engine with the given configuration.
    virtual bool Init() = 0;

    // Put a key-value pair into the storage.
    virtual bool Put(const std::string& key, const std::string& value) = 0;

    // Get the value associated with the given key.
    // Returns true if the key exists, false otherwise.
    virtual bool Get(const std::string& key, std::string* value) = 0;

    // Close the KV engine and release resources.
    virtual void Close() = 0;
};

}  // namespace bench
}  // namespace store
}  // namespace mooncake

#endif  // MOONCAKE_STORE_BENCH_KV_ENGINE_H_