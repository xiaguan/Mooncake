#pragma once

#include <csignal>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_set>

#include "allocator.h"
#include "client.h"
#include "kv_engine.h"
#include "utils.h"

namespace mooncake {
namespace store {
namespace bench {

class MooncakeEngine;

// Global resource tracker to handle cleanup on abnormal termination
class ResourceTracker {
   public:
    // Get the singleton instance
    static ResourceTracker &getInstance();

    // Register a DistributedObjectStore instance for cleanup
    void registerInstance(MooncakeEngine *instance);

    // Unregister a DistributedObjectStore instance
    void unregisterInstance(MooncakeEngine *instance);

   private:
    ResourceTracker();
    ~ResourceTracker();

    // Prevent copying
    ResourceTracker(const ResourceTracker &) = delete;
    ResourceTracker &operator=(const ResourceTracker &) = delete;

    // Cleanup all registered resources
    void cleanupAllResources();

    // Signal handler function
    static void signalHandler(int signal);

    // Exit handler function
    static void exitHandler();

    std::mutex mutex_;
    std::unordered_set<MooncakeEngine *> instances_;
};

class MooncakeEngine : public KVEngine {
   public:
    MooncakeEngine();
    ~MooncakeEngine();

    int put(const std::string &key, const std::string &value);

    int tearDownAll();

    // KVEngine interface implementation
    bool Init() override;
    bool Put(const std::string &key, const std::string &value) override;
    bool Get(const std::string &key, std::string *value) override;

    void Close() override;

   private:
    static mooncake::Client *getSharedClient();

    int allocateSlices(std::vector<mooncake::Slice> &slices,
                       const std::string &value);

    int allocateSlices(std::vector<mooncake::Slice> &slices,
                       const mooncake::Client::ObjectInfo &object_info,
                       uint64_t &length);

    std::string exportSlices(const std::vector<mooncake::Slice> &slices,
                             uint64_t length);

    int freeSlices(const std::vector<mooncake::Slice> &slices);

   public:
    mooncake::Client *client_ = nullptr;
    std::unique_ptr<mooncake::SimpleAllocator> client_buffer_allocator_ =
        nullptr;
};

}  // namespace bench
}  // namespace store
}  // namespace mooncake
