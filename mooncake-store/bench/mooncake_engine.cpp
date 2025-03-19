#include "mooncake_engine.h"

#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstddef>
#include <cstdlib>  // for atexit
#include <random>
#include <string>

#include "types.h"

using namespace mooncake;
using namespace mooncake::store::bench;

static bool isPortAvailable(int port) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) return false;

    int opt = 1;
    setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);

    bool available = (bind(sock, (struct sockaddr *)&addr, sizeof(addr)) == 0);
    close(sock);
    return available;
}

// Get a random available port between min_port and max_port
static int getRandomAvailablePort(int min_port = 12300, int max_port = 14300) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(min_port, max_port);

    for (int attempts = 0; attempts < 10; attempts++) {
        int port = dis(gen);
        std::cout << "port is " << port << std::endl;
        if (isPortAvailable(port)) {
            return port;
        }
    }
    LOG(ERROR) << "Failed to find available port";
    return -1;  // Failed to find available port
}

mooncake::Client *MooncakeEngine::getSharedClient() {
    static std::mutex mutex;
    static mooncake::Client *shared_client = nullptr;
    std::lock_guard<std::mutex> lock(mutex);
    // get a random available port
    int port = getRandomAvailablePort();
    std::string local_hostname = "localhost:" + std::to_string(port);
    if (!shared_client) {
        shared_client = new mooncake::Client();
        void **args = rdma_args("ibp51s0");
        ErrorCode rc = shared_client->Init(local_hostname, "localhost:2379",
                                           "rdma", args, "127.0.0.1:50051");
        if (rc != ErrorCode::OK) {
            LOG(ERROR) << "Failed to initialize shared client: "
                       << toString(rc);
            exit(1);
        }

        // 分配8gb的segment，然后挂载，重复10次
        for (int i = 0; i < 8; i++) {
            constexpr size_t kSegmentSize = (size_t)4 * 1024 * 1024 * 1024;
            void *ptr = allocate_buffer_allocator_memory(kSegmentSize);
            if (!ptr) {
                LOG(ERROR) << "Failed to allocate segment memory";
                exit(1);
            }
            rc = shared_client->MountSegment(local_hostname, ptr, kSegmentSize);
            if (rc != ErrorCode::OK) {
                LOG(ERROR) << "Failed to mount segment: " << toString(rc);
                exit(1);
            }
        }
    }
    return shared_client;
}

// ResourceTracker implementation using singleton pattern
ResourceTracker &ResourceTracker::getInstance() {
    static ResourceTracker instance;
    return instance;
}

ResourceTracker::ResourceTracker() {
    // Set up signal handlers
    struct sigaction sa;
    sa.sa_handler = signalHandler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;

    // Register for common termination signals
    sigaction(SIGINT, &sa, nullptr);   // Ctrl+C
    sigaction(SIGTERM, &sa, nullptr);  // kill command
    sigaction(SIGHUP, &sa, nullptr);   // Terminal closed

    // Register exit handler
    std::atexit(exitHandler);
}

ResourceTracker::~ResourceTracker() {
    // Cleanup is handled by exitHandler
}

void ResourceTracker::registerInstance(MooncakeEngine *instance) {
    std::lock_guard<std::mutex> lock(mutex_);
    instances_.insert(instance);
}

void ResourceTracker::unregisterInstance(MooncakeEngine *instance) {
    std::lock_guard<std::mutex> lock(mutex_);
    instances_.erase(instance);
}

void ResourceTracker::cleanupAllResources() {
    std::lock_guard<std::mutex> lock(mutex_);

    // Perform cleanup outside the lock to avoid potential deadlocks
    for (void *instance : instances_) {
        MooncakeEngine *store = static_cast<MooncakeEngine *>(instance);
        if (store) {
            LOG(INFO) << "Cleaning up DistributedObjectStore instance";
            store->tearDownAll();
        }
    }
}

void ResourceTracker::signalHandler(int signal) {
    LOG(INFO) << "Received signal " << signal << ", cleaning up resources";
    getInstance().cleanupAllResources();

    // Re-raise the signal with default handler to allow normal termination
    struct sigaction sa;
    sa.sa_handler = SIG_DFL;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(signal, &sa, nullptr);
    raise(signal);
}

void ResourceTracker::exitHandler() {
    LOG(INFO) << "Process exiting, cleaning up resources";
    getInstance().cleanupAllResources();
}

MooncakeEngine::MooncakeEngine() {
    // Register this instance with the global tracker
    ResourceTracker::getInstance().registerInstance(this);
}

MooncakeEngine::~MooncakeEngine() {
    // Unregister from the tracker before cleanup
    ResourceTracker::getInstance().unregisterInstance(this);
    // Let it leak
}

int MooncakeEngine::allocateSlices(std::vector<Slice> &slices,
                                   const std::string &value) {
    uint64_t offset = 0;
    while (offset < value.size()) {
        auto chunk_size = std::min(value.size() - offset, kMaxSliceSize);
        auto ptr = client_buffer_allocator_->allocate(chunk_size);
        if (!ptr) {
            // Deallocate any previously allocated slices
            for (auto &slice : slices) {
                client_buffer_allocator_->deallocate(slice.ptr, slice.size);
            }
            slices.clear();
            return 1;
        }

        memcpy(ptr, value.data() + offset, chunk_size);

        slices.emplace_back(Slice{ptr, chunk_size});
        offset += chunk_size;
    }
    return 0;
}

int MooncakeEngine::allocateSlices(
    std::vector<mooncake::Slice> &slices,
    const mooncake::Client::ObjectInfo &object_info, uint64_t &length) {
    length = 0;
    if (!object_info.replica_list_size()) return -1;
    auto &replica = object_info.replica_list(0);
    for (auto &handle : replica.handles()) {
        auto chunk_size = handle.size();
        assert(chunk_size <= kMaxSliceSize);
        auto ptr = client_buffer_allocator_->allocate(chunk_size);
        if (!ptr) return 1;
        slices.emplace_back(Slice{ptr, chunk_size});
        length += chunk_size;
    }
    return 0;
}

std::string MooncakeEngine::exportSlices(
    const std::vector<mooncake::Slice> &slices, uint64_t length) {
    std::string result;
    result.reserve(length);
    for (auto slice : slices) {
        result.append(static_cast<char *>(slice.ptr), slice.size);
    }
    return result;
}

int MooncakeEngine::freeSlices(const std::vector<mooncake::Slice> &slices) {
    for (auto slice : slices) {
        client_buffer_allocator_->deallocate(slice.ptr, slice.size);
    }
    return 0;
}

int MooncakeEngine::tearDownAll() {
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return 1;
    }
    client_->log_transfer_time();
    ErrorCode rc = client_->UnInit();
    if (rc != ErrorCode::OK) {
        LOG(ERROR) << "Failed to unmount segment: " << toString(rc);
        return 1;
    }
    client_ = nullptr;
    client_buffer_allocator_.reset();
    return 0;
}

int MooncakeEngine::put(const std::string &key, const std::string &value) {
    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return 1;
    }
    ReplicateConfig config;
    config.replica_num = 1;

    std::vector<Slice> slices;
    int ret = allocateSlices(slices, value);
    if (ret) return ret;
    ErrorCode error_code = client_->Put(std::string(key), slices, config);
    freeSlices(slices);
    if (error_code != ErrorCode::OK) return 1;
    return 0;
}

// KVEngine interface implementation

bool MooncakeEngine::Init() {
    // Use default parameters for setup
    // Register local buffer allocator
    constexpr size_t kLocalBufferSize = 1024 * 1024 * 256;
    client_buffer_allocator_ =
        std::make_unique<SimpleAllocator>(kLocalBufferSize);
    // Register shared client
    auto shared_client = getSharedClient();
    client_ = shared_client;
    ErrorCode rc = shared_client->RegisterLocalMemory(
        client_buffer_allocator_->getBase(), kLocalBufferSize, "cpu:0", false,
        false);
    if (rc != ErrorCode::OK) {
        LOG(ERROR) << "Failed to register local memory: " << toString(rc);
        return false;
    }
    // Segment already mounted, skip mount
    return true;
}

bool MooncakeEngine::Put(const std::string &key, const std::string &value) {
    int result = put(key, value);
    return result == 0;
}
bool MooncakeEngine::Get(const std::string &key, std::string *value) {
    if (!value) {
        return false;
    }

    if (!client_) {
        LOG(ERROR) << "Client is not initialized";
        return false;
    }
    mooncake::Client::ObjectInfo object_info;
    std::vector<Slice> slices;

    const std::string kNullString = "";
    ErrorCode error_code = client_->Query(key, object_info);
    if (error_code != ErrorCode::OK) return false;

    uint64_t str_length = 0;
    int ret = allocateSlices(slices, object_info, str_length);
    if (ret) return false;

    error_code = client_->Get(key, object_info, slices);
    if (error_code != ErrorCode::OK) {
        freeSlices(slices);
        return false;
    }

    // Avoid redundant memcpy by directly writing to value
    value->clear();
    value->reserve(str_length);
    for (auto slice : slices) {
        value->append(static_cast<char *>(slice.ptr), slice.size);
    }
    freeSlices(slices);
    return true;
}

void MooncakeEngine::Close() { tearDownAll(); }
