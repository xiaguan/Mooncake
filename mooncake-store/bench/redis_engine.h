#ifndef MOONCAKE_STORE_BENCH_REDIS_ENGINE_H_
#define MOONCAKE_STORE_BENCH_REDIS_ENGINE_H_

#include <hiredis/hiredis.h>

#include <string>

#include "kv_engine.h"

namespace mooncake {
namespace store {
namespace bench {

// RedisEngine implements the KVEngine interface using Redis as the backend.
class RedisEngine : public KVEngine {
   public:
    // Constructor with Redis connection parameters.
    RedisEngine(const std::string& host, int port,
                const std::string& password = "");

    // Destructor.
    ~RedisEngine() override;

    // Initialize the Redis connection.
    bool Init() override;

    // Put a key-value pair into Redis.
    bool Put(const std::string& key, const std::string& value) override;

    // Get a value from Redis by key.
    bool Get(const std::string& key, std::string* value) override;

    // Close the Redis connection.
    void Close() override;

   private:
    // Redis connection parameters.
    std::string host_;
    int port_;
    std::string password_;

    // Redis connection context.
    redisContext* redis_context_;

    // Check if Redis connection is valid.
    bool IsConnected() const;

    // Execute a Redis command and check for errors.
    redisReply* ExecuteCommand(const char* format, ...);
};

}  // namespace bench
}  // namespace store
}  // namespace mooncake

#endif  // MOONCAKE_STORE_BENCH_REDIS_ENGINE_H_