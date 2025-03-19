#ifndef MOONCAKE_STORE_BENCH_REDIS_MESSAGE_QUEUE_H_
#define MOONCAKE_STORE_BENCH_REDIS_MESSAGE_QUEUE_H_

#include <hiredis/hiredis.h>

#include <string>

namespace mooncake {
namespace store {
namespace bench {

// RedisMessageQueue provides a message queue implementation using Redis.
// This class is designed for benchmarking purposes and uses port 6380
// to avoid affecting normal Redis operations.
class RedisMessageQueue {
   public:
    // Constructor with Redis connection parameters.
    // Default port is 6380 to avoid affecting normal benchmarking.
    RedisMessageQueue(const std::string& host = "localhost", int port = 6380);

    // Destructor.
    ~RedisMessageQueue();

    // Initialize the Redis connection.
    bool Init();

    // Push a message to the queue.
    // Uses RPUSH to add the message to the end of the queue.
    bool Push(const std::string& queue_name, const std::string& message);

    // Pop a message from the queue.
    // Uses LPOP to get a message from the beginning of the queue.
    // If timeout_seconds > 0, uses BLPOP for blocking operation.
    bool Pop(const std::string& queue_name, std::string* message,
             int timeout_seconds = 0);

    // Get the length of the queue.
    // Returns -1 on error.
    int Length(const std::string& queue_name);

    // Clear the queue by deleting it.
    // Returns true if successful, false otherwise.
    bool Clear(const std::string& queue_name);

    // Close the Redis connection.
    void Close();

    // Check if Redis connection is valid.
    bool IsConnected() const;

   private:
    // Redis connection parameters.
    std::string host_;
    int port_;

    // Redis connection context.
    redisContext* redis_context_;

    // Execute a Redis command and check for errors.
    redisReply* ExecuteCommand(const char* format, ...);

    // Reconnect to Redis if connection is lost.
    bool Reconnect();
};

}  // namespace bench
}  // namespace store
}  // namespace mooncake

#endif  // MOONCAKE_STORE_BENCH_REDIS_MESSAGE_QUEUE_H_
