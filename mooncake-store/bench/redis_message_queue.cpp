#include "redis_message_queue.h"

#include <glog/logging.h>
#include <stdarg.h>
#include <string.h>

namespace mooncake {
namespace store {
namespace bench {

RedisMessageQueue::RedisMessageQueue(const std::string& host, int port)
    : host_(host), port_(port), redis_context_(nullptr) {}

RedisMessageQueue::~RedisMessageQueue() { Close(); }

bool RedisMessageQueue::Init() {
    // Connect to Redis server
    struct timeval timeout = {1, 500000};  // 1.5 seconds timeout
    redis_context_ = redisConnectWithTimeout(host_.c_str(), port_, timeout);
    if (redis_context_ == nullptr || redis_context_->err) {
        if (redis_context_) {
            LOG(ERROR) << "Redis connection error: " << redis_context_->errstr;
            redisFree(redis_context_);
            redis_context_ = nullptr;
        } else {
            LOG(ERROR)
                << "Redis connection error: can't allocate redis context";
        }
        return false;
    }

    LOG(INFO) << "Connected to Redis message queue at " << host_ << ":"
              << port_;
    return true;
}

bool RedisMessageQueue::Push(const std::string& queue_name,
                             const std::string& message) {
    if (!IsConnected()) {
        if (!Reconnect()) {
            return false;
        }
    }

    redisReply* reply = ExecuteCommand("RPUSH %s %b", queue_name.c_str(),
                                       message.c_str(), message.size());
    if (reply == nullptr) {
        return false;
    }

    bool success = (reply->type == REDIS_REPLY_INTEGER && reply->integer > 0);
    freeReplyObject(reply);
    return success;
}

bool RedisMessageQueue::Pop(const std::string& queue_name, std::string* message,
                            int timeout_seconds) {
    if (!IsConnected()) {
        if (!Reconnect()) {
            return false;
        }
    }

    if (message == nullptr) {
        LOG(ERROR) << "Message pointer is null";
        return false;
    }

    redisReply* reply = nullptr;
    if (timeout_seconds > 0) {
        // Use blocking pop with timeout
        reply =
            ExecuteCommand("BLPOP %s %d", queue_name.c_str(), timeout_seconds);
    } else {
        // Use non-blocking pop
        reply = ExecuteCommand("LPOP %s", queue_name.c_str());
    }

    if (reply == nullptr) {
        return false;
    }

    bool success = false;
    if (timeout_seconds > 0 && reply->type == REDIS_REPLY_ARRAY) {
        // BLPOP returns an array with [queue_name, value]
        if (reply->elements == 2 &&
            reply->element[1]->type == REDIS_REPLY_STRING) {
            message->assign(reply->element[1]->str, reply->element[1]->len);
            success = true;
        }
    } else if (reply->type == REDIS_REPLY_STRING) {
        // LPOP returns the value directly
        message->assign(reply->str, reply->len);
        success = true;
    } else if (reply->type == REDIS_REPLY_NIL) {
        // Queue is empty
        success = false;
    } else {
        LOG(ERROR) << "Unexpected Redis reply type: " << reply->type;
        success = false;
    }

    freeReplyObject(reply);
    return success;
}

int RedisMessageQueue::Length(const std::string& queue_name) {
    if (!IsConnected()) {
        if (!Reconnect()) {
            return -1;
        }
    }

    redisReply* reply = ExecuteCommand("LLEN %s", queue_name.c_str());
    if (reply == nullptr) {
        return -1;
    }

    int length = -1;
    if (reply->type == REDIS_REPLY_INTEGER) {
        length = static_cast<int>(reply->integer);
    } else {
        LOG(ERROR) << "Unexpected Redis reply type for LLEN: " << reply->type;
    }

    freeReplyObject(reply);
    return length;
}

bool RedisMessageQueue::Clear(const std::string& queue_name) {
    if (!IsConnected()) {
        if (!Reconnect()) {
            return false;
        }
    }

    redisReply* reply = ExecuteCommand("DEL %s", queue_name.c_str());
    if (reply == nullptr) {
        return false;
    }

    bool success = (reply->type == REDIS_REPLY_INTEGER);
    freeReplyObject(reply);
    return success;
}

void RedisMessageQueue::Close() {
    if (redis_context_) {
        redisFree(redis_context_);
        redis_context_ = nullptr;
        LOG(INFO) << "Disconnected from Redis message queue";
    }
}

bool RedisMessageQueue::IsConnected() const {
    return redis_context_ != nullptr && !redis_context_->err;
}

redisReply* RedisMessageQueue::ExecuteCommand(const char* format, ...) {
    if (!IsConnected()) {
        LOG(ERROR) << "Redis not connected";
        return nullptr;
    }

    va_list ap;
    va_start(ap, format);
    redisReply* reply = (redisReply*)redisvCommand(redis_context_, format, ap);
    va_end(ap);

    if (reply == nullptr) {
        LOG(ERROR) << "Redis command failed: " << redis_context_->errstr;
        return nullptr;
    }

    if (reply->type == REDIS_REPLY_ERROR) {
        LOG(ERROR) << "Redis error: " << reply->str;
        freeReplyObject(reply);
        return nullptr;
    }

    return reply;
}

bool RedisMessageQueue::Reconnect() {
    LOG(INFO) << "Attempting to reconnect to Redis message queue";

    // Free the old context if it exists
    if (redis_context_) {
        redisFree(redis_context_);
        redis_context_ = nullptr;
    }

    // Try to reconnect
    return Init();
}

}  // namespace bench
}  // namespace store
}  // namespace mooncake
