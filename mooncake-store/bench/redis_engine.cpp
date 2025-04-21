#include "redis_engine.h"

#include <glog/logging.h>
#include <stdarg.h>
#include <string.h>

namespace mooncake {
namespace store {
namespace bench {

RedisEngine::RedisEngine(const std::string& host, int port,
                         const std::string& password)
    : host_(host), port_(port), password_(password), redis_context_(nullptr) {}

RedisEngine::~RedisEngine() { Close(); }

bool RedisEngine::Init() {
    // Connect to Redis server
    redis_context_ = redisConnect(host_.c_str(), port_);
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

    // Authenticate if password is provided
    if (!password_.empty()) {
        redisReply* reply = ExecuteCommand("AUTH %s", password_.c_str());
        if (reply == nullptr) {
            LOG(ERROR) << "Redis authentication failed";
            return false;
        }
        freeReplyObject(reply);
    }

    LOG(INFO) << "Connected to Redis server at " << host_ << ":" << port_;
    return true;
}

bool RedisEngine::Put(const std::string& key, const std::string& value) {
    if (!IsConnected()) {
        LOG(ERROR) << "Redis not connected";
        return false;
    }

    redisReply* reply =
        ExecuteCommand("SET %s %b", key.c_str(), value.c_str(), value.size());
    if (reply == nullptr) {
        return false;
    }

    bool success =
        (reply->type == REDIS_REPLY_STATUS && strcmp(reply->str, "OK") == 0);
    freeReplyObject(reply);
    return success;
}

bool RedisEngine::Get(const std::string& key, std::string* value) {
    if (!IsConnected() || value == nullptr) {
        LOG(ERROR) << "Redis not connected or value pointer is null";
        return false;
    }

    redisReply* reply = ExecuteCommand("GET %s", key.c_str());
    if (reply == nullptr) {
        return false;
    }

    bool success = false;
    if (reply->type == REDIS_REPLY_STRING) {
        value->assign(reply->str, reply->len);
        success = true;
    } else if (reply->type == REDIS_REPLY_NIL) {
        // Key does not exist
        success = false;
    } else {
        LOG(ERROR) << "Unexpected Redis reply type: " << reply->type;
        success = false;
    }

    freeReplyObject(reply);
    return success;
}

void RedisEngine::Close() {
    if (redis_context_) {
        redisFree(redis_context_);
        redis_context_ = nullptr;
        LOG(INFO) << "Disconnected from Redis server";
    }
}

bool RedisEngine::IsConnected() const {
    return redis_context_ != nullptr && !redis_context_->err;
}

redisReply* RedisEngine::ExecuteCommand(const char* format, ...) {
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

}  // namespace bench
}  // namespace store
}  // namespace mooncake