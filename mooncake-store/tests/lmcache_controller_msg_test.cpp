#include "lmcache_controller_msg.h"

#include <gtest/gtest.h>

#include <iguana/json_writer.hpp>
#include <ylt/reflection/user_reflect_macro.hpp>

namespace mooncake::test {

class LMCacheControllerMsgTest : public ::testing::Test {
   protected:
    void SetUp() override {}
    void TearDown() override {}
};

struct Person {
    std::string name;
    int age{0};
    std::string msg_type = "Person";
};
YLT_REFL(Person, msg_type, name, age);

TEST_F(LMCacheControllerMsgTest, TestPersonSerialization) {
    // Create a Person instance with test data
    Person person;
    person.name = "Alice";
    person.age = 30;

    // Serialize the person
    std::string json_str;
    struct_json::to_json(person, json_str);

    // Compare with expected JSON string
    const char* expected = R"({"msg_type":"Person","name":"Alice","age":30})";
    EXPECT_EQ(json_str, expected);
}

TEST_F(LMCacheControllerMsgTest, TestKVAdmitMsgSerialization) {
    // Create a KVAdmitMsg instance with test data
    KVAdmitMsg msg;
    msg.instance_id = "lmcache-prod-cluster-01";
    msg.worker_id = 3;
    msg.key = "model_xxx:chunk_id_12345";
    msg.location = "mooncake_cpu";

    // Serialize the message
    std::string json_str;
    struct_json::to_json(msg, json_str);
    // Compare with expected JSON string
    const char* expected =
        R"({"type":"KVAdmitMsg","instance_id":"lmcache-prod-cluster-01","worker_id":3,"key":"model_xxx:chunk_id_12345","location":"mooncake_cpu"})";
    EXPECT_EQ(json_str, expected);
}

TEST_F(LMCacheControllerMsgTest, TestKVEvictMsgSerialization) {
    // Create a KVEvictMsg instance with test data
    KVEvictMsg msg;
    msg.instance_id = "lmcache-prod-cluster-02";
    msg.worker_id = 0;
    msg.key = "model_xxx:chunk_id_654321";
    msg.location = "mooncake_disk";

    // Serialize the message
    std::string json_str;
    struct_json::to_json(msg, json_str);

    // Compare with expected JSON string
    const char* expected =
        R"({"type":"KVEvictMsg","instance_id":"lmcache-prod-cluster-02","worker_id":0,"key":"model_xxx:chunk_id_654321","location":"mooncake_disk"})";
    EXPECT_EQ(json_str, expected);
}

}  // namespace mooncake::test
