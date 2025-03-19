add_executable(worker 
    worker.cpp
    benchmark.cpp
    redis_engine.cpp
    redis_message_queue.cpp
    mooncake_engine.cpp
)

target_link_libraries(worker PUBLIC 
    cache_allocator 
    cachelib_memory_allocator 
    glog 
    gtest 
    grpc 
    grpc++ 
    protobuf 
    gtest_main 
    pthread
    hiredis
    gflags
)
