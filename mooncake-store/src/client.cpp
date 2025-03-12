#include "client.h"

#include <glog/logging.h>

#include <cassert>
#include <chrono>
#include <cstdint>

#include "transfer_engine.h"
#include "transport/transport.h"
#include "types.h"

// Macro for RPC calls with error handling and timing
#define RPC_CALL_OR_RETURN(stub, method, request, response)                  \
    [&]() -> ErrorCode {                                                     \
        const auto _start_ts = std::chrono::steady_clock::now();             \
        grpc::ClientContext _context;                                        \
        grpc::Status _status =                                               \
            (stub)->method(&_context, (request), &(response));               \
        const auto _duration =                                               \
            std::chrono::duration_cast<std::chrono::microseconds>(           \
                std::chrono::steady_clock::now() - _start_ts);               \
                                                                             \
        VLOG(1) << #method ": rpc_request=" << (request).ShortDebugString(); \
                                                                             \
        if (!_status.ok()) {                                                 \
            LOG(ERROR) << #method ": rpc_error [" << _status.error_code()    \
                       << "] " << _status.error_message()                    \
                       << " duration=" << _duration.count() << "us";         \
            return ErrorCode::RPC_FAIL;                                      \
        }                                                                    \
                                                                             \
        VLOG(1) << #method ": status_code=" << (response).status_code()      \
                << " duration=" << _duration.count() << "us";                \
                                                                             \
        return fromInt((response).status_code());                            \
    }()

#define RETURN_ON_ERROR(expr, msg)                               \
    do {                                                         \
        ErrorCode __err__ = (expr);                              \
        if (__err__ != ErrorCode::OK) {                          \
            LOG(ERROR) << msg << " error_code=" << int(__err__); \
            return __err__;                                      \
        }                                                        \
    } while (0)

namespace mooncake {

[[nodiscard]] size_t CalculateSliceSize(const std::vector<Slice>& slices) {
    size_t slice_size = 0;
    for (const auto& slice : slices) {
        slice_size += slice.size;
    }
    return slice_size;
}

Client::Client() : transfer_engine_(nullptr), master_stub_(nullptr) {}

Client::~Client() = default;

ErrorCode Client::ConnectToMaster(const std::string& master_addr) {
    auto channel =
        grpc::CreateChannel(master_addr, grpc::InsecureChannelCredentials());
    master_stub_ = mooncake_store::MasterService::NewStub(channel);
    if (!master_stub_) {
        LOG(ERROR) << "Failed to create master service stub";
        return ErrorCode::INTERNAL_ERROR;
    }
    return ErrorCode::OK;
}

ErrorCode Client::InitTransferEngine(const std::string& local_hostname,
                                     const std::string& metadata_connstring,
                                     const std::string& protocol,
                                     void** protocol_args) {
    // Create transfer engine
    transfer_engine_ = std::make_unique<TransferEngine>();
    if (!transfer_engine_) {
        LOG(ERROR) << "Failed to create transfer engine";
        return ErrorCode::INTERNAL_ERROR;
    }

    auto [hostname, port] = parseHostNameWithPort(local_hostname);
    int rc = transfer_engine_->init(metadata_connstring, local_hostname,
                                    hostname, port);
    if (rc != 0) {
        LOG(ERROR) << "Failed to initialize transfer engine";
        return ErrorCode::INTERNAL_ERROR;
    }
    Transport* transport = nullptr;
    if (protocol == "rdma") {
        LOG(INFO) << "transport_type=rdma";
        transport = transfer_engine_->installTransport("rdma", protocol_args);
        if (!transport) {
            LOG(ERROR) << "Failed to install rdma transport";
            return ErrorCode::INTERNAL_ERROR;
        }
    } else if (protocol == "tcp") {
        LOG(INFO) << "transport_type=tcp";
        try {
            transport =
                transfer_engine_->installTransport("tcp", protocol_args);
        } catch (std::exception& e) {
            LOG(ERROR) << "tcp_transport_install_failed error_message=\""
                       << e.what() << "\"";
            return ErrorCode::INTERNAL_ERROR;
        }
    } else {
        LOG(ERROR) << "unsupported_protocol protocol=" << protocol;
        return ErrorCode::INVALID_PARAMS;
    }
    CHECK(transport) << "Failed to install transport";

    return ErrorCode::OK;
}

ErrorCode Client::Init(const std::string& local_hostname,
                       const std::string& metadata_connstring,
                       const std::string& protocol, void** protocol_args,
                       const std::string& master_addr) {
    if (transfer_engine_) return ErrorCode::INTERNAL_ERROR;

    // Store configuration
    local_hostname_ = local_hostname;
    metadata_connstring_ = metadata_connstring;

    // Connect to master service
    RETURN_ON_ERROR(ConnectToMaster(master_addr),
                    "Failed to connect to Master");

    // Initialize transfer engine
    RETURN_ON_ERROR(InitTransferEngine(local_hostname, metadata_connstring,
                                       protocol, protocol_args),
                    "Failed to initialize transfer engine");

    return ErrorCode::OK;
}

ErrorCode Client::UnInit() {
    // Unmount all Segment
    auto mounted_segments = mounted_segments_;
    for (auto& entry : mounted_segments) {
        RETURN_ON_ERROR(UnmountSegment(entry.first, entry.second),
                        "Failed to unmount segment");
    }
    transfer_engine_.reset();
    return ErrorCode::OK;
}

ErrorCode Client::Get(const std::string& object_key,
                      std::vector<Slice>& slices) {
    ObjectInfo object_info;
    RETURN_ON_ERROR(Query(object_key, object_info), "Failed to query object");
    return Get(object_key, object_info, slices);
}

ErrorCode Client::Query(const std::string& object_key,
                        ObjectInfo& object_info) const {
    // Get replica list from master
    mooncake_store::GetReplicaListRequest request;
    request.set_key(object_key);

    RETURN_ON_ERROR(
        RPC_CALL_OR_RETURN(master_stub_, GetReplicaList, request, object_info),
        "Failed to get replica list");

    if (object_info.replica_list().empty()) {
        LOG(ERROR) << "Internal error: object_info.replica_list().empty()";
        return ErrorCode::OBJECT_NOT_FOUND;
    }

    return ErrorCode::OK;
}

ErrorCode Client::Get(const std::string& object_key,
                      const ObjectInfo& object_info,
                      std::vector<Slice>& slices) {
    // Get the first complete replica
    for (int i = 0; i < object_info.replica_list_size(); ++i) {
        if (object_info.replica_list(i).status() ==
            mooncake_store::ReplicaInfo::COMPLETE) {
            const auto& replica = object_info.replica_list(i);

            std::vector<mooncake_store::BufHandle> handles;
            for (const auto& handle : replica.handles()) {
                VLOG(1) << "handle: segment_name=" << handle.segment_name()
                        << " buffer=" << handle.buffer()
                        << " size=" << handle.size();
                if (handle.status() != mooncake_store::BufHandle::COMPLETE) {
                    LOG(ERROR) << "incomplete_handle_found segment_name="
                               << handle.segment_name();
                    return ErrorCode::INVALID_PARAMS;
                }
                handles.push_back(handle);
            }

            RETURN_ON_ERROR(TransferRead(handles, slices),
                            "Failed to transfer read");
            return ErrorCode::OK;
        }
    }

    LOG(ERROR) << "Internal error: no complete replicas found";
    return ErrorCode::INVALID_REPLICA;
}

ErrorCode Client::Put(const ObjectKey& key, std::vector<Slice>& slices,
                      const ReplicateConfig& config) {
    // Start put operation
    mooncake_store::PutStartRequest start_request;
    start_request.set_key(key);

    size_t slice_size = 0;
    for (size_t i = 0; i < slices.size(); ++i) {
        start_request.add_slice_lengths(slices[i].size);
        slice_size += slices[i].size;
    }
    start_request.set_value_length(slice_size);

    auto* replica_config = start_request.mutable_config();
    replica_config->set_replica_num(config.replica_num);

    mooncake_store::PutStartResponse start_response;

    ErrorCode err = RPC_CALL_OR_RETURN(master_stub_, PutStart, start_request,
                                       start_response);
    if (err != ErrorCode::OK) {
        return (err == ErrorCode::OBJECT_ALREADY_EXISTS) ? ErrorCode::OK : err;
    }

    // Transfer data using allocated handles from all replicas
    for (const auto& replica : start_response.replica_list()) {
        std::vector<mooncake_store::BufHandle> handles;
        for (const auto& handle : replica.handles()) {
            handles.push_back(handle);
        }
        // Write just ignore the transfer size
        ErrorCode transfer_err = TransferWrite(handles, slices);
        if (transfer_err != ErrorCode::OK) {
            // Revoke put operation
            mooncake_store::PutRevokeRequest revoke_request;
            revoke_request.set_key(key);
            mooncake_store::PutRevokeResponse revoke_response;
            RETURN_ON_ERROR(RPC_CALL_OR_RETURN(master_stub_, PutRevoke,
                                               revoke_request, revoke_response),
                            "Failed to revoke put");
            return transfer_err;
        }
    }

    // End put operation
    mooncake_store::PutEndRequest end_request;
    end_request.set_key(key);

    mooncake_store::PutEndResponse end_response;
    RETURN_ON_ERROR(
        RPC_CALL_OR_RETURN(master_stub_, PutEnd, end_request, end_response),
        "Failed to end put");

    return ErrorCode::OK;
}

ErrorCode Client::Remove(const ObjectKey& key) const {
    mooncake_store::RemoveRequest request;
    request.set_key(key);

    mooncake_store::RemoveResponse response;
    RETURN_ON_ERROR(RPC_CALL_OR_RETURN(master_stub_, Remove, request, response),
                    "Failed to remove");
    return ErrorCode::OK;
}

ErrorCode Client::MountSegment(const std::string& segment_name,
                               const void* buffer, size_t size) {
    mooncake_store::MountSegmentRequest request;
    request.set_segment_name(segment_name);
    request.set_buffer(reinterpret_cast<uint64_t>(buffer));
    request.set_size(size);
    mooncake_store::MountSegmentResponse response;

    int rc = transfer_engine_->registerLocalMemory((void*)buffer, size, "cpu:0",
                                                   true, true);
    if (rc != 0) {
        LOG(ERROR) << "register_local_memory_failed segment_name="
                   << segment_name;
        return ErrorCode::INVALID_PARAMS;
    }

    RETURN_ON_ERROR(
        RPC_CALL_OR_RETURN(master_stub_, MountSegment, request, response),
        "Failed to mount segment");
    mounted_segments_[segment_name] = (void*)buffer;
    return ErrorCode::OK;
}

ErrorCode Client::UnmountSegment(const std::string& segment_name, void* addr) {
    mooncake_store::UnmountSegmentRequest request;
    request.set_segment_name(segment_name);
    mooncake_store::UnmountSegmentResponse response;

    RETURN_ON_ERROR(
        RPC_CALL_OR_RETURN(master_stub_, UnmountSegment, request, response),
        "Failed to unmount segment");
    int rc = transfer_engine_->unregisterLocalMemory(addr);
    if (rc != 0) {
        LOG(ERROR) << "Failed to unregister transfer buffer with transfer "
                      "engine ret is "
                   << rc;
        return ErrorCode::INVALID_PARAMS;
    }
    mounted_segments_.erase(segment_name);
    return ErrorCode::OK;
}

ErrorCode Client::RegisterLocalMemory(void* addr, size_t length,
                                      const std::string& location,
                                      bool remote_accessible,
                                      bool update_metadata) {
    if (this->transfer_engine_->registerLocalMemory(
            addr, length, location, remote_accessible, update_metadata) != 0) {
        return ErrorCode::INVALID_PARAMS;
    }
    return ErrorCode::OK;
}

ErrorCode Client::unregisterLocalMemory(void* addr, bool update_metadata) {
    if (this->transfer_engine_->unregisterLocalMemory(addr, update_metadata) !=
        0) {
        return ErrorCode::INVALID_PARAMS;
    }
    return ErrorCode::OK;
}

ErrorCode Client::IsExist(const std::string& key) const {
    ObjectInfo object_info;
    return Query(key, object_info);
}

ErrorCode Client::TransferData(
    const std::vector<mooncake_store::BufHandle>& handles,
    std::vector<Slice>& slices, TransferRequest::OpCode op_code) const {
    std::vector<TransferRequest> transfer_tasks;

    if (handles.size() > slices.size()) {
        LOG(ERROR) << "invalid_partition_count handles_size=" << handles.size()
                   << " slices_size=" << slices.size();
        return ErrorCode::TRANSFER_FAIL;
    }

    for (uint64_t idx = 0; idx < handles.size(); ++idx) {
        auto& handle = handles[idx];
        auto& slice = slices[idx];
        if (handle.size() > slice.size) {
            LOG(ERROR)
                << "Size of replica partition more than provided buffers";
            return ErrorCode::TRANSFER_FAIL;
        }
        Transport::SegmentHandle seg =
            transfer_engine_->openSegment(handle.segment_name().c_str());
        if (seg == (uint64_t)ERR_INVALID_ARGUMENT) {
            LOG(ERROR) << "Failed to open segment " << handle.segment_name();
            return ErrorCode::TRANSFER_FAIL;
        }
        TransferRequest request;
        request.opcode = op_code;
        request.source = static_cast<char*>(slice.ptr);
        request.target_id = seg;
        request.target_offset = handle.buffer();
        request.length = handle.size();
        transfer_tasks.push_back(request);
    }

    const size_t batch_size = transfer_tasks.size();
    BatchID batch_id = transfer_engine_->allocateBatchID(batch_size);
    if (batch_id == Transport::INVALID_BATCH_ID) {
        LOG(ERROR) << "Failed to allocate batch ID";
        return ErrorCode::TRANSFER_FAIL;
    }

    int error_code = transfer_engine_->submitTransfer(batch_id, transfer_tasks);
    if (error_code != 0) {
        LOG(ERROR) << "Failed to submit all transfers, error code is "
                   << error_code;
        transfer_engine_->freeBatchID(batch_id);
        return ErrorCode::TRANSFER_FAIL;
    }

    bool has_err = false;
    bool all_ready = true;
    uint32_t try_num = 0;
    const uint32_t max_try_num = 3;
    int64_t start_ts = getCurrentTimeInNano();
    const static int64_t kOneSecondInNano = 1000 * 1000 * 1000;

    while (try_num < max_try_num) {
        has_err = false;
        all_ready = true;
        if (getCurrentTimeInNano() - start_ts > 60 * kOneSecondInNano) {
            LOG(ERROR) << "Failed to complete transfers after 60 seconds";
            return ErrorCode::TRANSFER_FAIL;
        }
        for (size_t i = 0; i < batch_size; ++i) {
            TransferStatus status;
            error_code =
                transfer_engine_->getTransferStatus(batch_id, i, status);
            if (error_code != 0) {
                LOG(ERROR) << "Transfer " << i
                           << " error, error_code=" << error_code;
                transfer_engine_->freeBatchID(batch_id);
                return ErrorCode::TRANSFER_FAIL;
            }
            if (status.s != TransferStatusEnum::COMPLETED) all_ready = false;
            if (status.s == TransferStatusEnum::FAILED) {
                LOG(ERROR) << "Transfer failed for task" << i;
                has_err = true;
            }
        }

        if (has_err) {
            LOG(WARNING) << "Transfer incomplete, retrying... (attempt "
                         << try_num + 1 << "/" << max_try_num << ")";
            ++try_num;
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }

        if (all_ready) break;
    }

    if (!all_ready) {
        LOG(ERROR) << "transfer_incomplete max_attempts=" << max_try_num;
        return ErrorCode::TRANSFER_FAIL;
    }

    transfer_engine_->freeBatchID(batch_id);

    return ErrorCode::OK;
}

ErrorCode Client::TransferWrite(
    const std::vector<mooncake_store::BufHandle>& handles,
    std::vector<Slice>& slices) const {
    return TransferData(handles, slices, TransferRequest::WRITE);
}

ErrorCode Client::TransferRead(
    const std::vector<mooncake_store::BufHandle>& handles,
    std::vector<Slice>& slices) const {
    size_t total_size = 0;
    for (const auto& handle : handles) {
        total_size += handle.size();
    }

    size_t slices_size = CalculateSliceSize(slices);
    if (slices_size < total_size) {
        LOG(ERROR) << "Slice size " << slices_size << " is smaller than total "
                   << "size " << total_size;
        return ErrorCode::INVALID_PARAMS;
    }

    return TransferData(handles, slices, TransferRequest::READ);
}

}  // namespace mooncake
