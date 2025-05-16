#pragma once

#include <optional>
#include <string>
#include <vector>

#include "types.h"  // For Replica etc.

namespace mooncake {

struct ObjectMetadata {
    std::vector<Replica> replicas;
    uint64_t size;

    ObjectMetadata() : size(0) {}
    explicit ObjectMetadata(uint64_t val_size) : size(val_size) {}

    // Returns the segment name of the first buffer descriptor in the first
    // replica Returns std::nullopt if no valid segment name is available
    [[nodiscard]] std::optional<std::string> get_primary_segment_name() const {
        if (replicas.empty() ||
            replicas[0].get_descriptor().buffer_descriptors.empty()) {
            return std::nullopt;
        }
        return replicas[0].get_descriptor().buffer_descriptors[0].segment_name_;
    }
};

}  // namespace mooncake
