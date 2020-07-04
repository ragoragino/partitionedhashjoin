#pragma once

#include <cstdint>

namespace RadixClustering {
struct Configuration {
    int32_t MIN_BATCH_SIZE;
    double HASH_TABLE_SIZE_RATIO;
    int32_t HASH_TABLE_SIZE_LIMIT;
    uint32_t L2_CACHE_SIZE;
};
}  // namespace RadixClustering
