#pragma once

#include <cstdint>

namespace NoPartitioning {
struct Configuration {
    const int32_t MIN_BATCH_SIZE;
    const double HASH_TABLE_SIZE_RATIO;
    const int32_t HASH_TABLE_SIZE_LIMIT;
};
}  // namespace NoPartitioning
