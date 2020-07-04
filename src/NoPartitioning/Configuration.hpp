#pragma once

#include <cstdint>

namespace NoPartitioning {
struct Configuration {
    int32_t MIN_BATCH_SIZE;
    double HASH_TABLE_SIZE_RATIO;
};
}  // namespace NoPartitioning
