#pragma once

#include <cstdint>

namespace RadixClustering {
struct Configuration {
    size_t MinBatchSize = 10000;
    size_t NumberOfPartitions = 32;
};
}  // namespace RadixClustering