#include "HashJoin.hpp"

#include "Common/ThreadPool.hpp"

namespace RadixClustering {
namespace internal {
PrefixSumTable::PrefixSumTable(size_t numberOfPartitions, size_t numberOfWorkers)
    : m_table(numberOfPartitions * numberOfWorkers), m_numberOfPartitions(numberOfPartitions) {}

void PartitionsInfo::ComputePartitionsBoundaries(std::vector<size_t> partitionSizes) {
    m_partitionBorders.reserve(partitionSizes.size());

    for (size_t i = 0; i != partitionSizes.size(); i++) {
        size_t lastPartitionEnd = i == 0 ? 0 : m_partitionBorders.back().second;
        m_partitionBorders.emplace_back(lastPartitionEnd, lastPartitionEnd + partitionSizes[i]);
    }
}

}  // namespace internal
}  // namespace RadixClustering
