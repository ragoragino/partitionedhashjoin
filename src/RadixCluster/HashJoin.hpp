#include <memory>

#include "Common/IHasher.hpp"
#include "Common/IThreadPool.hpp"
#include "Common/Logger.hpp"
#include "Common/Table.hpp"
#include "Configuration.hpp"
#include "HashTables/SeparateChaining.hpp"

#ifndef HASH_TABLE_BUCKET_SIZE
#define HASH_TABLE_BUCKET_SIZE 3
#endif

namespace RadixClustering {
namespace internal {
class PartitionsInfo {
   public:
    void ComputePartitionsBoundaries(std::vector<size_t> partitionSizes);

    size_t GetPartitionBoundary(size_t partition);

   private:
    std::vector<size_t> m_partitionBorders;
};

struct PartitioningConfiguration {
    size_t NumberOfPartitions;
    size_t NumberOfWorkers;
    size_t BatchSize;
};

class PrefixSumTable {
   public:
    PrefixSumTable(size_t numberOfPartitions, size_t numberOfWorkers);

    size_t Get(size_t hashIndex, size_t workerIndex) const;

    void Set(size_t hashIndex, size_t workerIndex, size_t value);

    void Increment(size_t hashIndex, size_t workerIndex);

   private:
    std::vector<size_t> m_table;
    size_t m_numberOfWorkers;
};
}  // namespace internal

class HashJoiner {
   public:
    HashJoiner(Configuration configuration, std::shared_ptr<Common::IThreadPool> threadPool,
               std::shared_ptr<Common::IHasher> hasher);

    // tableA should be the build relation, while tableB should be probe relation
    std::shared_ptr<Common::Table<Common::JoinedTuple>> Run(
        std::shared_ptr<Common::Table<Common::Tuple>> tableA,
        std::shared_ptr<Common::Table<Common::Tuple>> tableB);

   private:
    std::future<Common::TasksErrorHolder> Partition(
        std::shared_ptr<Common::Table<Common::Tuple>> table,
        std::shared_ptr<Common::Table<Common::Tuple>> partitionedTable,
        internal::PartitionsInfo& partitionInfo,
        const internal::PartitioningConfiguration& partitionConfiguration);

    std::shared_ptr<HashTables::SeparateChainingHashTable<Common::Tuple, HASH_TABLE_BUCKET_SIZE>>
    Build(std::shared_ptr<Common::Table<Common::Tuple>> tableA, size_t numberOfWorkers);

    std::shared_ptr<Common::Table<Common::JoinedTuple>> Probe(
        std::shared_ptr<
            HashTables::SeparateChainingHashTable<Common::Tuple, HASH_TABLE_BUCKET_SIZE>>
            hashTable,
        std::shared_ptr<Common::Table<Common::Tuple>> tableB, size_t numberOfWorkers);

    std::future<Common::TasksErrorHolder> Join(
        std::shared_ptr<Common::Table<Common::JoinedTuple>> joinedTable,
        std::shared_ptr<Common::Table<Common::Tuple>> partitionedTableA,
        std::shared_ptr<Common::Table<Common::Tuple>> partitionedTableB,
        std::pair<internal::PartitionsInfo, internal::PartitionsInfo> partitionInfo,
        std::pair<internal::PartitioningConfiguration, internal::PartitioningConfiguration>
            partitionConfiguration);

    std::pair<internal::PartitioningConfiguration, internal::PartitioningConfiguration>
    GetPartitioningConfiguration(std::shared_ptr<Common::Table<Common::Tuple>> tableA,
                                 std::shared_ptr<Common::Table<Common::Tuple>> tableB);

    std::shared_ptr<Common::IHasher> m_hasher;
    std::shared_ptr<Common::IThreadPool> m_threadPool;
    Configuration m_configuration;
    Common::LoggerType m_logger;
};
}  // namespace RadixClustering
