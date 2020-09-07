#pragma once

#include <memory>
#include <optional>

#include "Common/IHasher.hpp"
#include "Common/IThreadPool.hpp"
#include "Common/Logger.hpp"
#include "Common/Table.hpp"
#include "Common/TestResults.hpp"
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

    std::pair<size_t, size_t> PartitionsInfo::GetPartitionBoundaries(size_t partition) const {
        return m_partitionBorders[partition];
    };

   private:
    std::vector<std::pair<size_t, size_t>> m_partitionBorders;
};

struct PartitioningConfiguration {
    size_t NumberOfPartitions;
    size_t NumberOfWorkers;
    size_t BatchSize;
};

class PrefixSumTable {
   public:
    PrefixSumTable(size_t numberOfPartitions, size_t numberOfWorkers);

    size_t PrefixSumTable::Get(size_t hashIndex, size_t workerIndex) const {
        return m_table[workerIndex * m_numberOfPartitions + hashIndex];
    }

    void PrefixSumTable::Set(size_t hashIndex, size_t workerIndex, size_t value) {
        m_table[workerIndex * m_numberOfPartitions + hashIndex] = value;
    }

    void PrefixSumTable::Increment(size_t hashIndex, size_t workerIndex) {
        m_table[workerIndex * m_numberOfPartitions + hashIndex]++;
    }

   private:
    std::vector<size_t> m_table;
    size_t m_numberOfPartitions;
};
}  // namespace internal

template <typename HashTableFactory, typename HasherType>
class HashJoiner {
    using HashTableType = typename HashTableFactory::HashTableType;

   public:
    HashJoiner(Configuration configuration, std::shared_ptr<Common::IThreadPool> threadPool,
               const HasherType& hasher, const HashTableFactory& hashTableFactory);

    // tableA should be the build relation, while tableB should be probe relation
    std::shared_ptr<Common::Table<Common::JoinedTuple>> Run(
        std::shared_ptr<Common::Table<Common::Tuple>> tableA,
        std::shared_ptr<Common::Table<Common::Tuple>> tableB, 
        std::shared_ptr<Common::IHashJoinTimer> timer =
            std::make_shared<Common::NoOpHashJoinTimer>());

   private:
    std::future<Common::TasksErrorHolder> Partition(
        std::shared_ptr<Common::Table<Common::Tuple>> table,
        std::shared_ptr<Common::Table<Common::Tuple>> partitionedTable,
        internal::PartitionsInfo& partitionInfo,
        const internal::PartitioningConfiguration& partitionConfiguration);

    std::shared_ptr<Common::Table<Common::JoinedTuple>> Probe(
        std::shared_ptr<HashTableType> hashTable,
        std::shared_ptr<Common::Table<Common::Tuple>> tableB, size_t numberOfWorkers);

    std::future<Common::TasksErrorHolder> Join(
        std::shared_ptr<Common::Table<Common::JoinedTuple>> joinedTable,
        std::shared_ptr<Common::Table<Common::Tuple>> partitionedTableA,
        std::shared_ptr<Common::Table<Common::Tuple>> partitionedTableB,
        const std::pair<internal::PartitionsInfo, internal::PartitionsInfo>& partitionInfo,
        const std::pair<internal::PartitioningConfiguration, internal::PartitioningConfiguration>&
            partitionConfiguration);

    std::pair<internal::PartitioningConfiguration, internal::PartitioningConfiguration>
    GetPartitioningConfiguration(std::shared_ptr<Common::Table<Common::Tuple>> tableA,
                                 std::shared_ptr<Common::Table<Common::Tuple>> tableB);

    std::shared_ptr<Common::IThreadPool> m_threadPool;
    Configuration m_configuration;
    Common::LoggerType m_logger;
    HasherType m_hasher;
    HashTableFactory m_hashTableFactory;
};

template <typename HashTableFactory, typename HasherType>
HashJoiner<HashTableFactory, HasherType>::HashJoiner(
    Configuration configuration, std::shared_ptr<Common::IThreadPool> threadPool,
    const HasherType& hasher, const HashTableFactory& hashTableFactory)
    : m_configuration(configuration),
      m_threadPool(threadPool),
      m_logger(Common::GetNewLogger()),
      m_hasher(hasher),
      m_hashTableFactory(hashTableFactory) {
    Common::AddComponentAttributeToLogger(m_logger, "NoPartitioning.HashJoiner");
}

template <typename HashTableFactory, typename HasherType>
std::pair<internal::PartitioningConfiguration, internal::PartitioningConfiguration>
HashJoiner<HashTableFactory, HasherType>::GetPartitioningConfiguration(
    std::shared_ptr<Common::Table<Common::Tuple>> tableA,
    std::shared_ptr<Common::Table<Common::Tuple>> tableB) {
    const size_t sizeA = tableA->GetSize();
    const size_t sizeB = tableB->GetSize();
    size_t numberOfWorkers = m_threadPool->GetNumberOfWorkers();
    size_t batchSizeA = static_cast<size_t>(sizeA / numberOfWorkers);
    size_t batchSizeB = static_cast<size_t>(sizeB / numberOfWorkers);

    if (batchSizeA < m_configuration.MinBatchSize) {
        numberOfWorkers = 1;
        batchSizeA = sizeA;
    }

    if (batchSizeB < m_configuration.MinBatchSize) {
        numberOfWorkers = 1;
        batchSizeB = sizeB;
    }

    internal::PartitioningConfiguration partitionConfigA{
        m_configuration.NumberOfPartitions, // NumberOfPartitions,
        numberOfWorkers,  // NumberOfWorkers;
        batchSizeA, // BatchSize
    };

    internal::PartitioningConfiguration partitionConfigB{
        m_configuration.NumberOfPartitions, // NumberOfPartitions,
        numberOfWorkers,  // NumberOfWorkers;
        batchSizeB, // BatchSize
    };

    return std::make_pair<internal::PartitioningConfiguration, internal::PartitioningConfiguration>(
        std::move(partitionConfigA), std::move(partitionConfigB));
}

template <typename HashTableFactory, typename HasherType>
std::shared_ptr<Common::Table<Common::JoinedTuple>> HashJoiner<HashTableFactory, HasherType>::Run(
    std::shared_ptr<Common::Table<Common::Tuple>> tableA,
    std::shared_ptr<Common::Table<Common::Tuple>> tableB,
    std::shared_ptr<Common::IHashJoinTimer> timer) {
    auto partitionedTableA =
        std::make_shared<Common::Table<Common::Tuple>>(tableA->GetSize(), Common::generate_uuid());
    auto partitionedTableB =
        std::make_shared<Common::Table<Common::Tuple>>(tableB->GetSize(), Common::generate_uuid());

    const std::pair<internal::PartitioningConfiguration, internal::PartitioningConfiguration>
        partitionConfiguration = this->GetPartitioningConfiguration(tableA, tableB);

    auto partitionInfo = std::make_pair<internal::PartitionsInfo, internal::PartitionsInfo>(
        internal::PartitionsInfo{}, internal::PartitionsInfo{});

    timer->SetBuildPhaseBegin();
 
    auto partitionedTableAFuture = this->Partition(tableA, partitionedTableA, partitionInfo.first,
                                                   partitionConfiguration.first);
    auto partitionedTableBFuture = this->Partition(tableB, partitionedTableB, partitionInfo.second,
                                                   partitionConfiguration.second);

    partitionedTableAFuture.wait();
    partitionedTableBFuture.wait();

    if (!partitionedTableAFuture.get().Empty()) {
        throw partitionedTableAFuture.get().Pop();
    } else if (!partitionedTableBFuture.get().Empty()) {
        throw partitionedTableBFuture.get().Pop();
    }

    timer->SetBuildPhaseEnd();
    timer->SetProbePhaseBegin();

    auto joinedTable =
        std::make_shared<Common::Table<Common::JoinedTuple>>(Common::generate_uuid());

    auto joinFuture = this->Join(joinedTable, partitionedTableA, partitionedTableB, partitionInfo,
                                 partitionConfiguration);

    joinFuture.wait();

    if (!joinFuture.get().Empty()) {
        throw joinFuture.get().Pop();
    }

    timer->SetProbePhaseEnd();

    return joinedTable;
}

template <typename HashTableFactory, typename HasherType>
std::future<Common::TasksErrorHolder> HashJoiner<HashTableFactory, HasherType>::Join(
    std::shared_ptr<Common::Table<Common::JoinedTuple>> joinedTable,
    std::shared_ptr<Common::Table<Common::Tuple>> partitionedTableA,
    std::shared_ptr<Common::Table<Common::Tuple>> partitionedTableB,
    const std::pair<internal::PartitionsInfo, internal::PartitionsInfo>& partitionInfo,
    const std::pair<internal::PartitioningConfiguration, internal::PartitioningConfiguration>&
        partitionConfiguration) {
    auto numberOfJoinedPartitions = std::make_shared<std::atomic<size_t>>(0);
    auto numberOfJoinedTuples = std::make_shared<std::atomic<size_t>>(0);
    auto join = [this, joinedTable, partitionedTableA, partitionedTableB, &partitionConfiguration,
                 &partitionInfo, numberOfJoinedPartitions, numberOfJoinedTuples](size_t id) {
        LOG(m_logger, Common::SeverityLevel::info)
            << "Partition " << id << " starting join process.";

        size_t joined = 0;
        for (size_t partitionId = id; partitionId < partitionConfiguration.first.NumberOfPartitions;
             partitionId += partitionConfiguration.first.NumberOfWorkers) {
            auto [partitionStartA, partitionEndA] =
                partitionInfo.first.GetPartitionBoundaries(partitionId);

            auto hashTable = m_hashTableFactory.New(partitionEndA - partitionStartA);

            // Build a hash table
            for (size_t i = partitionStartA; i != partitionEndA; i++) {
                const Common::Tuple& tuple = (*partitionedTableA)[i];
                hashTable->Insert(tuple.id, &tuple);
            }

            auto [partitionStartB, partitionEndB] =
                partitionInfo.second.GetPartitionBoundaries(partitionId);

            // Probe the hash table
            for (size_t i = partitionStartB; i != partitionEndB; i++) {
                const Common::Tuple& tuple = (*partitionedTableB)[i];
                const Common::Tuple* tableATuple = hashTable->Get(tuple.id);
                if (tableATuple != nullptr) {
                    joined++;
                }
            }

            numberOfJoinedPartitions->operator++();
        }

        numberOfJoinedTuples->fetch_add(joined);

        LOG(m_logger, Common::SeverityLevel::info)
            << "Partition " << id << " finished join process.";

        if (numberOfJoinedPartitions->load() == partitionConfiguration.first.NumberOfPartitions) {
            LOG(m_logger, Common::SeverityLevel::info)
                << "Joined  " << numberOfJoinedTuples->load() << " tuples";
        }
    };

    std::vector<std::function<void()>> joinTasks{};
    for (size_t i = 0; i != partitionConfiguration.first.NumberOfWorkers; ++i) {
        joinTasks.push_back(std::bind(join, i));
    }

    return m_threadPool->Push(std::move(joinTasks));
}

template <typename HashTableFactory, typename HasherType>
std::future<Common::TasksErrorHolder> HashJoiner<HashTableFactory, HasherType>::Partition(
    std::shared_ptr<Common::Table<Common::Tuple>> table,
    std::shared_ptr<Common::Table<Common::Tuple>> partitionedTable,
    internal::PartitionsInfo& partitionInfo,
    const internal::PartitioningConfiguration& partitionConfiguration) {
    auto local_logger = Common::GetScopedLogger(m_logger);
    Common::AddTableIDToLogger(*local_logger, table->GetID());

    auto prefixSumTable = std::make_shared<internal::PrefixSumTable>(
        partitionConfiguration.NumberOfPartitions, partitionConfiguration.NumberOfWorkers);

    // Define a task to create a temporary table containing sum of elements for each partition
    auto scanTable = [table, prefixSumTable, this, &partitionConfiguration, local_logger](
                         size_t tableStart, size_t tableEnd, size_t workerID) {
        LOG(*local_logger, Common::SeverityLevel::info)
            << "Worker " << workerID << " started scanning.";

        for (size_t i = tableStart; i != tableEnd; i++) {
            uint64_t partition =
                m_hasher.Hash((*table)[i].id, partitionConfiguration.NumberOfPartitions);
            prefixSumTable->Increment(partition, workerID);
        }

        LOG(*local_logger, Common::SeverityLevel::info)
            << "Worker " << workerID << " finished scanning positions [" << tableStart << ", "
            << tableEnd << "].";
    };

    // Define a task to modify the temporary table to contain running totals of the partitions
    auto numberOfFinishedPartitions = std::make_shared<std::atomic<size_t>>(0);
    auto partitionSizes =
        std::make_shared<std::vector<size_t>>(partitionConfiguration.NumberOfPartitions, 0);
    auto createPrefixSumTable = [prefixSumTable, this, &partitionConfiguration, &partitionInfo,
                                 partitionSizes, numberOfFinishedPartitions,
                                 local_logger](size_t partitionID) {
        LOG(*local_logger, Common::SeverityLevel::info)
            << "Partition " << partitionID << " started creating prefix sum table.";

        size_t runningBucketSize, currentBucketSize;
        for (size_t i = 0; i != partitionConfiguration.NumberOfWorkers; i++) {
            if (i == 0) {
                runningBucketSize = prefixSumTable->Get(partitionID, i);
                prefixSumTable->Set(partitionID, i, 0);
                continue;
            }

            currentBucketSize = prefixSumTable->Get(partitionID, i);
            prefixSumTable->Set(partitionID, i, runningBucketSize);
            runningBucketSize += currentBucketSize;
        }

        (*partitionSizes)[partitionID] = runningBucketSize;

        if (numberOfFinishedPartitions->operator++() == partitionConfiguration.NumberOfPartitions) {
            partitionInfo.ComputePartitionsBoundaries(*partitionSizes);
        }

        LOG(*local_logger, Common::SeverityLevel::info)
            << "Partition " << partitionID
            << " finished creating prefix sum table with size: " << runningBucketSize;
    };

    // Define a task to partition the original table so that its elements are organized per
    // partition and thread
    auto partitionTable = [table, partitionedTable, prefixSumTable, this, &partitionConfiguration,
                           &partitionInfo,
                           local_logger](size_t tableStart, size_t tableEnd, size_t workerID) {
        LOG(*local_logger, Common::SeverityLevel::info)
            << "Worker " << workerID << " started partitioning table [" << tableStart << ","
            << tableEnd << "].";

        for (size_t i = tableStart; i != tableEnd; i++) {
            uint64_t partition =
                m_hasher.Hash((*table)[i].id, partitionConfiguration.NumberOfPartitions);
            size_t position = prefixSumTable->Get(partition, workerID);
            auto [partitionStart, partitionEnd] = partitionInfo.GetPartitionBoundaries(partition);
            (*partitionedTable)[partitionStart + position] = (*table)[i];
            prefixSumTable->Increment(partition, workerID);
        }

        LOG(*local_logger, Common::SeverityLevel::info)
            << "Worker " << workerID << " finished partitioning table [" << tableStart << ","
            << tableEnd << "].";
    };

    std::vector<std::function<void()>> partitionTasks;
    std::vector<std::function<void()>> scanTasks;
    for (size_t i = 0; i != partitionConfiguration.NumberOfWorkers; i++) {
        size_t start = partitionConfiguration.BatchSize * i;
        size_t end = partitionConfiguration.BatchSize * (i + 1);

        if (i == partitionConfiguration.NumberOfWorkers - 1) {
            end = table->GetSize();
        }

        scanTasks.push_back(std::bind(scanTable, start, end, i));
        partitionTasks.push_back(std::bind(partitionTable, start, end, i));
    }

    std::vector<std::function<void()>> prefixSumTableTasks;
    for (size_t i = 0; i != partitionConfiguration.NumberOfPartitions; i++) {
        prefixSumTableTasks.push_back(std::bind(createPrefixSumTable, i));
    }

    std::shared_ptr<Common::IPipeline> pipeline = std::make_shared<Common::Pipeline>(m_threadPool);

    pipeline->Add(std::move(scanTasks));
    pipeline->Add(std::move(prefixSumTableTasks));
    pipeline->Add(std::move(partitionTasks));

    return m_threadPool->Push(pipeline);
}

}  // namespace RadixClustering
