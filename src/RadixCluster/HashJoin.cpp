#include "HashJoin.hpp"

#include "Common/ThreadPool.hpp"

namespace RadixClustering {
namespace internal {
PrefixSumTable::PrefixSumTable(size_t numberOfPartitions, size_t numberOfWorkers)
    : m_table(numberOfPartitions * numberOfWorkers), m_numberOfWorkers(numberOfWorkers) {}

void PrefixSumTable::Increment(size_t hashIndex, size_t workerIndex) {
    m_table[hashIndex * m_numberOfWorkers + workerIndex]++;
}

size_t PrefixSumTable::Get(size_t hashIndex, size_t workerIndex) const {
    return m_table[hashIndex * m_numberOfWorkers + workerIndex];
}

void PrefixSumTable::Set(size_t hashIndex, size_t workerIndex, size_t value) {
    m_table[hashIndex * m_numberOfWorkers + workerIndex] = value;
}

void PartitionsInfo::ComputePartitionsBoundaries(std::vector<size_t> partitionSizes) {
    m_partitionBorders.reserve(partitionSizes.size());

    m_partitionBorders.push_back(0);
    for (size_t i = 0; i != partitionSizes.size() - 1; i++) {
        m_partitionBorders.push_back(partitionSizes[i]);
    }
}

size_t PartitionsInfo::GetPartitionBoundary(size_t partition) {
    return m_partitionBorders[partition];
}

}  // namespace internal

HashJoiner::HashJoiner(Configuration configuration, std::shared_ptr<Common::IThreadPool> threadPool,
                       std::shared_ptr<Common::IHasher> hasher)
    : m_configuration(configuration),
      m_threadPool(threadPool),
      m_logger(Common::GetNewLogger()),
      m_hasher(hasher) {
    Common::AddComponentAttributeToLogger(m_logger, "NoPartitioning.HashJoiner");
}

std::pair<internal::PartitioningConfiguration, internal::PartitioningConfiguration>
HashJoiner::GetPartitioningConfiguration(std::shared_ptr<Common::Table<Common::Tuple>> tableA,
                                         std::shared_ptr<Common::Table<Common::Tuple>> tableB) {
    const size_t sizeA = tableA->GetSize();
    const size_t sizeB = tableB->GetSize();
    size_t numberOfWorkers = m_threadPool->GetNumberOfWorkers();
    size_t batchSizeA = static_cast<size_t>(sizeA / numberOfWorkers);
    size_t batchSizeB = static_cast<size_t>(sizeB / numberOfWorkers);

    if (batchSizeA < m_configuration.MIN_BATCH_SIZE) {
        numberOfWorkers = 1;
        batchSizeA = sizeA;
    }

    if (batchSizeB < m_configuration.MIN_BATCH_SIZE) {
        numberOfWorkers = 1;
        batchSizeB = sizeB;
    }

    internal::PartitioningConfiguration partitionConfigA{
        10,               // NumberOfPartitions,
        numberOfWorkers,  // NumberOfWorkers;
        batchSizeA,       // BatchSize
    };

    internal::PartitioningConfiguration partitionConfigB{
        10,               // NumberOfPartitions,
        numberOfWorkers,  // NumberOfWorkers;
        batchSizeB,       // BatchSize
    };

    return std::make_pair<internal::PartitioningConfiguration, internal::PartitioningConfiguration>(
        std::move(partitionConfigA), std::move(partitionConfigB));
}

std::shared_ptr<Common::Table<Common::JoinedTuple>> HashJoiner::Run(
    std::shared_ptr<Common::Table<Common::Tuple>> tableA,
    std::shared_ptr<Common::Table<Common::Tuple>> tableB) {
    auto partitionedTableA = std::make_shared<Common::Table<Common::Tuple>>(tableA->GetSize());
    auto partitionedTableB = std::make_shared<Common::Table<Common::Tuple>>(tableB->GetSize());

    const std::pair<internal::PartitioningConfiguration, internal::PartitioningConfiguration>
        partitionConfiguration = this->GetPartitioningConfiguration(tableA, tableB);

    auto partitionInfo =
        std::make_pair<internal::PartitionsInfo, internal::PartitionsInfo>(
            internal::PartitionsInfo{}, internal::PartitionsInfo{});

    auto partitionedTableAFuture =
        this->Partition(tableA, partitionedTableA, partitionInfo.first, partitionConfiguration.first);
    auto partitionedTableBFuture =
        this->Partition(tableB, partitionedTableB, partitionInfo.second, partitionConfiguration.second);

    partitionedTableAFuture.wait();
    partitionedTableBFuture.wait();

    if (!partitionedTableAFuture.get().Empty()) {
        throw partitionedTableAFuture.get().Pop();
    } else if (!partitionedTableBFuture.get().Empty()) {
        throw partitionedTableBFuture.get().Pop();
    }

    std::shared_ptr<Common::Table<Common::JoinedTuple>> joinedTable;

    auto joinFuture = this->Join(joinedTable, partitionedTableA, partitionedTableB, partitionInfo, partitionConfiguration);

    joinFuture.wait();

    if (!joinFuture.get().Empty()) {
        throw joinFuture.get().Pop();
    }

    return joinedTable;
}

// TODO
std::future<Common::TasksErrorHolder> HashJoiner::Join(
    std::shared_ptr<Common::Table<Common::JoinedTuple>> joinedTable,
    std::shared_ptr<Common::Table<Common::Tuple>> partitionedTableA,
    std::shared_ptr<Common::Table<Common::Tuple>> partitionedTableB,
    std::pair<internal::PartitionsInfo, internal::PartitionsInfo> partitionInfo,
    std::pair<internal::PartitioningConfiguration, internal::PartitioningConfiguration>
    partitionConfiguration) {

    auto join = [](){};
    std::vector<std::function<void()>> joinTasks{join};

    return m_threadPool->Push(std::move(joinTasks));
 }

std::future<Common::TasksErrorHolder> HashJoiner::Partition(
    std::shared_ptr<Common::Table<Common::Tuple>> table,
    std::shared_ptr<Common::Table<Common::Tuple>> partitionedTable,
    internal::PartitionsInfo& partitionInfo,
    const internal::PartitioningConfiguration& partitionConfiguration) {
    auto prefixSumTable = std::make_shared<internal::PrefixSumTable>(
        partitionConfiguration.NumberOfPartitions, partitionConfiguration.NumberOfWorkers);

    auto scanTable = [table, prefixSumTable, this, &partitionConfiguration](
                         size_t tableStart, size_t tableEnd, size_t workerID) {
        for (size_t i = tableStart; i != tableEnd; i++) {
            uint32_t partition =
                m_hasher->Hash((*table)[i].id, partitionConfiguration.NumberOfPartitions);
            prefixSumTable->Increment(partition, workerID);
        }

        LOG(m_logger, Common::SeverityLevel::info) << "Worker " << workerID << " finished scanning.";
    };

    auto numberOfFinishedPartitions = std::make_shared<std::atomic<size_t>>(0);
    auto partitionSizes = std::make_shared<std::vector<size_t>>(partitionConfiguration.NumberOfPartitions, 0);
    auto createPrefixSumTable = [prefixSumTable, this, &partitionConfiguration, &partitionInfo,
                                 partitionSizes, numberOfFinishedPartitions](size_t partitionID) {
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

        LOG(m_logger, Common::SeverityLevel::info) << "Worker " << partitionID << " finished creating prefix sum table.";
    };

    auto partitionTable = [table, partitionedTable, prefixSumTable, this, &partitionConfiguration,
                           &partitionInfo](size_t tableStart, size_t tableEnd, size_t workerID) {
        for (size_t i = tableStart; i != tableEnd; i++) {
            uint32_t partition =
                m_hasher->Hash((*table)[i].id, partitionConfiguration.NumberOfPartitions);
            size_t position = prefixSumTable->Get(partition, workerID);
            size_t partitionStart = partitionInfo.GetPartitionBoundary(partition);
            (*partitionedTable)[partitionStart + position] = (*table)[i];
            prefixSumTable->Increment(partition, workerID);
        }

        LOG(m_logger, Common::SeverityLevel::info)
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
