#pragma once

#include <memory>
#include <utility>

#include "Common/IHasher.hpp"
#include "Common/IThreadPool.hpp"
#include "Common/TestResults.hpp"
#include "Common/Logger.hpp"
#include "Common/Table.hpp"
#include "Configuration.hpp"

namespace NoPartitioning {
template <typename HashTableFactory>
class HashJoiner {
    using HashTableType = typename HashTableFactory::HashTableType;

   public:
    HashJoiner(Configuration configuration, std::shared_ptr<Common::IThreadPool> threadPool,
               const HashTableFactory& hashTableFactory);

    // tableA should be the build relation, while tableB should be probe relation
    std::shared_ptr<Common::Table<Common::JoinedTuple>> Run(
        std::shared_ptr<Common::Table<Common::Tuple>> tableA,
        std::shared_ptr<Common::Table<Common::Tuple>> tableB,
        std::shared_ptr<Common::IHashJoinTimer> timer =
            std::make_shared<Common::NoOpHashJoinTimer>());

   private:
    std::shared_ptr<HashTableType> Build(std::shared_ptr<Common::Table<Common::Tuple>> tableA,
                                         size_t numberOfWorkers);

    std::shared_ptr<Common::Table<Common::JoinedTuple>> Probe(
        std::shared_ptr<HashTableType> hashTable,
        std::shared_ptr<Common::Table<Common::Tuple>> tableB, size_t numberOfWorkers);

    std::shared_ptr<Common::IThreadPool> m_threadPool;
    Configuration m_configuration;
    Common::LoggerType m_logger;
    HashTableFactory m_hashTableFactory;
};

template <typename HashTableFactory>
HashJoiner<HashTableFactory>::HashJoiner(Configuration configuration,
                                         std::shared_ptr<Common::IThreadPool> threadPool,
                                         const HashTableFactory& hashTableFactory)
    : m_configuration(configuration),
      m_threadPool(threadPool),
      m_logger(Common::GetNewLogger()),
      m_hashTableFactory(hashTableFactory) {
    Common::AddComponentAttributeToLogger(m_logger, "NoPartitioning.HashJoiner");
}

template <typename HashTableFactory>
std::shared_ptr<Common::Table<Common::JoinedTuple>> HashJoiner<HashTableFactory>::Run(
    std::shared_ptr<Common::Table<Common::Tuple>> tableA,
    std::shared_ptr<Common::Table<Common::Tuple>> tableB,
    std::shared_ptr<Common::IHashJoinTimer> timer) {
    size_t numberOfWorkers = m_threadPool->GetNumberOfWorkers();

    LOG(m_logger, Common::debug) << "Starting hash partitioning.";

    timer->SetBuildPhaseBegin();
    auto hashTable = this->Build(tableA, numberOfWorkers);
    timer->SetBuildPhaseEnd();

    timer->SetProbePhaseBegin();
    auto joinedTable = this->Probe(hashTable, tableB, numberOfWorkers);
    timer->SetProbePhaseEnd();

    LOG(m_logger, Common::debug) << "Finished hash partitioning.";

    return joinedTable;
}

template <typename HashTableFactory>
std::shared_ptr<typename HashJoiner<HashTableFactory>::HashTableType>
HashJoiner<HashTableFactory>::Build(std::shared_ptr<Common::Table<Common::Tuple>> tableA,
                                    size_t numberOfWorkers) {
    const size_t tableASize = tableA->GetSize();

    auto hashTable = m_hashTableFactory.New(tableASize);

    size_t batchSize =
        static_cast<size_t>(static_cast<double>(tableASize) / static_cast<double>(numberOfWorkers));

    if (batchSize < m_configuration.MinBatchSize) {
        numberOfWorkers = static_cast<size_t>(std::ceil(
            static_cast<double>(tableASize) / static_cast<double>(m_configuration.MinBatchSize)));
        batchSize = m_configuration.MinBatchSize;
    }

    auto buildHashTable = [&tableA, hashTable](size_t tableStart, size_t tableEnd) {
        for (size_t i = tableStart; i != tableEnd; i++) {
            const Common::Tuple& tuple = (*tableA)[i];
            hashTable->Insert(tuple.id, &tuple);
        }
    };

    std::vector<std::function<void()>> tasks;
    for (size_t i = 0; i != numberOfWorkers; i++) {
        size_t start = batchSize * i;
        size_t end = batchSize * (i + 1);

        if (i == numberOfWorkers - 1) {
            end = tableASize;
        }

        tasks.push_back(std::bind(buildHashTable, start, end));
    }

    LOG(m_logger, Common::debug) << "Starting build phase.";

    std::future<Common::TasksErrorHolder> createHashTableFuture =
        m_threadPool->Push(std::move(tasks));

    createHashTableFuture.wait();

    if (!createHashTableFuture.get().Empty()) {
        throw createHashTableFuture.get().Pop();
    }

    LOG(m_logger, Common::debug) << "Finished build phase.";

    return hashTable;
}

template <typename HashTableFactory>
std::shared_ptr<Common::Table<Common::JoinedTuple>> HashJoiner<HashTableFactory>::Probe(
    std::shared_ptr<HashTableType> hashTable, std::shared_ptr<Common::Table<Common::Tuple>> tableB,
    size_t numberOfWorkers) {
    const size_t tableBSize = tableB->GetSize();
    
    size_t batchSize =
        static_cast<size_t>(static_cast<double>(tableBSize) / static_cast<double>(numberOfWorkers));

    if (batchSize < m_configuration.MinBatchSize) {
        numberOfWorkers = static_cast<size_t>(std::ceil(
            static_cast<double>(tableBSize) / static_cast<double>(m_configuration.MinBatchSize)));
        batchSize = m_configuration.MinBatchSize;
    }

    std::atomic<size_t> globalCounter(0);
    auto probeHashTable = [&globalCounter, &tableB, hashTable](size_t index, size_t tableStart,
                                                               size_t tableEnd) {
        size_t counter = 0;

        for (size_t i = tableStart; i != tableEnd; i++) {
            const Common::Tuple& tuple = (*tableB)[i];
            const Common::Tuple* tableATuple = hashTable->Get(tuple.id);
            if (tableATuple != nullptr) {
                counter++;
            }
        }

        globalCounter.fetch_add(counter);
    };

    std::vector<std::function<void()>> tasks;
    for (size_t i = 0; i != numberOfWorkers; i++) {
        size_t start = batchSize * i;
        size_t end = batchSize * (i + 1);

        if (i == numberOfWorkers - 1) {
            end = tableBSize;
        }

        tasks.push_back(std::bind(probeHashTable, i, start, end));
    }

    LOG(m_logger, Common::debug) << "Starting probe phase.";

    std::future<Common::TasksErrorHolder> probeHashTableFuture =
        m_threadPool->Push(std::move(tasks));

    probeHashTableFuture.wait();

    if (!probeHashTableFuture.get().Empty()) {
        throw probeHashTableFuture.get().Pop();
    }

    LOG(m_logger, Common::debug) << "Finished probe phase.";

    LOG(m_logger, Common::debug) << "Joined " << globalCounter.load() << " tuples.";

    return std::make_shared<Common::Table<Common::JoinedTuple>>(Common::generate_uuid());
}

}  // namespace NoPartitioning
