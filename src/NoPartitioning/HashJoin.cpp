#include "HashJoin.hpp"

#include "HashTable.hpp"

namespace NoPartitioning {
HashJoiner::HashJoiner(Configuration configuration, std::shared_ptr<Common::IThreadPool> threadPool)
    : m_configuration(configuration), m_threadPool(threadPool), m_logger(Common::GetNewLogger()) {
    Common::AddComponentAttributeToLogger(m_logger, "NoPartitioning.HashJoiner");
}

std::shared_ptr<Common::Table<Common::JoinedTuple>> HashJoiner::Run(
    std::shared_ptr<Common::Table<Common::Tuple>> tableA,
    std::shared_ptr<Common::Table<Common::Tuple>> tableB) {
    size_t numberOfWorkers = m_threadPool->GetNumberOfWorkers();

    LOG(m_logger, Common::debug) << "Starting hash partitioning with " << numberOfWorkers
                                 << " number of workers.";

    auto hashTable = this->Build(tableA, numberOfWorkers);
    return this->Probe(hashTable, tableB, numberOfWorkers);
}

std::shared_ptr<SeparateChainingHashTable<Common::Tuple, HASH_TABLE_BUCKET_SIZE>> HashJoiner::Build(
    std::shared_ptr<Common::Table<Common::Tuple>> tableA, size_t numberOfWorkers) {
    const size_t tableASize = tableA->GetSize();
    double expectedHashTableSize =
        static_cast<double>(tableASize) / m_configuration.HASH_TABLE_SIZE_RATIO;

    if (expectedHashTableSize > m_configuration.HASH_TABLE_SIZE_LIMIT) {
        expectedHashTableSize = m_configuration.HASH_TABLE_SIZE_LIMIT;
    }

    size_t hashTableSize = expectedHashTableSize / static_cast<double>(HASH_TABLE_BUCKET_SIZE);

    std::shared_ptr<Common::IHasher> hasher = std::make_shared<Common::XXHasher>();
    auto hashTable =
        std::make_shared<SeparateChainingHashTable<Common::Tuple, HASH_TABLE_BUCKET_SIZE>>(
            hasher, hashTableSize, tableASize);

    size_t batchSize = static_cast<size_t>(tableASize / numberOfWorkers);

    if (batchSize < m_configuration.MIN_BATCH_SIZE) {
        numberOfWorkers = 1;
        batchSize = tableASize;
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

    std::future<std::vector<std::string>> createHashTableFuture =
        m_threadPool->Push(std::move(tasks));

    createHashTableFuture.wait();

    if (createHashTableFuture.get().size() != 0) {
        std::string concatErrors;
        for (const std::string& error : createHashTableFuture.get()) {
            concatErrors += error + "; ";
        }
        throw std::runtime_error(concatErrors);
    }

    return hashTable;
}

std::shared_ptr<Common::Table<Common::JoinedTuple>> HashJoiner::Probe(
    std::shared_ptr<SeparateChainingHashTable<Common::Tuple, HASH_TABLE_BUCKET_SIZE>> hashTable,
    std::shared_ptr<Common::Table<Common::Tuple>> tableB, size_t numberOfWorkers) {
    const size_t tableBSize = tableB->GetSize();

    size_t batchSize = static_cast<size_t>(tableBSize / numberOfWorkers);

    if (batchSize < m_configuration.MIN_BATCH_SIZE) {
        numberOfWorkers = 1;
        batchSize = tableBSize;
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

    std::future<std::vector<std::string>> probeHashTableFuture =
        m_threadPool->Push(std::move(tasks));

    probeHashTableFuture.wait();

    if (probeHashTableFuture.get().size() != 0) {
        std::string concatErrors;
        for (const std::string& error : probeHashTableFuture.get()) {
            concatErrors += error + "; ";
        }
        throw std::runtime_error(concatErrors);
    }
        
    LOG(m_logger, Common::debug) << "Joined " << globalCounter.load() << " tuples.";

    return std::make_shared<Common::Table<Common::JoinedTuple>>();
}

}  // namespace NoPartitioning