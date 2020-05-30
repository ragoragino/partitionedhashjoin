#include "HashJoin.hpp"
#include "HashTable.hpp"

#include "Common/Logger.hpp"

namespace NoPartitioning {
HashJoiner::HashJoiner(Configuration configuration, std::shared_ptr<Common::IThreadPool> threadPool)
    : m_configuration(configuration), m_threadPool(threadPool), m_logger(Common::GetNewLogger()) {
    Common::AddComponentAttributeToLogger(m_logger, "HashJoiner");
}

void HashJoiner::Run(std::shared_ptr<Common::Table> tableA, std::shared_ptr<Common::Table> tableB) {
    const size_t tableASize = tableA->GetSize();
    double expectedHashTableSize =
        static_cast<double>(tableASize) * 1.0 / m_configuration.HASH_TABLE_SIZE_RATIO;

    if (expectedHashTableSize > m_configuration.HASH_TABLE_SIZE_LIMIT) {
        expectedHashTableSize = m_configuration.HASH_TABLE_SIZE_LIMIT;
    }

    std::shared_ptr<Common::IHasher> hasher = std::make_shared<Common::XXHasher>();
    auto hashTable = std::make_shared<HashTable<Common::Tuple>>(hasher, 100000);

    size_t numberOfWorkers = m_threadPool->GetNumberOfWorkers();
    size_t batchSize = static_cast<size_t>(tableASize / numberOfWorkers);

    if (batchSize < m_configuration.MIN_BATCH_SIZE) {
        numberOfWorkers = 1;
        batchSize = tableASize;
    }

    auto populateHashTable = [this, &tableA, hashTable](size_t tableStart, size_t tableEnd) {
        LOG(m_logger, Common::SeverityLevel::debug)
            << "Joining ranges: " << tableStart << " - " << tableEnd << ".";

        for (size_t i = tableStart; i != tableEnd; i++) {
            const Common::Tuple& tuple = (*tableA)[i];
            hashTable->Insert(tuple.id, &tuple);
        }

        LOG(m_logger, Common::SeverityLevel::debug)
            << "Joining ranges: " << tableStart << " - " << tableEnd << " finished.";
    };

    std::vector<std::function<void()>> tasks;
    for (size_t i = 0; i != numberOfWorkers; i++) {
        size_t start = batchSize * i;
        size_t end = batchSize * (i + 1);

        if (i == numberOfWorkers - 1) {
            end = tableASize;
        }

        tasks.push_back(std::bind(populateHashTable, start, end));
    }

    std::future<void> createHashTableFuture = m_threadPool->Push(std::move(tasks));

    createHashTableFuture.wait();
}
}  // namespace NoPartitioning
