#include <memory>

#include "Common/IThreadPool.hpp"
#include "Common/Logger.hpp"
#include "Common/Table.hpp"
#include "Configuration.hpp"
#include "HashTable.hpp"

#ifndef HASH_TABLE_BUCKET_SIZE
#define HASH_TABLE_BUCKET_SIZE 3
#endif

namespace NoPartitioning {
class HashJoiner {
   public:
    HashJoiner(Configuration configuration, std::shared_ptr<Common::IThreadPool> threadPool);

    // tableA should be the build relation, while tableB should be probe relation
    std::shared_ptr<Common::Table<Common::JoinedTuple>>
    Run(std::shared_ptr<Common::Table<Common::Tuple>> tableA,
             std::shared_ptr<Common::Table<Common::Tuple>> tableB);

   private:
    std::shared_ptr<SeparateChainingHashTable<Common::Tuple, HASH_TABLE_BUCKET_SIZE>> Build(
        std::shared_ptr<Common::Table<Common::Tuple>> tableA, size_t numberOfWorkers);

    std::shared_ptr<Common::Table<Common::JoinedTuple>> Probe(
        std::shared_ptr<SeparateChainingHashTable<Common::Tuple, HASH_TABLE_BUCKET_SIZE>> hashTable,
        std::shared_ptr<Common::Table<Common::Tuple>> tableB, size_t numberOfWorkers);

    std::shared_ptr<Common::IThreadPool> m_threadPool;
    Configuration m_configuration;
    Common::LoggerType m_logger;
};
}  // namespace NoPartitioning
