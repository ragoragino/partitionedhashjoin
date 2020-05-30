#include "HashTable.hpp"
#include "Configuration.hpp"

#include "Common/Table.hpp"
#include "Common/Logger.hpp"
#include "Common/IThreadPool.hpp"

namespace NoPartitioning {
class HashJoiner {
   public:
    HashJoiner(Configuration configuration, std::shared_ptr<Common::IThreadPool> threadPool);

    // tableA should be the build relation, while tableB should be probe relation
    void Run(std::shared_ptr<Common::Table> tableA, std::shared_ptr<Common::Table> tableB);

    private:
        std::shared_ptr<Common::IThreadPool> m_threadPool;
        Configuration m_configuration;
};
}  // namespace NoPartitioning