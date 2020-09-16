#pragma once

#include <cstdint>
#include <future>
#include <memory>

#include "Common/IThreadPool.hpp"
#include "Common/Table.hpp"

namespace DataGenerator {
class Sequential {
   public:
    struct Parameters {
        int64_t start;
        size_t minBatchSize = 10000;
    };

    static std::future<Common::TasksErrorHolder> FillTable(
        std::shared_ptr<Common::IThreadPool> threadPool,
        std::shared_ptr<Common::Table<Common::Tuple>> table, const Parameters& parameters);
};
}  // namespace DataGenerator
