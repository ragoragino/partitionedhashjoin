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
        const int64_t start;
    };

    static std::future<void> FillTable(std::shared_ptr<Common::IThreadPool> threadPool,
                                      std::shared_ptr<Common::Table> table,
                                      const Parameters& parameters);
};
};  // namespace DataGenerator
