#pragma once

#include <cstdint>
#include <future>
#include <memory>
#include <utility>

#include "Common/IThreadPool.hpp"
#include "Common/Random.hpp"
#include "Common/Table.hpp"

namespace DataGenerator {
class Zipf {
   public:
    struct Parameters {
        const double alpha;
        const std::pair<size_t, size_t> range;
        std::shared_ptr<Common::IRandomNumberGeneratorFactory> generatorFactory;
    };

    static std::future<std::vector<std::string>> FillTable(
        std::shared_ptr<Common::IThreadPool> threadPool,
                                       std::shared_ptr<Common::Table<Common::Tuple>> table,
                                       const Parameters& parameters);

   protected:
    static uint64_t generate(double alpha, uint64_t cardinality,
                             std::shared_ptr<Common::IRandomNumberGenerator> generator);
};
};  // namespace DataGenerator
