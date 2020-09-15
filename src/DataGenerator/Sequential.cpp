#include "Sequential.hpp"

#include <cmath>

namespace DataGenerator {
std::future<Common::TasksErrorHolder> Sequential::FillTable(
    std::shared_ptr<Common::IThreadPool> threadPool,
    std::shared_ptr<Common::Table<Common::Tuple>> table, const Parameters& parameters) {
    const size_t size = table->GetSize();
    size_t numberOfWorkers = threadPool->GetNumberOfWorkers();
    size_t batchSize =
        static_cast<size_t>(static_cast<double>(size) / static_cast<double>(numberOfWorkers));

    if (batchSize < parameters.minBatchSize) {
        numberOfWorkers = static_cast<size_t>(
            std::ceil(static_cast<double>(size) / static_cast<double>(parameters.minBatchSize)));
        batchSize = parameters.minBatchSize;
    }

    auto fillBatch = [&table](size_t tableStart, size_t tableEnd, int64_t indexStart) {
        for (size_t i = tableStart; i != tableEnd; i++) {
            (*table)[i].id = indexStart++;
            (*table)[i].payload = i;  // TODO: Maybe choose proper payload
        }
    };

    std::vector<std::function<void()>> tasks;
    for (size_t i = 0; i != numberOfWorkers; i++) {
        size_t start = batchSize * i;
        size_t end = batchSize * (i + 1);

        if (i == numberOfWorkers - 1) {
            end = size;
        }

        tasks.push_back(std::bind(fillBatch, start, end, parameters.start + start));
    }

    return threadPool->Push(std::move(tasks));
}
}  // namespace DataGenerator
