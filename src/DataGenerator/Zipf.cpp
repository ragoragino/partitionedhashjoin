#include "Zipf.hpp"

#include <math.h>

#include <cstdint>
#include <future>
#include <memory>
#include <sstream>
#include <vector>
#include <random>

#ifndef MIN_BATCH_SIZE
#define MIN_BATCH_SIZE 10000
#endif

#ifndef ERROR_DIFFERENTIAL
#define ERROR_DIFFERENTIAL 0.01
#endif

namespace DataGenerator {

// https://medium.com/@jasoncrease/rejection-sampling-the-zipf-distribution-6b359792cffa
uint64_t Zipf::generate(double alpha, uint64_t cardinality,
                        std::shared_ptr<Common::IRandomNumberGenerator> generator) {
    if (alpha < 0.01) {
        throw std::invalid_argument("Skew parameter must be greater than 0.01.");
    }

    double skewDifferential = 1.001 - alpha; 
    if (double diff = 1.0 - alpha; abs(diff) < ERROR_DIFFERENTIAL) { 
        skewDifferential = ERROR_DIFFERENTIAL * ((diff < 0) ? 1 : -1);
        alpha = 1.0 - skewDifferential;
    }

    double normalizationConstant = (pow(cardinality, skewDifferential) - alpha) / skewDifferential;

    while (true) {
        double uniformRandom1 = generator->Next();
        double uniformRandom2 = generator->Next();

        // Set inverse CDF
        double invertedCDFSamplingFunc;
        if (uniformRandom1 * normalizationConstant <= 1.0) {
            invertedCDFSamplingFunc = uniformRandom1 * normalizationConstant;
        } else {
            invertedCDFSamplingFunc =
                pow((uniformRandom1 * normalizationConstant) * skewDifferential + alpha,
                    1.0 / skewDifferential);
        }

        double sample = floor(invertedCDFSamplingFunc + 1);
        double densityOriginalFunc = pow(sample, -alpha);
        double densitySamplingFunc =
            sample <= 1.0
                ? 1.0 / normalizationConstant
                : pow(invertedCDFSamplingFunc, -alpha) / normalizationConstant;
        double densitiesRatio =
            densityOriginalFunc / (densitySamplingFunc * normalizationConstant);

        if (uniformRandom2 < densitiesRatio) {
            return static_cast<uint64_t>(sample);
        }
    }
}

std::future<Common::TasksErrorHolder> Zipf::FillTable(
    std::shared_ptr<Common::IThreadPool> threadPool,
                                  std::shared_ptr<Common::Table<Common::Tuple>> table,
                                 const Parameters& parameters) {
    if (parameters.range.first >= parameters.range.second) {
        std::ostringstream errorMessage;
        errorMessage << "Range for Zipf generation is incorrectly specified: ["
                     << parameters.range.first << ", " << parameters.range.second << "].";

        throw std::invalid_argument(errorMessage.str());
    }

    const size_t size = table->GetSize();
    size_t numberOfWorkers = threadPool->GetNumberOfWorkers();
    size_t batchSize = static_cast<size_t>(size / numberOfWorkers);

    if (batchSize < MIN_BATCH_SIZE) {
        numberOfWorkers = 1;
        batchSize = size;
    }

    // We are doing closed range sampling, i.e. [x, y]
    int64_t cardinality = parameters.range.second - parameters.range.first + 1;
    int64_t correction =
        parameters.range.first - 1;  // [1, cardinality] is the sampling range of zip function

    auto fillBatch = [&table, &parameters, cardinality, correction](size_t start, size_t end) {
        std::shared_ptr<Common::IRandomNumberGenerator> generator = parameters.generatorFactory->GetNewGenerator();

        for (size_t i = start; i != end; i++) {
            // TODO: Probably call zipf for a batch of values?
            (*table)[i].id = Zipf::generate(parameters.alpha, cardinality, generator) + correction;
            (*table)[i].payload = 0;  // TODO
        }
    };

    std::vector<std::function<void()>> tasks;
    for (size_t i = 0; i != numberOfWorkers; i++) {
        size_t start = batchSize * i;
        size_t end = batchSize * (i + 1);

        if (i == numberOfWorkers - 1) {
            end = size;
        }

        tasks.push_back(std::bind(fillBatch, start, end));
    }

    return threadPool->Push(std::move(tasks));
}
}  // namespace DataGenerator
