#include <mimalloc.h>

#include <cstdint>
#include <fstream>
#include <future>
#include <iostream>
#include <limits>
#include <memory>
#include <thread>

#include "Common/Configuration.hpp"
#include "Common/IThreadPool.hpp"
#include "Common/Logger.hpp"
#include "Common/Random.hpp"
#include "Common/Table.hpp"
#include "Common/ThreadPool.hpp"
#include "DataGenerator/Sequential.hpp"
#include "DataGenerator/Zipf.hpp"
#include "NoPartitioning/HashJoin.hpp"

int main(int argc, char** argv) {
    mi_version();  // ensure mimalloc library is linked

    Common::Configuration configuration{NoPartitioning::Configuration{
        10000,          // MIN_BATCH_SIZE
        0.75,           // HASH_TABLE_SIZE_RATIO
        1'000'000'000,  // HASH_TABLE_SIZE_LIMIT
    }};

    Common::LoggerConfiguration logger_configuration{};
    logger_configuration.severity_level = Common::SeverityLevel::trace;

    Common::InitializeLogger(logger_configuration);

    auto logger = Common::GetNewLogger();
    Common::AddComponentAttributeToLogger(logger, "main");

    uint64_t primaryKeyRelationSize = 16'000'000;
    uint64_t secondaryKeyRelationSize = 25'600'000;

    LOG(logger, Common::SeverityLevel::info)
        << "Generating primary relation with size " << primaryKeyRelationSize << " and "
        << "secondary relation with size " << secondaryKeyRelationSize << ".";

    // hardware_concurrency should take into account hyperthreading
    int maxNumberOfThreads = std::thread::hardware_concurrency() - 1;
    if (maxNumberOfThreads < 1) {
        maxNumberOfThreads = 1;
    }

    std::shared_ptr<Common::IThreadPool> threadPool =
        std::make_shared<Common::ThreadPool>(maxNumberOfThreads);

    auto randomNumberGeneratorFactory =
        std::make_shared<Common::MultiplicativeLCGRandomNumberGeneratorFactory>();

    auto primaryKeyRelation =
        std::make_shared<Common::Table<Common::Tuple>>(primaryKeyRelationSize);
    auto secondaryKeyRelation =
        std::make_shared<Common::Table<Common::Tuple>>(secondaryKeyRelationSize);

    int64_t startIndex = 1;
    int64_t endIndex = startIndex + primaryKeyRelationSize - 1;

    std::future<std::vector<std::string>> generatePrimaryKeyRelationFuture =
        DataGenerator::Sequential::FillTable(
        threadPool, primaryKeyRelation, DataGenerator::Sequential::Parameters{startIndex});

    std::future<std::vector<std::string>> generateSecondaryKeyRelationFuture =
        DataGenerator::Zipf::FillTable(
        threadPool, secondaryKeyRelation,
        DataGenerator::Zipf::Parameters{1.0, std::make_pair(startIndex, endIndex),
                                        randomNumberGeneratorFactory});

    generatePrimaryKeyRelationFuture.wait();
    generateSecondaryKeyRelationFuture.wait();

    if (generatePrimaryKeyRelationFuture.get().size() != 0) {
        std::string concatErrors;
        for (const std::string& error : generatePrimaryKeyRelationFuture.get()) {
            concatErrors += error + "; ";
        }
        throw std::runtime_error(concatErrors);
    }

    if (generateSecondaryKeyRelationFuture.get().size() != 0) {
        std::string concatErrors;
        for (const std::string& error : generateSecondaryKeyRelationFuture.get()) {
            concatErrors += error + "; ";
        }
        throw std::runtime_error(concatErrors);
    }

    LOG(logger, Common::SeverityLevel::info) << "Generating finished.";

    LOG(logger, Common::SeverityLevel::info) << "Executing NoPartitionHashJoin algorithm.";

    NoPartitioning::HashJoiner noPartitioningHashJoiner(configuration.NoPartitioningConfiguration,
                                                        threadPool);

    std::shared_ptr<Common::Table<Common::JoinedTuple>> outputTuple = 
        noPartitioningHashJoiner.Run(primaryKeyRelation, secondaryKeyRelation);

    threadPool->Stop();

    LOG(logger, Common::SeverityLevel::info) << "ThreadPool stopped.";

    /*
    HashJoin::RadixClusterPartitioned(threadPool, primaryKeyRelation, secondaryKeyRelation);

    // Data distribution with low skew
    DataGenerator::Zipf::generate(threadPool, secondaryKeyRelation,
    DataGenerator::Zipf::Parameters{ 1.05, primaryKeyRelationSize });

    HashJoin::NoPartitioned(threadPool, primaryKeyRelation, secondaryKeyRelation);
    HashJoin::RadixClusterPartitioned(threadPool, primaryKeyRelation, secondaryKeyRelation);

    // Data distribution with high skew
    DataGenerator::Zipf::generate(threadPool, secondaryKeyRelation,
    DataGenerator::Zipf::Parameters{ 1.25, primaryKeyRelationSize });

    HashJoin::NoPartitioned(threadPool, primaryKeyRelation, secondaryKeyRelation);
    HashJoin::RadixClusterPartitioned(threadPool, primaryKeyRelation, secondaryKeyRelation);
    */

    return 0;
}
