#include <cstdint>
#include <future>
#include <limits>
#include <memory>
#include <thread>

#include <fstream>
#include <iostream>

#include "Common/IThreadPool.hpp"
#include "Common/Logger.hpp"
#include "Common/Random.hpp"
#include "Common/Table.hpp"
#include "Common/ThreadPool.hpp"
#include "DataGenerator/Sequential.hpp"
#include "DataGenerator/Zipf.hpp"

int main(int argc, char** argv) {
    Common::LoggerConfiguration logger_configuration{};
    logger_configuration.severity_level = Common::SeverityLevel::info;

    Common::InitializeLogger(logger_configuration);

    auto logger = Common::GetNewLogger();

    uint64_t primaryKeyRelationSize = 16'000;
    uint64_t secondaryKeyRelationSize = 256'000;

    std::string primaryFilename = "primary_relation.txt";
    std::string secondaryFilename = "secondary_relation.txt";

    BOOST_LOG_SEV(logger, Common::SeverityLevel::info)
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

    std::shared_ptr<Common::Table> primaryKeyRelation =
        std::make_shared<Common::Table>(primaryKeyRelationSize);
    std::shared_ptr<Common::Table> secondaryKeyRelation =
        std::make_shared<Common::Table>(secondaryKeyRelationSize);

    int64_t startIndex = 1;
    int64_t endIndex = startIndex + primaryKeyRelationSize - 1;

    std::future<void> generatePrimaryKeyRelationFuture = DataGenerator::Sequential::FillTable(
        threadPool, primaryKeyRelation, DataGenerator::Sequential::Parameters{startIndex});

    std::future<void> generateSecondaryKeyRelationFuture = DataGenerator::Zipf::FillTable(
        threadPool, secondaryKeyRelation,
        DataGenerator::Zipf::Parameters{1.0, std::make_pair(startIndex, endIndex),
                                        randomNumberGeneratorFactory});

    generatePrimaryKeyRelationFuture.wait();
    generateSecondaryKeyRelationFuture.wait();

    threadPool->Stop();

    BOOST_LOG_SEV(logger, Common::SeverityLevel::info) << "Generating finished.";

    std::ofstream primaryFile(primaryFilename);
    for (int i = 0; i != primaryKeyRelation->GetSize(); i++) {
        if (primaryFile.is_open()) {
            primaryFile << (*primaryKeyRelation)[i] << "\n";
        }
    }

    primaryFile.close();

    std::ofstream secondaryFile(secondaryFilename);
    for (int i = 0; i != secondaryKeyRelation->GetSize(); i++) {
        if (secondaryFile.is_open()) {
            secondaryFile << (*secondaryKeyRelation)[i] << "\n";
        }
    }

    secondaryFile.close();

    /*
    HashJoin::NoPartitioned(threadPool, primaryKeyRelation, secondaryKeyRelation);
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
