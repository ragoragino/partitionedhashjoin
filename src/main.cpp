#ifdef PHJ_USE_MIMALLOC
#include <mimalloc.h>
#endif

#include <boost/program_options.hpp>
#include <boost/thread/thread.hpp>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <future>
#include <iostream>
#include <limits>
#include <memory>
#include <thread>
#include <utility>

#include "Arguments.hpp"
#include "Common/Configuration.hpp"
#include "Common/IThreadPool.hpp"
#include "Common/Logger.hpp"
#include "Common/Random.hpp"
#include "Common/Table.hpp"
#include "Common/TestResults.hpp"
#include "Common/ThreadPool.hpp"
#include "Common/XXHasher.hpp"
#include "DataGenerator/Sequential.hpp"
#include "DataGenerator/Zipf.hpp"
#include "HashTables/LinearProbing.hpp"
#include "HashTables/SeparateChaining.hpp"
#include "NoPartitioning/HashJoin.hpp"
#include "RadixCluster/HashJoin.hpp"

std::pair<std::shared_ptr<Common::Table<Common::Tuple>>,
          std::shared_ptr<Common::Table<Common::Tuple>>>
generateTables(Common::LoggerType& logger, const Common::Configuration& config,
               std::shared_ptr<Common::IThreadPool> threadPool) {
    LOG(logger, Common::SeverityLevel::debug)
        << "Generating primary relation with size " << config.PrimaryRelationSize << " and "
        << "secondary relation with size " << config.SecondaryRelationSize << ".";

    auto randomNumberGeneratorFactory =
        std::make_shared<Common::MultiplicativeLCGRandomNumberGeneratorFactory>();

    auto primaryKeyRelation = std::make_shared<Common::Table<Common::Tuple>>(
        config.PrimaryRelationSize, Common::generate_uuid());
    auto secondaryKeyRelation = std::make_shared<Common::Table<Common::Tuple>>(
        config.SecondaryRelationSize, Common::generate_uuid());

    int64_t startIndex = 1;
    int64_t endIndex = startIndex + config.PrimaryRelationSize - 1;

    std::future<Common::TasksErrorHolder> generatePrimaryKeyRelationFuture =
        DataGenerator::Sequential::FillTable(threadPool, primaryKeyRelation,
                                             DataGenerator::Sequential::Parameters{startIndex});

    std::future<Common::TasksErrorHolder> generateSecondaryKeyRelationFuture =
        DataGenerator::Zipf::FillTable(
            threadPool, secondaryKeyRelation,
            DataGenerator::Zipf::Parameters{config.SkewParameter,
                                            std::make_pair(startIndex, endIndex),
                                            randomNumberGeneratorFactory});

    generatePrimaryKeyRelationFuture.wait();
    generateSecondaryKeyRelationFuture.wait();

    if (!generatePrimaryKeyRelationFuture.get().Empty()) {
        throw generatePrimaryKeyRelationFuture.get().Pop();
    }

    if (!generateSecondaryKeyRelationFuture.get().Empty()) {
        throw generateSecondaryKeyRelationFuture.get().Pop();
    }

    LOG(logger, Common::SeverityLevel::debug) << "Generation of relations finished.";

    return std::make_pair(primaryKeyRelation, secondaryKeyRelation);
}

template <typename HashTableFactory>
Common::HashJoinTimingResult joinNoPartitioning(
    Common::LoggerType& logger, const Common::Configuration& config,
    std::shared_ptr<Common::IThreadPool> threadPool,
    std::pair<std::shared_ptr<Common::Table<Common::Tuple>>,
              std::shared_ptr<Common::Table<Common::Tuple>>>
        relations,
    HashTableFactory factory) {
    LOG(logger, Common::SeverityLevel::debug) << "Executing NoPartitionHashJoin algorithm.";

    auto noPartitioningHashJoiner =
        NoPartitioning::HashJoiner(config.NoPartitioningConfiguration, threadPool, factory);

    Common::Parameters params;
    params.SetParameter("PrimaryRelationSize", std::to_string(config.PrimaryRelationSize));
    params.SetParameter("SecondaryRelationSize", std::to_string(config.SecondaryRelationSize));
    params.SetParameter("Skew", std::to_string(config.SkewParameter));
    params.SetParameter("Type", "NoPartitioning");

    std::shared_ptr<Common::IHashJoinTimer> timer = std::make_shared<Common::HashJoinTimer>(params);
    std::shared_ptr<Common::Table<Common::JoinedTuple>> outputTupleNoPartitioning =
        noPartitioningHashJoiner.Run(relations.first, relations.second, timer);

    LOG(logger, Common::SeverityLevel::debug)
        << "Finished executing NoPartitionHashJoin algorithm.";

    return timer->GetResult();
}

template <typename HashTableFactory, typename PartitionHasher>
Common::HashJoinTimingResult joinRadixPartitioning(
    Common::LoggerType& logger, const Common::Configuration& config,
    std::shared_ptr<Common::IThreadPool> threadPool,
    std::pair<std::shared_ptr<Common::Table<Common::Tuple>>,
              std::shared_ptr<Common::Table<Common::Tuple>>>
        relations,
    PartitionHasher partitionsHasher, HashTableFactory factory) {
    LOG(logger, Common::SeverityLevel::debug) << "Executing Radix Clustering join algorithm.";

    auto radixClusteringHashJoiner = RadixClustering::HashJoiner(
        config.RadixClusteringConfiguration, threadPool, partitionsHasher, factory);

    Common::Parameters params;
    params.SetParameter("PrimaryRelationSize", std::to_string(config.PrimaryRelationSize));
    params.SetParameter("SecondaryRelationSize", std::to_string(config.SecondaryRelationSize));
    params.SetParameter("Skew", std::to_string(config.SkewParameter));
    params.SetParameter("Type", "RadixParitioning");
    params.SetParameter("NumberOfPartitions",
                        std::to_string(config.RadixClusteringConfiguration.NumberOfPartitions));

    std::shared_ptr<Common::IHashJoinTimer> timer = std::make_shared<Common::HashJoinTimer>(params);
    std::shared_ptr<Common::Table<Common::JoinedTuple>> outputTupleRadixClustering =
        radixClusteringHashJoiner.Run(relations.first, relations.second, timer);

    LOG(logger, Common::SeverityLevel::debug)
        << "Finished executing Radix Clustering join algorithm.";

    return timer->GetResult();
}

Common::Configuration parseArguments(int argc, char** argv) {
    Common::Configuration configuration{};

    boost::program_options::options_description desc("Allowed options");
    desc.add_options()("help,h", "Help screen")(
        "primary",
        boost::program_options::value<size_t>(&configuration.PrimaryRelationSize)
            ->default_value(10'000'000),
        "Size of the primary relation.")(
        "secondary",
        boost::program_options::value<size_t>(&configuration.SecondaryRelationSize)
            ->default_value(200'000'000),
        "Size of the secondary relation.")(
        "skew",
        boost::program_options::value<double>(&configuration.SkewParameter)->default_value(1.05),
        "Parameter of skew for Zipf distribution used for the generation of tuples for secondary "
        "relation.")("log",
                     boost::program_options::value<Common::SeverityLevel>(
                         &configuration.LoggerConfiguration.SeverityLevel)
                         ->default_value(Common::debug, "trace"),
                     "Logging level. One of {trace, debug, info, error, critical}.")(
        "join",
        boost::program_options::value<Common::JoinAlgorithmType>(&configuration.JoinType)
            ->required(),
        "Type of join algorithm: either no-partitioning or radix-partitioning.")(
        "format",
        boost::program_options::value<Common::ResultsFormat>(&configuration.ResultsFormat)
            ->default_value(Common::ResultsFormat::JSON),
        "Format of the output. Currently only JSON is supported.")(
        "unit,u",
        boost::program_options::value<std::string>(&configuration.ResultsFormatConfiguration.TimeUnit)
            ->default_value("ms"),
        "Duration unit of the timing output. One of {ns, us, ms, s}.")(
        "output,o",
        boost::program_options::value<Common::OutputType>(&configuration.Output.Type)
            ->default_value(Common::OutputType::File),
        "Type of the output. Currently only file is supported.")(
        "filename,f",
        boost::program_options::value<std::string>(&configuration.Output.File.Name)
            ->default_value("hashjoin.txt"),
        "Name of the file if output type is file.")(
        "partitions,p",
        boost::program_options::value<size_t>(
            &configuration.RadixClusteringConfiguration.NumberOfPartitions),
        "Number of partitions for algorithms using partitioning.");

    try {
        boost::program_options::variables_map vm;
        boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc),
                                      vm);

        if (vm.count("help")) {
            std::cout << desc << "\n";
            exit(0);
        }

        boost::program_options::notify(vm);

        validateParsedConfiguration(configuration, vm);
    } catch (std::exception& e) {
        std::cout << e.what() << "\n\n";
        std::cout << desc << "\n";
        exit(1);
    }

    return configuration;
}

int main(int argc, char** argv) {
    static constexpr size_t TupleSize = 3;
    using TupleType = Common::Tuple;
    using HasherType = Common::XXHasher;

    HasherType hasher{};
    HashTables::LinearProbingFactory<TupleType, TupleSize, HasherType> hashTableFactory(
        HashTables::LinearProbingConfiguration{}, hasher);

#ifdef PHJ_USE_MIMALLOC
    // Ensure mimalloc library is linked
    mi_version();
#endif

    // Parse command line arguments with configuration parameters
    Common::Configuration configuration = parseArguments(argc, argv);

    // Initialize logger
    Common::InitializeLogger(configuration.LoggerConfiguration);

    auto logger = Common::GetNewLogger();
    Common::AddComponentAttributeToLogger(logger, "main");

    // Initialize thread pool
    // hardware_concurrency should take into account hyperthreading
    int maxNumberOfThreads = std::thread::hardware_concurrency() - 1;
    if (maxNumberOfThreads < 1) {
        maxNumberOfThreads = 1;
    }

    std::shared_ptr<Common::IThreadPool> threadPool =
        std::make_shared<Common::ThreadPool>(maxNumberOfThreads);

    // Initialize results formatter
    std::shared_ptr<Common::ITestResultsFormatter> resultsFormatter =
        Common::SelectResultsFormatter(configuration);

    // Initialize results renderer
    std::shared_ptr<Common::ITestResultsRenderer> resultsRenderer =
        Common::SelectResultsRenderer(configuration);

    LOG(logger, Common::SeverityLevel::info) << "Starting running tests.";

    // Generate primary and secondary tables
    auto relations = generateTables(logger, configuration, threadPool);

    // Run selected join algorithm
    Common::HashJoinTimingResult joinResults;
    switch (configuration.JoinType) {
        case Common::JoinAlgorithmType::NoPartitioning: {
            joinResults =
                joinNoPartitioning(logger, configuration, threadPool, relations, hashTableFactory);
            break;
        }
        case Common::JoinAlgorithmType::RadixParitioning: {
            HasherType partitionsHasher{};
            joinResults = joinRadixPartitioning(logger, configuration, threadPool, relations,
                                                partitionsHasher, hashTableFactory);
            break;
        }
        default:
            std::stringstream is;
            is << "Unrecognized join algorithm: " << configuration.JoinType << ".";
            throw std::runtime_error(is.str());
    }

    // Output results of the join algorithm
    resultsRenderer->Render(resultsFormatter, joinResults);

    LOG(logger, Common::SeverityLevel::info) << "Finished running tests.";

    threadPool->Stop();

    LOG(logger, Common::SeverityLevel::debug) << "ThreadPool stopped.";

    return 0;
}
