#pragma once

#include <iostream>
#include <string>
#include <vector>

#include "Logger.hpp"
#include "NoPartitioning/Configuration.hpp"
#include "RadixCluster/Configuration.hpp"

namespace Common {
enum class JoinAlgorithmType : uint8_t {
    NoPartitioning = 0,
    RadixParitioning = 1,
};

JoinAlgorithmType GetJoinAlgorithmTypeFromString(const std::string& algorithmType);
std::istream& operator>>(std::istream& in, JoinAlgorithmType& obj);
std::ostream& operator<<(std::ostream& os, JoinAlgorithmType algorithmType);

enum class ResultsFormat : uint8_t {
    JSON = 0,
};

ResultsFormat GetResultsFormatFromString(const std::string& resultsFormat);
std::istream& operator>>(std::istream& in, ResultsFormat& obj);
std::ostream& operator<<(std::ostream& os, ResultsFormat resultsFormat);

enum class OutputType : uint8_t {
    File = 0,
};

OutputType GetOutputTypeFromString(const std::string& outputType);
std::istream& operator>>(std::istream& in, OutputType& obj);
std::ostream& operator<<(std::ostream& os, OutputType outputType);

struct FileConfiguration {
    std::string Name;
};

struct OutputConfiguration {
    OutputType Type;
    FileConfiguration File;
};

struct Configuration {
    JoinAlgorithmType JoinType;
    ResultsFormat ResultFormat;
    OutputConfiguration Output;

    size_t PrimaryRelationSize;
    size_t SecondaryRelationSize;
    double SkewParameter;

    NoPartitioning::Configuration NoPartitioningConfiguration;
    RadixClustering::Configuration RadixClusteringConfiguration;

    LoggerConfiguration LoggerConfiguration;
};
}  // namespace Common
