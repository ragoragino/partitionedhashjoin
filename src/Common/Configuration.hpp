#pragma once

#include <string>
#include <iostream>
#include <vector>
#include <boost/program_options.hpp>

#include "NoPartitioning/Configuration.hpp"
#include "RadixCluster/Configuration.hpp"
#include "Logger.hpp"

namespace Common {
enum class JoinAlgorithmType : uint8_t {
        NoPartitioning = 0,
        RadixParitioning = 1,  
};

inline JoinAlgorithmType GetJoinAlgorithmTypeFromString(const std::string& algorithmType) {
    if (algorithmType == "no-partitioning") {
        return JoinAlgorithmType::NoPartitioning;
    } else if (algorithmType == "radix-partitioning") {
        return JoinAlgorithmType::RadixParitioning;
    } else {
        throw std::runtime_error("Unrecognized join algorithm type: " + algorithmType + ".");
    }
}

inline std::ostream& operator<<(std::ostream& os, JoinAlgorithmType algorithmType) {
    switch (algorithmType) {
        case Common::JoinAlgorithmType::NoPartitioning:
            return os <<  "no-partitioning";
        case Common::JoinAlgorithmType::RadixParitioning:
            return os << "radix-partitioning";
        default:
            os << static_cast<uint8_t>(algorithmType);
    }

    return os;
}

enum class ResultsFormat : uint8_t { 
    JSON = 0,
};

inline ResultsFormat GetResultsFormatFromString(const std::string& resultsFormat) {
    if (resultsFormat == "json") {
        return ResultsFormat::JSON;
    } else {
        throw std::runtime_error("Unrecognized results format: " + resultsFormat + ".");
    }
}

inline std::ostream& operator<<(std::ostream& os, ResultsFormat resultsFormat) {
    switch (resultsFormat) {
        case ResultsFormat::JSON:
            return os << "json";
        default:
            os << static_cast<uint8_t>(resultsFormat);
    }

    return os;
}

enum class OutputType : uint8_t {
    File = 0,
};

inline ResultsFormat GetOutputTypeFromString(const std::string& outputType) {
    if (outputType == "file") {
        return ResultsFormat::JSON;
    } else {
        throw std::runtime_error("Unrecognized output type: " + outputType + ".");
    }
}

inline std::ostream& operator<<(std::ostream& os, OutputType outputType) {
    switch (outputType) {
        case OutputType::File:
            return os << "file";
        default:
            os << static_cast<uint8_t>(outputType);
    }

    return os;
}

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

inline void validate(boost::any& v, const std::vector<std::string>& values,
                     SeverityLevel*,
              int) {
    if (values.size() != 1) {
        throw boost::program_options::invalid_option_value(
            "Invalid logger configuration specification");
    }

    v = SeverityLevelFromString(values.at(0));
}

inline void validate(boost::any& v, const std::vector<std::string>& values, JoinAlgorithmType*,
              int) {
    if (values.size() != 1) {
        throw boost::program_options::invalid_option_value(
            "Invalid logger configuration specification");
    }

    v = GetJoinAlgorithmTypeFromString(values.at(0));
}

inline void validate(boost::any& v, const std::vector<std::string>& values, ResultsFormat*, int) {
    if (values.size() != 1) {
        throw boost::program_options::invalid_option_value(
            "Invalid logger configuration specification");
    }

    v = GetResultsFormatFromString(values.at(0));
}

inline void validate(boost::any& v, const std::vector<std::string>& values, OutputType*, int) {
    if (values.size() != 1) {
        throw boost::program_options::invalid_option_value(
            "Invalid logger configuration specification");
    }

    v = GetResultsFormatFromString(values.at(0));
}
}

