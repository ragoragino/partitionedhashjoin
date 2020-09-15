#include "Configuration.hpp"

namespace Common {
JoinAlgorithmType GetJoinAlgorithmTypeFromString(const std::string& algorithmType) {
    if (algorithmType == "no-partitioning") {
        return JoinAlgorithmType::NoPartitioning;
    } else if (algorithmType == "radix-partitioning") {
        return JoinAlgorithmType::RadixParitioning;
    } else {
        throw std::runtime_error("Unrecognized join algorithm type: " + algorithmType + ".");
    }
}

std::istream& operator>>(std::istream& in, JoinAlgorithmType& obj) {
    std::string s;
    in >> s;
    obj = GetJoinAlgorithmTypeFromString(std::move(s));
    return in;
}

std::ostream& operator<<(std::ostream& os, JoinAlgorithmType algorithmType) {
    switch (algorithmType) {
        case Common::JoinAlgorithmType::NoPartitioning:
            return os << "no-partitioning";
        case Common::JoinAlgorithmType::RadixParitioning:
            return os << "radix-partitioning";
        default:
            os << static_cast<uint8_t>(algorithmType);
    }

    return os;
}

ResultsFormat GetResultsFormatFromString(const std::string& resultsFormat) {
    if (resultsFormat == "json") {
        return ResultsFormat::JSON;
    } else {
        throw std::runtime_error("Unrecognized results format: " + resultsFormat + ".");
    }
}

std::istream& operator>>(std::istream& in, ResultsFormat& obj) {
    std::string s;
    in >> s;
    obj = GetResultsFormatFromString(std::move(s));
    return in;
}

std::ostream& operator<<(std::ostream& os, ResultsFormat resultsFormat) {
    switch (resultsFormat) {
        case ResultsFormat::JSON:
            return os << "json";
        default:
            os << static_cast<uint8_t>(resultsFormat);
    }

    return os;
}

OutputType GetOutputTypeFromString(const std::string& outputType) {
    if (outputType == "file") {
        return OutputType::File;
    } else {
        throw std::runtime_error("Unrecognized output type: " + outputType + ".");
    }
}

std::istream& operator>>(std::istream& in, OutputType& obj) {
    std::string s;
    in >> s;
    obj = GetOutputTypeFromString(std::move(s));
    return in;
}

std::ostream& operator<<(std::ostream& os, OutputType outputType) {
    switch (outputType) {
        case OutputType::File:
            return os << "file";
        default:
            os << static_cast<uint8_t>(outputType);
    }

    return os;
}

}  // namespace Common
