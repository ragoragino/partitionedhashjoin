#pragma once

#include "Common/Configuration.hpp"
#include <boost/program_options.hpp>

inline void validateParsedConfiguration(const Common::Configuration& configuration,
                                        const boost::program_options::variables_map& vm) {
    if (configuration.Output.Type == Common::OutputType::File) {
        if (configuration.Output.File.Name == "") {
            throw std::invalid_argument(
                "validateParsedConfiguration: empty configuration filename specified.");
        }
    }

    if (configuration.JoinType != Common::JoinAlgorithmType::RadixParitioning) {
        if (vm.count("partitions")) {
            throw std::invalid_argument(
                "validateParsedConfiguration: number of partitions can be specified only for "
                "RadixParitioning.");
        }
    }
}