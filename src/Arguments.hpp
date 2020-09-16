#pragma once

#include <boost/program_options.hpp>

#include "Common/Configuration.hpp"

inline void validateParsedConfiguration(const Common::Configuration& configuration,
                                        const boost::program_options::variables_map& vm) {
    configuration.Output.Validate();
    configuration.ResultsFormatConfiguration.Validate();

    if (configuration.JoinType != Common::JoinAlgorithmType::RadixParitioning) {
        if (vm.count("partitions")) {
            throw std::invalid_argument(
                "validateParsedConfiguration: number of partitions can be specified only for "
                "RadixParitioning.");
        }
    }
}