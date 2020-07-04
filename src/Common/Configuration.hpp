#pragma once

#include "NoPartitioning/Configuration.hpp"
#include "RadixCluster/Configuration.hpp"

namespace Common {
struct Configuration {
    const NoPartitioning::Configuration NoPartitioningConfiguration;
    const RadixClustering::Configuration RadixClusteringConfiguration;
};
}
