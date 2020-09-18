#include "LinearProbing.hpp"

namespace HashTables {
namespace internal {
namespace LinearProbing {

size_t getNumberOfBuckets(const LinearProbingConfiguration& configuration, size_t numberOfObjects) {
    size_t numberOfBuckets = static_cast<size_t>(
        std::ceil(configuration.HASH_TABLE_SIZE_RATIO * static_cast<double>(numberOfObjects)));

    return numberOfBuckets;
}

}  // namespace LinearProbing
}  // namespace internal
}  // namespace HashTables
