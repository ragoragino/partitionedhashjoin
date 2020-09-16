#include "LinearProbing.hpp"

namespace HashTables {
namespace internal {
namespace LinearProbing {

size_t getNumberOfBuckets(const LinearProbingConfiguration& configuration, size_t numberOfObjects) {
    size_t numberOfBuckets = static_cast<size_t>(
        std::ceil(configuration.HASH_TABLE_SIZE_RATIO * static_cast<double>(numberOfObjects)));

    if (numberOfBuckets < 1) {
        throw std::runtime_error(
            "getNumberOfBuckets: Cannot allocate less than 1 bucket for a linear probing hash "
            "table.");
    }

    return numberOfBuckets;
}

}  // namespace LinearProbing
}  // namespace internal
}  // namespace HashTables
