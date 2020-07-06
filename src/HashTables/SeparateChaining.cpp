#include "SeparateChaining.hpp"

namespace HashTables {
namespace internal {
namespace SeparateChaining {

size_t getNumberOfBuckets(const SeparateChainingConfiguration& configuration,
                          size_t numberOfObjects) {
    size_t numberOfBuckets = static_cast<size_t>(
        ceil(configuration.HASH_TABLE_SIZE_RATIO * static_cast<double>(numberOfObjects)));

    if (numberOfBuckets < 1) {
        throw std::runtime_error(
            "Cannot allocate less than 1 bucket for a separate chaining hash table.");
    }

    return numberOfBuckets;
}

}  // namespace SeparateChaining
}  // namespace internal
}  // namespace HashTables