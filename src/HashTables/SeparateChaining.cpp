#include "SeparateChaining.hpp"

namespace HashTables {
namespace internal {
namespace SeparateChaining {

size_t getNumberOfBuckets(const SeparateChainingConfiguration& configuration,
                          size_t numberOfObjects) {
    size_t numberOfBuckets = static_cast<size_t>(
        std::ceil(configuration.HASH_TABLE_SIZE_RATIO * static_cast<double>(numberOfObjects)));

    return numberOfBuckets;
}

}  // namespace SeparateChaining
}  // namespace internal
}  // namespace HashTables