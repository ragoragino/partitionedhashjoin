#pragma once

#include <cstdint>

namespace Common {
class IHasher {
   public:
    virtual uint64_t Hash(int64_t key, size_t cardinality) = 0;

    virtual ~IHasher() = default;
};
}  // namespace Common
