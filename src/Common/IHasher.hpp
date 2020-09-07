#pragma once

#include <cstdint>
#include <functional>
#include <future>
#include <vector>

namespace Common {
class IHasher {
   public:
    virtual uint64_t Hash(int64_t key, size_t cardinality) = 0;

    virtual ~IHasher() = default;
};
}  // namespace Common
