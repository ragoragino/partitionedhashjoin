#pragma once

#include "IHasher.hpp"
#include "MurmurHash.hpp"

namespace Common {
class MurmurHasher : public IHasher {
   public:
    virtual uint64_t Hash(int64_t key, size_t cardinality) override {
        auto hash = MurmurHash64A(static_cast<const void*>(&key), sizeof(key), 100);
        return hash % cardinality;
    }

    virtual ~MurmurHasher() = default;
};
}  // namespace Common