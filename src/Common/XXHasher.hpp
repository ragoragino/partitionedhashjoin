#pragma once

#include "IHasher.hpp"

#include "xxhash.hpp"

namespace Common {
class XXHasher : public IHasher {
   public:
    virtual uint32_t Hash(int64_t key, size_t cardinality) override {
        xxh::hash_t<64> hash = xxh::xxhash<64>(static_cast<const void*>(&key), sizeof(key));
        return hash % cardinality;
    }

    virtual ~XXHasher() = default;
};
}  // namespace Common