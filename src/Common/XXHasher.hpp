#pragma once

#include "IHasher.hpp"

#include "xxh3.h"

#include <limits>
#include <random>

namespace Common {
class XXHasher : public IHasher {
   public:
    XXHasher() {
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<uint64_t> dist(0, std::numeric_limits<uint64_t>::max());
        m_seed = dist(gen);
    };

    virtual uint64_t Hash(int64_t key, size_t cardinality) override {
        auto hash = XXH3_64bits_withSeed(static_cast<const void*>(&key), sizeof(key), m_seed);
        return hash % cardinality;
    }

    virtual ~XXHasher() = default;

    private:
        uint64_t m_seed;
};
}  // namespace Common