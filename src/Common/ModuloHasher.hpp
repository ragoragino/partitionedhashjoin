#pragma once

#include "IHasher.hpp"

namespace Common {
class ModuloHasher : public IHasher {
   public:
    virtual uint64_t Hash(int64_t key, size_t cardinality) override {
        return key % cardinality;
    }

    virtual ~ModuloHasher() = default;
};
}  // namespace Common