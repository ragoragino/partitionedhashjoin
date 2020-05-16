#pragma once

#include <cstdint>
#include <new>
#include <vector>
#include <iostream>

#ifndef CACHE_LINE_SIZE
#define CACHE_LINE_SIZE std::hardware_destructive_interference_size
#endif

namespace Common {
struct alignas(16) Tuple {
    int64_t id;
    int64_t payload;

    friend std::ostream &operator<<(std::ostream &out, const Tuple &tuple);
};



class Table {
   public:
    Table();

    Table(size_t size);

    Tuple& operator[](size_t index);

    const Tuple& operator[](size_t index) const;

    size_t GetSize();

   private:
    // To minimize the posobility of false sharing in cache, we align the vector on
    // cache line size
    alignas(CACHE_LINE_SIZE) std::vector<Tuple> m_tuples;
};
}  // namespace Common
