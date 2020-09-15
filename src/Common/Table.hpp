#pragma once

#include <cstdint>
#include <iostream>
#include <new>
#include <string>
#include <vector>

#ifndef CACHE_LINE_SIZE
#define CACHE_LINE_SIZE std::hardware_destructive_interference_size
#endif

namespace Common {
std::string generate_uuid();

struct alignas(16) Tuple {
    int64_t id;
    int64_t payload;

    friend std::ostream &operator<<(std::ostream &out, const Tuple &tuple);
};

struct JoinedTuple {
    int64_t id;
    int64_t payloadA;
    int64_t payloadB;

    friend std::ostream &operator<<(std::ostream &out, const JoinedTuple &tuple);
};

template <typename TupleType>
class Table {
   public:
    Table(std::string id) : m_id(id){};

    Table(size_t size, std::string id) : m_tuples(size), m_id(id) {}

    TupleType &operator[](size_t index) { return m_tuples[index]; }

    const TupleType &operator[](size_t index) const { return m_tuples[index]; }

    size_t GetSize() const { return m_tuples.size(); }

    size_t GetCapacity() const { return m_tuples.capacity(); }

    std::string GetID() const { return m_id; }

   private:
    const std::string m_id;
    // To minimize the posobility of false sharing in cache, we align the vector on
    // cache line size
    alignas(CACHE_LINE_SIZE) std::vector<TupleType> m_tuples;
};
}  // namespace Common
