#pragma once

#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstdint>
#include <map>
#include <thread>
#include <vector>

#include "Common/IHasher.hpp"
#include "Common/Table.hpp"
#include "Common/XXHasher.hpp"

namespace HashTables {
struct LinearProbingConfiguration {
    double HASH_TABLE_SIZE_RATIO = 1.25;
};

namespace internal {
namespace LinearProbing {
template <typename Value, size_t N>
class alignas(64) Bucket {
   public:
    Bucket() : m_freePosition(0) {}

    Bucket(int64_t key, const Value* tuple) : m_freePosition(1) {
        m_keys[0] = key;
        m_values[0] = tuple;
    }

    bool Insert(int64_t key, const Value* tuple) {
        if (m_freePosition == N) {
            return false;
        }

        m_keys[m_freePosition] = key;
        m_values[m_freePosition] = tuple;

        m_freePosition++;

        return true;
    }

    bool Exists(int64_t key) const {
        size_t end = m_freePosition;

        for (size_t i = 0; i != end; i++) {
            if (m_keys[i] == key) {
                return true;
            }
        }

        return false;
    }

    bool IsFull() const { return m_freePosition == N; }

    const Value* Get(int64_t key) const {
        size_t end = m_freePosition;

        for (size_t i = 0; i != end; i++) {
            if (m_keys[i] == key) {
                return m_values[i];
            }
        }

        return nullptr;
    }

    void GetAll(int64_t key, std::vector<const Value*>& result) const {
        for (size_t i = 0; i != m_freePosition; ++i) {
            if (m_keys[i] == key) {
                result.push_back(m_values[i]);
            }
        };
    }

   private:
    int8_t m_freePosition;
    int64_t m_keys[N];
    const Value* m_values[N];
};

size_t getNumberOfBuckets(const LinearProbingConfiguration& configuration, size_t numberOfObjects);

}  // namespace LinearProbing
}  // namespace internal

template <typename BucketValueType, size_t BucketSize, typename HasherType>
class LinearProbingHashTable {
    using Bucket = internal::LinearProbing::Bucket<BucketValueType, BucketSize>;

   public:
    LinearProbingHashTable(const LinearProbingConfiguration& configuration,
                           const HasherType& hasher, size_t numberOfObjects)
        : m_configuration(configuration),
          m_numberOfBuckets(
              internal::LinearProbing::getNumberOfBuckets(configuration, numberOfObjects)),
          m_buckets(m_numberOfBuckets, Bucket{}),
          m_bucketLatches(m_numberOfBuckets),
          m_hasher(hasher) {
        std::for_each(m_bucketLatches.begin(), m_bucketLatches.end(),
                      [](std::atomic_flag& latch) { latch.clear(); });

        if (numberOfObjects <= 0) {
            throw std::invalid_argument(
                "LinearProbingHashTable::LinearProbingHashTable: numberOfObjects must be greater "
                "than zero.");
        }
    }

    // thread-safe
    void Insert(int64_t key, const BucketValueType* tuple) {
        uint64_t hash = m_hasher.Hash(key, m_numberOfBuckets);

        bool insertSucceeded = false;
        while (true) {
            // Spin on latch until you succeed in locking it
            while (m_bucketLatches[hash].test_and_set(std::memory_order_acquire))
                ;

            insertSucceeded = m_buckets[hash].Insert(key, tuple);

            // Unlock latch
            m_bucketLatches[hash].clear(std::memory_order_release);

            if (insertSucceeded) {
                break;
            }

            hash = ++hash == m_numberOfBuckets ? 0 : hash;
        }
    }

    // not thread-safe - we shouldn't need to run Exists during building of hash index
    bool Exists(int64_t key) {
        uint64_t hash = m_hasher.Hash(key, m_numberOfBuckets);

        bool exists = false;
        const Bucket* bucketPtr;
        while (true) {
            bucketPtr = &m_buckets[hash];
            exists = bucketPtr->Exists(key);
            if (exists) {
                return true;
            }

            if (!bucketPtr->IsFull()) {
                return false;
            }

            hash = ++hash == m_numberOfBuckets ? 0 : hash;
        }

        return false;
    }

    // not thread-safe - we shouldn't need to run Get during building of hash index
    const BucketValueType* Get(int64_t key) {
        uint64_t hash = m_hasher.Hash(key, m_numberOfBuckets);

        const BucketValueType* bucketValue = nullptr;
        const Bucket* bucketPtr;
        while (true) {
            bucketPtr = &m_buckets[hash];
            bucketValue = bucketPtr->Get(key);
            if (bucketValue) {
                return bucketValue;
            }

            if (!bucketPtr->IsFull()) {
                return bucketValue;
            }

            hash = ++hash == m_numberOfBuckets ? 0 : hash;
        }

        return bucketValue;
    }

    // not thread-safe - we shouldn't need to run GetAll during building of hash index
    std::vector<const BucketValueType*> GetAll(int64_t key) {
        uint64_t hash = m_hasher.Hash(key, m_numberOfBuckets);

        bool exists = false;
        std::vector<const BucketValueType*> bucketValues{};
        const Bucket* bucketPtr;
        while (true) {
            bucketPtr = &m_buckets[hash];
            bucketPtr->GetAll(key, bucketValues);
            if (!bucketPtr->IsFull()) {
                return bucketValues;
            }

            hash = ++hash == m_numberOfBuckets ? 0 : hash;
        }

        return bucketValues;
    }

    ~LinearProbingHashTable() = default;

   private:
    const LinearProbingConfiguration m_configuration;
    const size_t m_numberOfBuckets;
    std::vector<Bucket> m_buckets;
    std::vector<std::atomic_flag> m_bucketLatches;
    HasherType m_hasher;
};

template <typename BucketValueType, size_t BucketSize, typename HasherType>
class LinearProbingFactory {
   public:
    typedef LinearProbingHashTable<BucketValueType, BucketSize, HasherType> HashTableType;

    LinearProbingFactory(const LinearProbingConfiguration& configuration, HasherType hasher)
        : m_hasher(hasher), m_configuration(configuration) {}

    std::shared_ptr<HashTableType> New(size_t numberOfObjects) const {
        return std::make_shared<HashTableType>(m_configuration, m_hasher, numberOfObjects);
    }

   private:
    const HasherType m_hasher;
    const LinearProbingConfiguration m_configuration;
};

}  // namespace HashTables
