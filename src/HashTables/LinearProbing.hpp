#pragma once

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
    const double HASH_TABLE_SIZE_RATIO;
    const size_t HASH_TABLE_SIZE_LIMIT;
};

namespace internal {
namespace LinearProbing {
template <typename Value, size_t N>
class alignas(64) Bucket {
   public:
    explicit Bucket() : m_freePosition(0) {}

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

    void SetNext(Bucket<Value, N>* nextBucket) { m_nextBucket = nextBucket; };

    Bucket<Value, N>* Next() const { return m_nextBucket; }

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

size_t getNumberOfBuckets(const LinearProbingConfiguration& configuration, size_t numberOfObjects) {
    size_t numberOfBuckets = static_cast<size_t>(
        ceil(configuration.HASH_TABLE_SIZE_RATIO * static_cast<double>(numberOfObjects)));

    if (numberOfBuckets < 1) {
        throw std::runtime_error(
            "Cannot allocate less than 1 bucket for a linear probing hash table.");
    }

    return numberOfBuckets;
};

}  // namespace LinearProbing
}  // namespace internal

template <typename BucketValueType, size_t BucketSize>
class LinearProbingHashTable {
    using Bucket = internal::LinearProbing::Bucket<BucketValueType, BucketSize>;

   public:
    LinearProbingHashTable(const LinearProbingConfiguration& configuration,
                           std::shared_ptr<Common::IHasher> hasher, size_t numberOfObjects)
        : m_hasher(hasher),
          m_configuration(configuration),
          m_numberOfBuckets(
              internal::LinearProbing::getNumberOfBuckets(configuration, numberOfObjects)),
          m_buckets(m_numberOfBuckets, Bucket{}),
          m_bucketLatches(m_numberOfBuckets) {
        std::for_each(m_bucketLatches.begin(), m_bucketLatches.end(),
                      [](std::atomic_flag& latch) { latch.clear(); });

        if (numberOfObjects <= 0) {
            throw std::invalid_argument("numberOfObjects must be greater than zero.");
        }
    }

    // thread-safe
    void Insert(int64_t key, const BucketValueType* tuple) {
        uint32_t hash = m_hasher->Hash(key, m_numberOfBuckets);

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
        uint32_t hash = m_hasher->Hash(key, m_numberOfBuckets);

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
    const Common::Tuple* Get(int64_t key) {
        uint32_t hash = m_hasher->Hash(key, m_numberOfBuckets);

        bool exists = false;
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
        uint32_t hash = m_hasher->Hash(key, m_numberOfBuckets);

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
    std::vector<std::atomic_flag> m_bucketLatches;
    std::vector<Bucket> m_buckets;
    std::shared_ptr<Common::IHasher> m_hasher;
};
}  // namespace HashTables
