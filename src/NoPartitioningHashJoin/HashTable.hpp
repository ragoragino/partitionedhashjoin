#pragma once

#include <atomic>
#include <cstdint>
#include <map>
#include <thread>
#include <vector>

#include "Common/IHasher.hpp"
#include "Common/Table.hpp"
#include "Common/XXHasher.hpp"

namespace NoPartioniningHashJoin {
namespace internal {
template <typename Value, size_t N = 3>
class alignas(64) Bucket {
   public:
    explicit Bucket(Bucket<Value, N>* oldBucket)
        : m_nextBucket(oldBucket), m_freePosition(0) {}

    Bucket(Bucket<Value, N>* oldBucket, int64_t key, Value* tuple)
        : m_nextBucket(oldBucket), m_freePosition(1) {
        m_keys[0] = key;
        m_values[0] = tuple;
    }

    bool Insert(int64_t key, Value* tuple) {
        if (m_freePosition == N) {
            return false;
        }

        m_keys[m_freePosition] = key;
        m_values[m_freePosition] = tuple;

        m_freePosition++;

        return true;
    }

    bool Exists(int64_t key) {
        size_t end = m_freePosition;

        for (size_t i = 0; i != end; i++) {
            if (m_keys[i] == key) {
                return true;
            }
        }

        return false;
    }

    Value* Get(int64_t key) {
        size_t end = m_freePosition;

        for (size_t i = 0; i != end; i++) {
            if (m_keys[i] == key) {
                return m_values[i];
            }
        }

        return nullptr;
    }

    Bucket<Value, N>* Next() { return m_nextBucket; }

   private:
    Bucket<Value, N>* m_nextBucket;
    int8_t m_freePosition;
    int64_t m_keys[N];
    Value* m_values[N];
};
}  // namespace internal

template <typename BucketValueType>
class HashTable {
   public:
    HashTable(std::shared_ptr<Common::IHasher> hasher, size_t numberOfBuckets)
        : m_hasher(hasher),
          m_numberOfBuckets(numberOfBuckets),
          m_bucketPtrs(numberOfBuckets),
          m_bucketPtrsLatches(numberOfBuckets) {
        std::for_each(m_bucketPtrsLatches.begin(), m_bucketPtrsLatches.end(),
                      [](std::atomic_flag& latch) { latch.clear(); });
    }

    // thread-safe
    void Insert(int64_t key, BucketValueType* tuple) {
        // get hash key
        uint32_t hash = m_hasher->Hash(key, m_numberOfBuckets);

        // Spin on latch until you succeed in locking it
        while (m_bucketPtrsLatches[hash].test_and_set(std::memory_order_acquire))
            ;

        if (m_bucketPtrs[hash] == nullptr) {
            // TODO: optimize memory allocation - Fucking jemalloc and tcmalloc are broken on
            // Windows
            m_bucketPtrs[hash] =
                new internal::Bucket<BucketValueType, m_bucketSize>(nullptr, key, tuple);
        } else {
            bool insertSucceeded = m_bucketPtrs[hash]->Insert(key, tuple);
            if (insertSucceeded) {
                // TODO: optimize memory allocation - Fucking jemalloc and tcmalloc are broken on
                // Windows
                internal::Bucket<BucketValueType, m_bucketSize>* bucket =
                    new internal::Bucket<BucketValueType, m_bucketSize>(m_bucketPtrs[hash], key,
                                                                        tuple);
                m_bucketPtrs[hash] = bucket;
            }
        }

        // Unlock latch
        m_bucketPtrsLatches[hash].clear(std::memory_order_release);
    }

    // not thread-safe - we shouldn't need to run Exists during building of hash index
    bool Exists(int64_t key) {
        // get hash key
        uint32_t hash = m_hasher->Hash(key, m_numberOfBuckets);

        if (m_bucketPtrs[hash] == nullptr) {
            return false;
        }

        internal::Bucket<BucketValueType, m_bucketSize>* bucketPtr = m_bucketPtrs[hash];
        while (bucketPtr != nullptr) {
            if (bucketPtr->Exists(key)) {
                return true;
            }

            bucketPtr = bucketPtr->Next();
        }

        return false;
    }

    // not thread-safe - we shouldn't need to run Get during building of hash index
    Common::Tuple* Get(int64_t key) {
        // get hash key
        uint32_t hash = m_hasher->Hash(key, m_numberOfBuckets);

        if (m_bucketPtrs[hash] == nullptr) {
            return false;
        }

        internal::Bucket<BucketValueType, m_bucketSize>* bucketPtr = m_bucketPtrs[hash];
        while (bucketPtr != nullptr) {
            Common::Tuple* tuple = bucketPtr->Get(key);
            if (tuple != nullptr) {
                return tuple;
            }

            bucketPtr = bucketPtr->Next();
        }

        return false;
    }

    ~HashTable() = default;

   private:
    static constexpr size_t m_bucketSize = 3;
    const size_t m_numberOfBuckets;
    std::vector<internal::Bucket<BucketValueType, m_bucketSize>*> m_bucketPtrs;
    std::vector<std::atomic_flag>
        m_bucketPtrsLatches;  // TODO: Maybe merge latches with buckets themselves?
    std::shared_ptr<Common::IHasher> m_hasher;
};
}  // namespace NoPartioniningHashJoin
