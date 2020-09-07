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
struct SeparateChainingConfiguration {
    double HASH_TABLE_SIZE_RATIO = 0.25;
};

namespace internal {
namespace SeparateChaining {
template <typename Value, size_t N>
class alignas(64) Bucket {
   public:
    explicit Bucket(Bucket<Value, N>* oldBucket) : m_nextBucket(oldBucket), m_freePosition(0) {}

    Bucket(Bucket<Value, N>* oldBucket, int64_t key, const Value* tuple)
        : m_nextBucket(oldBucket), m_freePosition(1) {
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

    std::vector<const Value*> GetAll(int64_t key) const {
        std::vector<const Value*> result{};

        size_t currentPosition = 0;
        const Bucket<Value, N>* currenBucket = this;
        while (currenBucket != nullptr) {
            if (currentPosition == currenBucket->m_freePosition) {
                currenBucket = currenBucket->Next();
                currentPosition = 0;
                continue;
            }

            if (currenBucket->m_keys[currentPosition] == key) {
                result.push_back(currenBucket->m_values[currentPosition]);
            }

            currentPosition++;
        }

        return result;
    }

   private:
    Bucket<Value, N>* m_nextBucket;
    int8_t m_freePosition;
    int64_t m_keys[N];
    const Value* m_values[N];
};

template <typename T>
class BucketAllocator {
   public:
    template <typename... Fs>
    BucketAllocator() : m_currentIndex{0} {}

    template <typename... Fs>
    BucketAllocator(size_t numberOfObjects, Fs... fs)
        : m_currentIndex{0}, m_objects(numberOfObjects, T(fs...)) {}

    T* New() {
        size_t oldIndex = m_currentIndex++;

        if (oldIndex >= m_objects.size()) {
            throw std::runtime_error("BucketAllocator exceeded its limit.");
        }

        return &m_objects[oldIndex];
    }

    BucketAllocator<T>& operator=(BucketAllocator<T>&& other) {
        m_objects = std::move(other.m_objects);

        size_t otherValue = other.m_currentIndex.load();
        m_currentIndex.store(otherValue);

        return *this;
    }

   private:
    std::vector<T> m_objects;
    std::atomic<size_t> m_currentIndex;
};

size_t getNumberOfBuckets(const SeparateChainingConfiguration& configuration,
                          size_t numberOfObjects);

}  // namespace SeparateChaining
}  // namespace internal

template <typename BucketValueType, size_t BucketSize, typename HasherType>
class SeparateChainingHashTable {
    using Bucket = internal::SeparateChaining::Bucket<BucketValueType, BucketSize>;
    using BucketAllocator = internal::SeparateChaining::BucketAllocator<Bucket>;

   public:
    SeparateChainingHashTable(const SeparateChainingConfiguration& configuration, HasherType hasher,
                              size_t numberOfObjects)
        : m_hasher(hasher),
          m_configuration(configuration),
          m_numberOfBuckets(
              internal::SeparateChaining::getNumberOfBuckets(configuration, numberOfObjects)),
          m_bucketPtrs(m_numberOfBuckets),
          m_firstBuckets(m_numberOfBuckets,
                         Bucket(nullptr)),  // TODO: Maybe use only bucket allocator
          m_bucketPtrsLatches(m_numberOfBuckets) {
        std::for_each(m_bucketPtrsLatches.begin(), m_bucketPtrsLatches.end(),
                      [](std::atomic_flag& latch) { latch.clear(); });

        if (m_numberOfBuckets <= 0) {
            throw std::invalid_argument("numberOfBuckets must be greater than zero.");
        } else if (numberOfObjects <= 0) {
            throw std::invalid_argument("numberOfObjects must be greater than zero.");
        }

        size_t bucketAllocatorBufferSize = static_cast<size_t>(
            ceil(static_cast<double>(numberOfObjects) / static_cast<double>(BucketSize)));

        if (bucketAllocatorBufferSize > 1) {
            m_bucketAllocator = BucketAllocator(bucketAllocatorBufferSize, nullptr);
        }
    }

    // thread-safe
    void Insert(int64_t key, const BucketValueType* tuple) {
        uint64_t hash = m_hasher.Hash(key, m_numberOfBuckets);

        // Spin on latch until you succeed in locking it
        while (m_bucketPtrsLatches[hash].test_and_set(std::memory_order_acquire))
            ;

        // Insert the key
        // If the bucket was never allocated, it will be null and just make the pointer
        // point to the preallocated buffer. If it is not null, add to the existing bucket
        if (m_bucketPtrs[hash] == nullptr) {
            m_bucketPtrs[hash] = &m_firstBuckets[hash];
            bool insertSucceeded = m_bucketPtrs[hash]->Insert(key, tuple);

            // This should never fail as this is a newly allocated bucket
            if (!insertSucceeded) {
                throw std::runtime_error("Unable to insert key to a newly allocated bucket!");
            }
        } else {
            bool insertSucceeded = m_bucketPtrs[hash]->Insert(key, tuple);

            // In case insert failed, get new bucket from bucker allocator
            if (!insertSucceeded) {
                internal::SeparateChaining::Bucket<BucketValueType, BucketSize>* oldBucket =
                    m_bucketPtrs[hash];
                m_bucketPtrs[hash] = m_bucketAllocator.New();
                m_bucketPtrs[hash]->SetNext(oldBucket);
                insertSucceeded = m_bucketPtrs[hash]->Insert(key, tuple);

                // This should also never fail as this is a newly allocated bucket
                if (!insertSucceeded) {
                    throw std::runtime_error("Unable to insert key to a newly allocated bucket!");
                }
            }
        }

        // Unlock latch
        m_bucketPtrsLatches[hash].clear(std::memory_order_release);
    }

    // not thread-safe - we shouldn't need to run Exists during building of hash index
    bool Exists(int64_t key) {
        uint64_t hash = m_hasher.Hash(key, m_numberOfBuckets);

        if (m_bucketPtrs[hash] == nullptr) {
            return false;
        }

        const Bucket* bucketPtr = m_bucketPtrs[hash];
        while (bucketPtr != nullptr) {
            if (bucketPtr->Exists(key)) {
                return true;
            }

            bucketPtr = bucketPtr->Next();
        }

        return false;
    }

    // not thread-safe - we shouldn't need to run Get during building of hash index
    const BucketValueType* Get(int64_t key) {
        uint64_t hash = m_hasher.Hash(key, m_numberOfBuckets);

        if (m_bucketPtrs[hash] == nullptr) {
            return false;
        }

        const Bucket* bucketPtr = m_bucketPtrs[hash];
        while (bucketPtr != nullptr) {
            const BucketValueType* tuple = bucketPtr->Get(key);
            if (tuple != nullptr) {
                return tuple;
            }

            bucketPtr = bucketPtr->Next();
        }

        return false;
    }

    // not thread-safe - we shouldn't need to run GetAll during building of hash index
    std::vector<const BucketValueType*> GetAll(int64_t key) {
        uint64_t hash = m_hasher.Hash(key, m_numberOfBuckets);

        if (m_bucketPtrs[hash] == nullptr) {
            return std::vector<const BucketValueType*>{};
        }

        return m_bucketPtrs[hash]->GetAll(key);
    }

    ~SeparateChainingHashTable() = default;

   private:
    const size_t m_numberOfBuckets;
    std::vector<Bucket*> m_bucketPtrs;
    std::vector<std::atomic_flag> m_bucketPtrsLatches;
    std::vector<Bucket> m_firstBuckets;
    HasherType m_hasher;
    BucketAllocator m_bucketAllocator;
    const SeparateChainingConfiguration m_configuration;
};

template <typename BucketValueType, size_t BucketSize, typename HasherType>
class SeparateChainingFactory {
   public:
    typedef SeparateChainingHashTable<BucketValueType, BucketSize, HasherType> HashTableType;

    SeparateChainingFactory(const SeparateChainingConfiguration& configuration, HasherType hasher)
        : m_configuration(configuration), m_hasher(hasher) {}

    std::shared_ptr<HashTableType> New(size_t numberOfObjects) const {
        return std::make_shared<HashTableType>(m_configuration, m_hasher, numberOfObjects);
    }

   private:
    const HasherType m_hasher;
    const SeparateChainingConfiguration m_configuration;
};

}  // namespace HashTables
