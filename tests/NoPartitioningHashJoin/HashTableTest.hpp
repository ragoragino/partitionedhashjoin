#include <map>

#include "../../src/Common/Table.hpp"
#include "../../src/Common/XXHasher.hpp"
#include "../../src/HashTables/LinearProbing.hpp"
#include "../../src/HashTables/SeparateChaining.hpp"
#include "../../src/NoPartitioning/Configuration.hpp"
#include "gtest/gtest.h"

template <typename T>
void testInsertGetAndExists(T&& hashTable) {
    Common::Tuple tuple{
        123456789,  // id
        987654321,  // payload
    };

    hashTable.Insert(tuple.id, &tuple);

    bool hashKeyExists = hashTable.Exists(tuple.id);

    EXPECT_TRUE(hashKeyExists);

    const Common::Tuple* hashValue = hashTable.Get(tuple.id);

    EXPECT_EQ(&tuple, hashValue);
}

template <typename T>
void testIterator(T&& hashTable, size_t numberOfTuples) {
    int64_t id = 123456789;
    std::vector<Common::Tuple> input_tuples(10);
    for (size_t i = 0; i != input_tuples.size(); i++) {
        input_tuples[i] = Common::Tuple{
            id,
            static_cast<int64_t>(i),
        };

        hashTable.Insert(input_tuples[i].id, &input_tuples[i]);
    }

    std::vector<const Common::Tuple*> allTuples = hashTable.GetAll(id);

    EXPECT_EQ(input_tuples.size(), allTuples.size());
}

template <typename T>
void testMultiThreaded(T&& hashTable, size_t numberOfTuples) {
    Common::Tuple tuple{
        123456789,  // id
        987654321,  // payload
    };

    auto hashTableInserterFunc = [&hashTable, &tuple](size_t start, size_t end) {
        for (size_t i = start; i != end; i++) {
            hashTable->Insert(i, &tuple);
        }
    };

    size_t start = 0;
    size_t end = numberOfTuples;
    size_t nOfWorkers = 4;
    size_t range = (end - start) / nOfWorkers;

    std::vector<std::thread> threads{};
    for (size_t i = 0; i != nOfWorkers; i++) {
        size_t endRange = start + (i + 1) * range;
        if (i == (nOfWorkers - 1)) {
            endRange = end;
        }

        threads.emplace_back(hashTableInserterFunc, start + i * range, endRange);
    }

    for (auto&& thread : threads) {
        thread.join();
    }

    for (size_t i = 0; i != end; i++) {
        bool hashKeyExists = hashTable->Exists(i);
        EXPECT_TRUE(hashKeyExists);
    }
}

TEST(SeparateChainingTest, InsertGetAndExists) {
    size_t nOfObjects = 10;
    HashTables::SeparateChainingConfiguration configuration{
        0.3,  // HASH_TABLE_SIZE_RATIO
    };
    std::shared_ptr<Common::IHasher> hasher = std::make_shared<Common::XXHasher>();
    HashTables::SeparateChainingHashTable<Common::Tuple, 3> hashTable(configuration, hasher,
                                                                      nOfObjects);
    testInsertGetAndExists(hashTable);
}

TEST(LinearProbingTest, InsertGetAndExists) {
    size_t nOfObjects = 10;
    std::shared_ptr<Common::IHasher> hasher = std::make_shared<Common::XXHasher>();
    HashTables::LinearProbingConfiguration config{/*.HASH_TABLE_SIZE_RATIO =*/1.0 / 0.75};
    HashTables::LinearProbingHashTable<Common::Tuple, 3> hashTable(config, hasher, nOfObjects);
    testInsertGetAndExists(hashTable);
}

TEST(SeparateChainingTest, Iterator) {
    size_t nOfObjects = 10;
    HashTables::SeparateChainingConfiguration configuration{
        0.3,  // HASH_TABLE_SIZE_RATIO
    };
    std::shared_ptr<Common::IHasher> hasher = std::make_shared<Common::XXHasher>();
    HashTables::SeparateChainingHashTable<Common::Tuple, 3> hashTable(configuration, hasher,
                                                                      nOfObjects);
    testIterator(hashTable, nOfObjects);
}

TEST(LinearProbingTest, Iterator) {
    size_t nOfObjects = 10;
    std::shared_ptr<Common::IHasher> hasher = std::make_shared<Common::XXHasher>();
    HashTables::LinearProbingConfiguration config{/*.HASH_TABLE_SIZE_RATIO =*/1.0 / 0.75};
    HashTables::LinearProbingHashTable<Common::Tuple, 3> hashTable(config, hasher, nOfObjects);
    testIterator(hashTable, nOfObjects);
}

TEST(SeparateChainingTest, TestMultiThreadedInsert) {
    size_t nOfObjects = 1000;
    HashTables::SeparateChainingConfiguration configuration{
        0.1,  // HASH_TABLE_SIZE_RATIO
    };
    std::shared_ptr<Common::IHasher> hasher = std::make_shared<Common::XXHasher>();
    auto hashTable = std::make_shared<HashTables::SeparateChainingHashTable<Common::Tuple, 3>>(
        configuration, hasher, nOfObjects);
    testMultiThreaded(hashTable, nOfObjects);
}

TEST(LinearProbingTest, TestMultiThreadedInsert) {
    size_t nOfObjects = 1000;
    std::shared_ptr<Common::IHasher> hasher = std::make_shared<Common::XXHasher>();
    HashTables::LinearProbingConfiguration config{/*.HASH_TABLE_SIZE_RATIO =*/1.0 / 0.75};
    HashTables::LinearProbingHashTable<Common::Tuple, 3> hashTable(config, hasher, nOfObjects);
    testIterator(hashTable, nOfObjects);
}
