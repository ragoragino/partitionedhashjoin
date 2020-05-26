#include <map>

#include "Common/Table.hpp"
#include "Common/XXHasher.hpp"
#include "NoPartitioningHashJoin/HashTable.hpp"
#include "gtest/gtest.h"

TEST(HashTableTest, InsertGetAndExists) {
    std::shared_ptr<Common::IHasher> hasher = std::make_shared<Common::XXHasher>();
    auto hashTable = std::make_shared<NoPartioniningHashJoin::HashTable<Common::Tuple>>(hasher, 10);

    Common::Tuple tuple{
        123456789,  // id
        987654321,  // payload
    };

    hashTable->Insert(tuple.id, &tuple);

    bool hashKeyExists = hashTable->Exists(tuple.id);

    EXPECT_TRUE(hashKeyExists);

    Common::Tuple* hashValue = hashTable->Get(tuple.id);

    EXPECT_EQ(&tuple, hashValue);
}

TEST(HashTableTest, TestMultiThreadedInsert) {
    std::shared_ptr<Common::IHasher> hasher = std::make_shared<Common::XXHasher>();
    auto hashTable =
        std::make_shared<NoPartioniningHashJoin::HashTable<Common::Tuple>>(hasher, 100000);

    Common::Tuple tuple{
        123456789,  // id
        987654321,  // payload
    };

    auto hashTableInserterFunc = [&hashTable, &tuple](int start, int end) {
        for (int i = start; i != end; i++) {
            hashTable->Insert(i, &tuple);
        }
    };

    int start = 0;
    int workerRange = 10000;
    int nOfWorkers = 10;
    int end = start + nOfWorkers * workerRange;

    std::vector<std::thread> threads{};
    for (int i = 0; i != nOfWorkers; i++) {
        threads.emplace_back(hashTableInserterFunc, start + i * workerRange,
                             start + (i + 1) * workerRange);
    }

    for (auto&& thread : threads) {
        thread.join();
    }

    for (int i = 0; i != end; i++) {
        bool hashKeyExists = hashTable->Exists(i);
        EXPECT_TRUE(hashKeyExists);
    }
}
