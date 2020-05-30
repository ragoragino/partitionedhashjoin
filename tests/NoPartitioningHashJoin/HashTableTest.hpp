#include <map>

#include "Common/Table.hpp"
#include "Common/XXHasher.hpp"
#include "NoPartitioning/HashTable.hpp"
#include "gtest/gtest.h"

TEST(HashTableTest, InsertGetAndExists) {
    std::shared_ptr<Common::IHasher> hasher = std::make_shared<Common::XXHasher>();
    auto hashTable = std::make_shared<NoPartitioning::HashTable<Common::Tuple>>(hasher, 10);

    Common::Tuple tuple{
        123456789,  // id
        987654321,  // payload
    };

    hashTable->Insert(tuple.id, &tuple);

    bool hashKeyExists = hashTable->Exists(tuple.id);

    EXPECT_TRUE(hashKeyExists);

    const Common::Tuple* hashValue = hashTable->Get(tuple.id);

    EXPECT_EQ(&tuple, hashValue);
}

TEST(HashTableTest, TestMultiThreadedInsert) {
    std::shared_ptr<Common::IHasher> hasher = std::make_shared<Common::XXHasher>();
    auto hashTable = std::make_shared<NoPartitioning::HashTable<Common::Tuple>>(hasher, 100000);

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
    int end = 1000000;
    int nOfWorkers = 4;
    int range = (end - start) / nOfWorkers;

    std::vector<std::thread> threads{};
    for (int i = 0; i != nOfWorkers; i++) {
        int endRange = start + (i + 1) * range;
        if (i == (nOfWorkers - 1)) {
            endRange = end;
        }

        threads.emplace_back(hashTableInserterFunc, start + i * range, endRange);
    }

    for (auto&& thread : threads) {
        thread.join();
    }

    for (int i = 0; i != end; i++) {
        bool hashKeyExists = hashTable->Exists(i);
        EXPECT_TRUE(hashKeyExists);
    }
}
