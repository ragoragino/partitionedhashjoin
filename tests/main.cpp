#ifdef PHJ_USE_MIMALLOC
#include <mimalloc.h>
#endif

#include "DataGenerator/ZipfTest.hpp"
#include "NoPartitioningHashJoin/HashTableTest.hpp"
#include "gtest/gtest.h"

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}