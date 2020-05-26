#include "gtest/gtest.h"

#include "DataGenerator/ZipfTest.hpp"
#include "NoPartitioningHashJoin/HashTableTest.hpp"

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}