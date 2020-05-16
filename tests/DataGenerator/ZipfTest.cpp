#include <map>

#include "Common/Random.hpp"
#include "DataGenerator/Zipf.hpp"
#include "gtest/gtest.h"

class ZipfGeneratorTester : public DataGenerator::Zipf {
   public:
    uint64_t generate(double alpha, uint64_t cardinality,
                            std::shared_ptr<Common::IRandomNumberGenerator> generator) {
        return DataGenerator::Zipf::generate(alpha, cardinality, generator);
    }
};

TEST(ZipfTest, TestHighSkew) {
    long seed = 123456789;
    double alpha = 0.99;
    uint64_t cardinality = 10;
    uint64_t numberOfGeneratedSamples = 10000;
    auto randomNumberGeneratorFactory =
        std::make_shared<Common::MultiplicativeLCGRandomNumberGeneratorFactory>();

    std::shared_ptr<Common::IRandomNumberGenerator> randomNumberGenerator =
        randomNumberGeneratorFactory->GetNewGenerator(seed);

    ZipfGeneratorTester zipGenerator{};

    std::map<uint64_t, uint64_t> results{};
    for (int i = 0; i != numberOfGeneratedSamples; i++) {
        uint64_t sample = zipGenerator.generate(alpha, cardinality, randomNumberGenerator);
        results[sample]++;
    }

    bool previous = false;
    std::pair<uint64_t, uint64_t> previousResult;
    for (std::pair<uint64_t, uint64_t> result : results) {
        // Check if the value is within the bounds of [1, N]
        EXPECT_LE(result.first, cardinality);
        EXPECT_GE(result.first, 1);

        if (!previous) {
            previousResult = result;
            previous = true;
        }

        // Just check if the sampling rate is lower than for the previous number
        EXPECT_GE(previousResult.second, result.second);

        previousResult = result;
    }
}
