#pragma once

#include <memory>

namespace Common {
class IRandomNumberGenerator {
   public:
    virtual double Next() = 0;

    virtual ~IRandomNumberGenerator() = default;
};

class IRandomNumberGeneratorFactory {
   public:
    virtual std::shared_ptr<IRandomNumberGenerator> GetNewGenerator() = 0;

    virtual std::shared_ptr<IRandomNumberGenerator> GetNewGenerator(long seed) = 0;

    virtual ~IRandomNumberGeneratorFactory() = default;
};

// A multiplicative LCG pseudo random number generator
// Generate random doubles in 0-1 range. Is not thread-safe.
// https://www.csee.usf.edu/~kchriste/tools/genzipf.c
class MultiplicativeLCGRandomNumberGenerator : public IRandomNumberGenerator {
   public:
    explicit MultiplicativeLCGRandomNumberGenerator(long seed);

    virtual double Next() override;

    virtual ~MultiplicativeLCGRandomNumberGenerator() = default;

   private:
    long m_state;
};

class MultiplicativeLCGRandomNumberGeneratorFactory : public IRandomNumberGeneratorFactory {
   public:
    virtual std::shared_ptr<IRandomNumberGenerator> GetNewGenerator() override;

    virtual std::shared_ptr<IRandomNumberGenerator> GetNewGenerator(long seed) override;
};

}  // namespace Common
