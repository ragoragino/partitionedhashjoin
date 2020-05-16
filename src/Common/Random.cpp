#include "Random.hpp"

#include <random>

namespace Common {
MultiplicativeLCGRandomNumberGenerator::MultiplicativeLCGRandomNumberGenerator(long seed)
    : m_state(seed){};

double MultiplicativeLCGRandomNumberGenerator::Next() {
    const long a = 16807;       // Multiplier
    const long m = 2147483647;  // Modulus
    const long q = 127773;      // m div a
    const long r = 2836;        // m mod a
    long x_div_q;               // x divided by q
    long x_mod_q;               // x modulo q
    long x_new;                 // New x value

    // RNG using integer arithmetic
    x_div_q = m_state / q;
    x_mod_q = m_state % q;
    x_new = (a * x_mod_q) - (r * x_div_q);
    if (x_new > 0) {
        m_state = x_new;
    } else {
        m_state = x_new + m;
    }

    // Return a random value between 0.0 and 1.0
    return static_cast<double>(m_state) / static_cast<double>(m);
}

std::shared_ptr<IRandomNumberGenerator>
MultiplicativeLCGRandomNumberGeneratorFactory::GetNewGenerator() {
    std::random_device rd;
    return std::make_shared<MultiplicativeLCGRandomNumberGenerator>(rd());
}

std::shared_ptr<IRandomNumberGenerator>
MultiplicativeLCGRandomNumberGeneratorFactory::GetNewGenerator(long seed) {
    return std::make_shared<MultiplicativeLCGRandomNumberGenerator>(seed);
}
}  // namespace Common
