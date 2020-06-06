#include "Table.hpp"

namespace Common {
std::ostream &operator<<(std::ostream &out, const Tuple &tuple) {
    out << tuple.id << ", " << tuple.payload;
    return out;
}

std::ostream &operator<<(std::ostream &out, const JoinedTuple &tuple) {
    out << tuple.id << ", " << tuple.payloadA << ", " << tuple.payloadB;
    return out;
}
}  // namespace Common
