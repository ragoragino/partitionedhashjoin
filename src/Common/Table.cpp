#include "Table.hpp"

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

namespace Common {
std::ostream &operator<<(std::ostream &out, const Tuple &tuple) {
    out << tuple.id << ", " << tuple.payload;
    return out;
}

std::ostream &operator<<(std::ostream &out, const JoinedTuple &tuple) {
    out << tuple.id << ", " << tuple.payloadA << ", " << tuple.payloadB;
    return out;
}

std::string generate_uuid() {
    static boost::uuids::random_generator gen;
    boost::uuids::uuid id = gen();
    return boost::uuids::to_string(id);
}

}  // namespace Common
