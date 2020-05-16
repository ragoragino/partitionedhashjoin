#include "Table.hpp"

namespace Common {
Table::Table(){};

Table::Table(size_t size) : m_tuples(size) {}

Tuple& Table::operator[](size_t index) { return m_tuples[index]; }

const Tuple& Table::operator[](size_t index) const { return m_tuples[index]; }

size_t Table::GetSize() { return m_tuples.size(); }

std::ostream& operator<<(std::ostream& out, const Tuple& tuple) {
    out << tuple.id << ", " << tuple.payload;
    return out;
}
}  // namespace Common
