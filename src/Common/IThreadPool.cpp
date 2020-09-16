#include "IThreadPool.hpp"

namespace Common {
bool TasksErrorHolder::Empty() { return m_exceptions.empty(); }

void TasksErrorHolder::Push(std::exception& e) { m_exceptions.push_back(e); }

std::exception TasksErrorHolder::Pop() {
    if (m_exceptions.empty()) {
        throw std::out_of_range(
            "TasksErrorHolder::Pop: No exception has been pushed to the error holder.");
    }

    std::exception e = m_exceptions.back();
    m_exceptions.pop_back();
    return e;
}

std::string TasksErrorHolder::Concatenate() {
    std::string error;
    std::for_each(m_exceptions.begin(), m_exceptions.end(), [&error](const std::exception& e) {
        std::string message = e.what();
        error += message + "; ";
    });

    return error;
}

}  // namespace Common
