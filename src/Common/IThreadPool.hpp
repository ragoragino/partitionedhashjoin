#pragma once

#include <cstdint>
#include <functional>
#include <future>
#include <vector>

namespace Common {
class IThreadPool {
   public:
    virtual std::future<void> Push(std::function<void()>&& f) = 0;

    virtual std::future<void> Push(std::vector<std::function<void()>>&& f) = 0;

    virtual size_t GetNumberOfWorkers() const = 0;

    virtual void Stop() = 0;

    virtual ~IThreadPool() = default;
};
}  // namespace Common
