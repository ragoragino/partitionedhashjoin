#pragma once

#include <cstdint>
#include <functional>
#include <future>
#include <vector>

namespace Common {
class TasksErrorHolder {
   public:
    void Push(std::exception& e);

    std::exception Pop();

    std::string Concatenate();

    bool Empty();

   private:
    std::vector<std::exception> m_exceptions;
};

// Pipeline interface can only handle dependencies represented as a unary tree
// TODO: Pipeline might be refactored to support more complicated graph types of dependencies
class IPipeline {
   public:
    virtual size_t Add(std::vector<std::function<void()>>&& f) = 0;

    virtual std::vector<std::function<void()>> Next() = 0;

    virtual std::future<TasksErrorHolder> Start() = 0;

    virtual ~IPipeline() = default;
};

class IThreadPool {
   public:
    virtual std::future<TasksErrorHolder> Push(std::function<void()>&& f) = 0;

    virtual std::future<TasksErrorHolder> Push(std::vector<std::function<void()>>&& f) = 0;

    virtual std::future<TasksErrorHolder> Push(std::shared_ptr<IPipeline> pipeline) = 0;

    virtual size_t GetNumberOfWorkers() const = 0;

    virtual void Stop() = 0;

    virtual ~IThreadPool() = default;
};
}  // namespace Common
