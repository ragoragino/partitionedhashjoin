#pragma once
#include <atomic>
#include <functional>
#include <future>
#include <memory>
#include <queue>
#include <vector>
#include <atomic>

#include "IThreadPool.hpp"
#include "Logger.hpp"

namespace Common {

namespace internal {
// WorkPipe is responsible for holding a queue of tasks
// and pushing them to workers
class WorkPipe {
   public:
    WorkPipe();

    std::future<void> Push(std::function<void()>&& f);

    std::future<void> Push(std::vector<std::function<void()>>&& f);

    // Workers call this to wait for the signal whether to quit,
    // and if signal is negative, they receive a new task.
    // Workers should continue working if there is still some work to do
    // even after the call to Stop (however, no Push calls can be executed after calling Stop).
    // Only after all work is done, workers will receive stop signal.
    std::tuple<bool, std::function<void()>> Wait(size_t id);

    // Calling stop will signal to the work pipe that it should not allow
    // enqueueing any new tasks. The call will block until all tasks that
    // were enqueued before calling Stop are finished.
    void Stop();

   private:
    std::condition_variable m_condition_variable;
    std::queue<std::function<void()>> m_global_workqueue;
    std::mutex m_global_workqueue_mutex;
    bool m_stopped; // it also protected by m_global_workqueue
};

// Worker is the owner of a thread and executes tasks from WorkPipe
class Worker {
   public:
    Worker() = default;

    void Start(std::shared_ptr<WorkPipe> workPipe);

    void WaitForFinish();

   private:
    void Run(std::shared_ptr<WorkPipe> workPipe);

    std::thread m_thread;
    std::mutex m_threadMutex;
};

// WorkManager is a helper class to unify multiple tasks into a one logical package,
// in order to provide the functionality of notifying clients only after all of the component
// tasks execute. WorkManager is not thread-safe. GetTasks and GetFuture should be called only once.
class WorkManager : public std::enable_shared_from_this<WorkManager> {
   public:
    WorkManager(std::vector<std::function<void()>>&& funcs);

    // Can be called only once, otherwise throws std::invalid_argument
    std::vector<std::function<void()>> GetTasks();

    // Can be called only once, otherwise throws std::future_error
    std::future<void> GetFuture();

   private:
    void finished();

    std::atomic_size_t m_counter;
    std::promise<void> m_promise;
    std::vector<std::function<void()>> m_work;
};
}  // namespace internal

class ThreadPool : public IThreadPool {
   public:
    ThreadPool(size_t numberOfWorkers);

    virtual std::future<void> Push(std::function<void()>&& f) override;

    virtual std::future<void> Push(std::vector<std::function<void()>>&& f) override;

    virtual size_t GetNumberOfWorkers() const override;

    virtual void Stop() override;

    virtual ~ThreadPool() override = default;

   private:
    std::shared_ptr<internal::WorkPipe> m_workPipe;
    std::vector<internal::Worker> m_workers;
};
}  // namespace Common
