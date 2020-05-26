#include "ThreadPool.hpp"

#include "Common/Logger.hpp"

namespace Common {
ThreadPool::ThreadPool(size_t numberOfWorkers)
    : m_workers(numberOfWorkers), m_workPipe(std::make_shared<internal::WorkPipe>()) {
    std::for_each(m_workers.begin(), m_workers.end(),
                  [this](internal::Worker& worker) { worker.Start(this->m_workPipe); });
}

std::future<void> ThreadPool::Push(std::function<void()>&& f) {
    return m_workPipe->Push(std::move(f));
}

std::future<void> ThreadPool::Push(std::vector<std::function<void()>>&& f) {
    return m_workPipe->Push(std::move(f));
}

size_t ThreadPool::GetNumberOfWorkers() const { return m_workers.size(); }

void ThreadPool::Stop() {
    m_workPipe->Stop();

    std::for_each(m_workers.begin(), m_workers.end(),
                  [](internal::Worker& worker) { worker.WaitForFinish(); });
}

namespace internal {
WorkManager::WorkManager(std::vector<std::function<void()>>&& funcs)
    : m_counter(funcs.size()), m_work(std::move(funcs)) {};

std::vector<std::function<void()>> WorkManager::GetTasks() {
    if (m_work.size() == 0) {
        throw std::invalid_argument(
            "The size of work to be managed is zero."
            "Either the object was instantiated with an empty vector or GetTasks got called "
            "multiple times.");
    }

    std::vector<std::function<void()>> tasks{};
    tasks.reserve(m_work.size());
    for (auto&& func : m_work) {
        tasks.push_back(
            std::function<void()>([f = std::move(func), workManager = shared_from_this()]() {
                f();
                workManager->finished();
            }));
    }

    m_work.clear();

    return tasks;
}

std::future<void> WorkManager::GetFuture() { return m_promise.get_future(); }

void WorkManager::finished() {
    if (m_counter.fetch_sub(1) == 1) {
        m_promise.set_value();
    }
}

WorkPipe::WorkPipe() : m_stopped(false) {}

std::future<void> WorkPipe::Push(std::function<void()>&& f) {
    std::lock_guard<std::mutex> lock(m_global_workqueue_mutex);

    if (m_stopped) {
        throw std::runtime_error("Cannot push to WorkPipe because it had already been stopped!");
    }

    // TODO: Do not use WorkManager and optimize for 1-task case?
    auto workManager =
        std::make_shared<WorkManager>(std::vector<std::function<void()>>{std::move(f)});

    auto tasks = workManager->GetTasks();
    for (auto&& task : tasks) {
        m_global_workqueue.push(std::move(task));
    }

    m_condition_variable.notify_all();

    return workManager->GetFuture();
}

std::future<void> WorkPipe::Push(std::vector<std::function<void()>>&& f) {
    std::lock_guard<std::mutex> lock(m_global_workqueue_mutex);

    if (m_stopped) {
        throw std::runtime_error("Cannot push to WorkPipe because it had already been stopped!");
    }

    auto workManager = std::make_shared<WorkManager>(std::move(f));

    auto tasks = workManager->GetTasks();
    for (auto&& task : tasks) {
        m_global_workqueue.push(std::move(task));
    }

    m_condition_variable.notify_all();

    return workManager->GetFuture();
}

std::tuple<bool, std::function<void()>> WorkPipe::Wait() {
    std::unique_lock<std::mutex> lock(m_global_workqueue_mutex);

    if (!m_global_workqueue.empty()) {
        std::function<void()> element(std::move(m_global_workqueue.front()));
        m_global_workqueue.pop();
        return std::make_tuple<bool, std::function<void()>>(false, std::move(element));
    } else if (m_stopped) {
        return std::make_tuple<bool, std::function<void()>>(true, std::function<void()>());
    }

    m_condition_variable.wait(lock,
                              [this]() { return !m_global_workqueue.empty() || m_stopped; });

    if (!m_global_workqueue.empty()) {
        std::function<void()> element(std::move(m_global_workqueue.front()));
        m_global_workqueue.pop();
        return std::make_tuple<bool, std::function<void()>>(false, std::move(element));
    }

    return std::make_tuple<bool, std::function<void()>>(true, std::function<void()>());
}

void WorkPipe::Stop() {
    std::lock_guard<std::mutex> lock(m_global_workqueue_mutex);

    m_stopped = true;

    m_condition_variable.notify_all();
}

void Worker::Start(std::shared_ptr<WorkPipe> workPipe) {
    std::lock_guard<std::mutex> lock(m_threadMutex);

    if (m_thread.joinable()) {
        throw std::runtime_error("Worker thread has been already initialized!");
    }

    m_thread = std::thread(&Worker::Run, this, workPipe);
}

void Worker::Run(std::shared_ptr<WorkPipe> workPipe) {
    while (true) {
        auto work = workPipe->Wait();

        if (std::get<0>(work)) {
            // A stop signal has been sent
            return;
        }

        auto workUnit = std::move(std::get<1>(work));
        workUnit();
    }
}

void Worker::WaitForFinish() {
    if (!m_thread.joinable()) {
        throw std::runtime_error(
            "Cannot wait for finish, because worker thread has never been initialized!");
    }

    m_thread.join();
}
}  // namespace internal
}  // namespace Common
