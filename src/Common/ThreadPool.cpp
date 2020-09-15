#include "ThreadPool.hpp"

#include <functional>
#include <map>

#include "Common/Logger.hpp"

namespace Common {
ThreadPool::ThreadPool(size_t numberOfWorkers)
    : m_workers(numberOfWorkers), m_workPipe(std::make_shared<internal::ThreadPool::WorkPipe>()) {
    std::for_each(m_workers.begin(), m_workers.end(),
                  [this](internal::ThreadPool::Worker& worker) { worker.Start(this->m_workPipe); });
}

std::future<TasksErrorHolder> ThreadPool::Push(std::function<void()>&& f) {
    return m_workPipe->Push(std::move(f));
}

std::future<TasksErrorHolder> ThreadPool::Push(std::vector<std::function<void()>>&& f) {
    return m_workPipe->Push(std::move(f));
}

std::future<TasksErrorHolder> ThreadPool::Push(std::shared_ptr<IPipeline> pipeline) {
    return m_workPipe->Push(std::move(pipeline));
}

size_t ThreadPool::GetNumberOfWorkers() const { return m_workers.size(); }

void ThreadPool::Stop() {
    m_workPipe->Stop();

    std::for_each(m_workers.begin(), m_workers.end(),
                  [](internal::ThreadPool::Worker& worker) { worker.WaitForFinish(); });
}

Pipeline::Pipeline(std::shared_ptr<IThreadPool> thread_pool)
    : m_idCounter(0),
      m_finishedBatches(0),
      m_failed(false),
      m_started(false),
      m_threadPool(thread_pool) {}

size_t Pipeline::Add(std::vector<std::function<void()>>&& f) {
    if (m_started) {
        throw std::runtime_error("Pipeline::Add: Pipeline has already been started.");
    }

    size_t id = m_idCounter++;

    std::vector<std::function<void()>> tasks{};
    tasks.reserve(f.size());
    for (auto&& func : f) {
        tasks.push_back(
            std::function<void()>([f = std::move(func), pipeline = shared_from_this(), id]() {
                try {
                    f();
                } catch (std::exception& e) {
                    pipeline->m_mutex.lock();
                    pipeline->m_exceptions.Push(e);
                    pipeline->m_mutex.unlock();
                };

                pipeline->finished(id);
            }));
    }

    m_counters[id] = tasks.size();
    m_tasks[id] = std::move(tasks);

    return id;
}

std::future<TasksErrorHolder> Pipeline::Start() {
    if (m_started) {
        throw std::runtime_error("Pipeline::Start: Pipeline has already been started.");
    }

    m_started = true;
    m_tasksIterator = m_tasks.begin();
    return m_globalPromise.get_future();
}

std::vector<std::function<void()>> Pipeline::Next() {
    std::lock_guard<std::mutex> lock(m_mutex);
    return this->next();
}

std::vector<std::function<void()>> Pipeline::next() {
    if (m_tasksIterator == m_tasks.end()) {
        throw std::runtime_error("Pipeline::next: Cannot call next on empty pipeline.");
    }

    std::vector<std::function<void()>> tasks = m_tasksIterator->second;
    m_tasksIterator++;

    return std::move(tasks);
}

void Pipeline::finished(size_t id) {
    std::lock_guard<std::mutex> lock(m_mutex);

    // We check whether all tasks in a batch have finished.
    if (m_counters[id]-- == 1) {
        m_finishedBatches++;
    } else {
        return;
    }

    size_t numberOfSpawnedBatches = std::distance(m_tasks.begin(), m_tasksIterator);

    // If a task has failed before, we just check whether we have processed all the tasks
    // spawned before the failure. After the failure, no new tasks are spawned, therefore we can
    // safely set the promise as no new tasks will ever finish.
    if (m_failed) {
        if (m_finishedBatches == numberOfSpawnedBatches) {
            m_globalPromise.set_value(m_exceptions);
        }

        return;
    }

    // If a task has not failed before, we set the failure flag and check whether we have
    // processed all the previously spawned tasks. If so, we can safely
    // set the promise as no new tasks will ever finish.
    if (!m_exceptions.Empty()) {
        m_failed = true;

        if (m_finishedBatches == numberOfSpawnedBatches) {
            m_globalPromise.set_value(m_exceptions);
        }

        return;
    }

    // If we have not spawned all the tasks, we now spawn a new batch
    // If we have processed all the tasks, we set the promise as we have reached
    // the end of the pipeline
    if (numberOfSpawnedBatches != m_tasks.size()) {
        auto tasks = this->next();
        m_threadPool->Push(std::move(tasks));
        return;
    } else if (m_finishedBatches == m_tasks.size()) {
        m_globalPromise.set_value(m_exceptions);
        return;
    }
}

namespace internal {
namespace ThreadPool {
WorkManager::WorkManager(std::vector<std::function<void()>>&& funcs)
    : m_counter(funcs.size()), m_work(std::move(funcs)){};

std::vector<std::function<void()>> WorkManager::GetTasks() {
    if (m_work.size() == 0) {
        throw std::invalid_argument(
            "WorkManager::GetTasks: The size of work to be managed is zero."
            "Either the object was instantiated with an empty vector or GetTasks got called "
            "multiple times.");
    }

    std::vector<std::function<void()>> tasks{};
    tasks.reserve(m_work.size());
    for (auto&& func : m_work) {
        tasks.push_back(
            std::function<void()>([f = std::move(func), workManager = shared_from_this()]() {
                try {
                    f();
                } catch (std::exception& e) {
                    workManager->m_exceptionsMutex.lock();
                    workManager->m_exceptions.Push(e);
                    workManager->m_exceptionsMutex.unlock();
                };

                workManager->finished();
            }));
    }

    m_work.clear();

    return tasks;
}

std::future<TasksErrorHolder> WorkManager::GetFuture() { return m_promise.get_future(); }

void WorkManager::finished() {
    if (m_counter.fetch_sub(1) == 1) {
        m_promise.set_value(m_exceptions);
    }
}

WorkPipe::WorkPipe() : m_stopped(false) {}

std::future<TasksErrorHolder> WorkPipe::Push(std::function<void()>&& f) {
    std::lock_guard<std::mutex> lock(m_mutex);

    if (m_stopped) {
        throw std::runtime_error(
            "WorkPipe::Push: Cannot push to WorkPipe because it has already been stopped!");
    }

    auto workManager =
        std::make_shared<WorkManager>(std::vector<std::function<void()>>{std::move(f)});

    auto tasks = workManager->GetTasks();
    for (auto&& task : tasks) {
        m_global_workqueue.push(std::move(task));
    }

    m_condition_variable.notify_all();

    return workManager->GetFuture();
}

std::future<TasksErrorHolder> WorkPipe::Push(std::vector<std::function<void()>>&& f) {
    std::lock_guard<std::mutex> lock(m_mutex);

    if (m_stopped) {
        throw std::runtime_error(
            "WorkPipe::Push: Cannot push to WorkPipe because it has already been stopped!");
    }

    auto workManager = std::make_shared<WorkManager>(std::move(f));

    auto tasks = workManager->GetTasks();
    for (auto&& task : tasks) {
        m_global_workqueue.push(std::move(task));
    }

    m_condition_variable.notify_all();

    return workManager->GetFuture();
}

std::future<TasksErrorHolder> WorkPipe::Push(std::shared_ptr<IPipeline> pipeline) {
    std::lock_guard<std::mutex> lock(m_mutex);

    if (m_stopped) {
        throw std::runtime_error(
            "WorkPipe::Push: Cannot push to WorkPipe because it has already been stopped!");
    }

    std::future<TasksErrorHolder> future = pipeline->Start();

    auto tasks = pipeline->Next();
    for (auto&& task : tasks) {
        m_global_workqueue.push(std::move(task));
    }

    m_condition_variable.notify_all();

    return future;
}

std::tuple<bool, std::function<void()>> WorkPipe::Wait(size_t id) {
    std::unique_lock<std::mutex> lock(m_mutex);

    if (!m_global_workqueue.empty()) {
        std::function<void()> element(std::move(m_global_workqueue.front()));
        m_global_workqueue.pop();
        return std::make_tuple<bool, std::function<void()>>(false, std::move(element));
    } else if (m_stopped) {
        return std::make_tuple<bool, std::function<void()>>(true, std::function<void()>());
    }

    m_condition_variable.wait(lock, [this]() { return !m_global_workqueue.empty() || m_stopped; });

    if (!m_global_workqueue.empty()) {
        std::function<void()> element(std::move(m_global_workqueue.front()));
        m_global_workqueue.pop();
        return std::make_tuple<bool, std::function<void()>>(false, std::move(element));
    }

    return std::make_tuple<bool, std::function<void()>>(true, std::function<void()>());
}

void WorkPipe::Stop() {
    std::lock_guard<std::mutex> lock(m_mutex);

    m_stopped = true;

    m_condition_variable.notify_all();
}

void Worker::Start(std::shared_ptr<WorkPipe> workPipe) {
    std::lock_guard<std::mutex> lock(m_threadMutex);

    if (m_thread.joinable()) {
        throw std::runtime_error("Worker::Start: Worker thread has been already initialized!");
    }

    m_thread = std::thread(&Worker::Run, this, workPipe);
}

void Worker::Run(std::shared_ptr<WorkPipe> workPipe) {
    auto threadID = std::hash<std::thread::id>{}(std::this_thread::get_id());

    while (true) {
        auto work = workPipe->Wait(threadID);

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
            "Worker::WaitForFinish: Cannot wait for finish, because worker thread has never been "
            "initialized!");
    }

    m_thread.join();
}
}  // namespace ThreadPool
}  // namespace internal
}  // namespace Common
