#pragma once

#include <benchmark/benchmark.h>

#include <memory>
#include <utility>

#include "../../src/HashTables/SeparateChaining.hpp"
#include "../../src/HashTables/LinearProbing.hpp"
#include "Common/Logger.hpp"
#include "Common/IThreadPool.hpp"
#include "Common/ThreadPool.hpp"

bool loggerInitialized = false;

class SeparateChainingFixture : public benchmark::Fixture {
   public:
    void SetUp(const ::benchmark::State& state) {
        if (!loggerInitialized) {
            loggerInitialized = true;
            Common::LoggerConfiguration logger_configuration{};
            logger_configuration.severity_level = Common::SeverityLevel::trace;
            Common::InitializeLogger(logger_configuration);
        }

        m_start = 0;
        m_end = state.range(0);
        m_numberOfObjects = m_end - m_start;
        m_range = static_cast<size_t>(
            ceil(static_cast<double>(m_numberOfObjects) / static_cast<double>(state.threads)));

        m_hasher = std::make_shared<Common::XXHasher>();

        m_waiting.store(0);
        m_finished.store(false);
    }

    void TearDown(const ::benchmark::State& state) {
        m_hashTable.reset();
        m_hasher.reset();
    }

    void Sync(const ::benchmark::State& state) { 
        std::unique_lock<std::mutex> newWaitersLock(m_mutex);
        if (m_finished.load()) {
            m_newWaiterCond.wait(newWaitersLock, [this]() { return !m_finished.load(); });
        }
        newWaitersLock.unlock();

        m_waiting.fetch_add(1);
        if (m_waiting.load() == state.threads) {
            std::unique_lock<std::mutex> masterLock(m_mutex);

            m_finished.store(true);

            HashTables::SeparateChainingConfiguration configuration{
                0.1,  // HASH_TABLE_SIZE_RATIO
            };

            Common::XXHasher hasher{};
            HashTables::SeparateChainingFactory<Common::Tuple, 3, Common::XXHasher> factory(
                configuration, hasher);
            m_hashTable = factory.New(m_numberOfObjects);

            m_cond.notify_all();

            m_masterCond.wait(masterLock, [this, &state]() {
                if (m_waiting == 1) {
                    m_waiting = 0;
                    return true;
                }

                return false;
            });

            m_finished.store(false);
            masterLock.unlock();

            m_newWaiterCond.notify_all();
            return;
        }

        std::unique_lock<std::mutex> baseLock(m_mutex);
        m_cond.wait(baseLock, [this, &state]() { return m_finished.load(); });
        m_waiting.fetch_sub(1);
        baseLock.unlock();

        m_masterCond.notify_one();
    }

   protected:
    size_t m_start, m_end, m_range, m_numberOfObjects;
    std::shared_ptr<Common::XXHasher> m_hasher;
    std::shared_ptr<HashTables::SeparateChainingFactory<Common::Tuple, 3, Common::XXHasher>::HashTableType> m_hashTable;
    Common::LoggerType m_logger;

    std::atomic<bool> m_finished;
    std::atomic<size_t> m_waiting;
    std::mutex m_mutex;
    std::condition_variable m_cond;
    std::condition_variable m_masterCond;
    std::condition_variable m_newWaiterCond;
};

BENCHMARK_DEFINE_F(SeparateChainingFixture, SeparateChainingTest)(benchmark::State& state) {
    Common::Tuple tuple{
        123456789,  // id
        987654321,  // payload
    };

    while (state.KeepRunning()) {
        state.PauseTiming(); 
        this->Sync(state);
        state.ResumeTiming(); 

        size_t threadStart = this->m_start + state.thread_index * this->m_range;
        size_t threadEnd = this->m_start + (state.thread_index + 1) * this->m_range;
        if (state.thread_index == (state.threads - 1)) {
            threadEnd = this->m_end;
        }

        //LOG(m_logger, Common::trace) << "Starting " << state.thread_index << "[" << threadStart << ", " << threadEnd
        //                                                                             << "]\n";

        for (int64_t i = threadStart; i != threadEnd; ++i) {
            this->m_hashTable->Insert(i, &tuple);
        }

        //LOG(m_logger, Common::trace) << "Ending " << state.thread_index << "\n";
    }
}

BENCHMARK_REGISTER_F(SeparateChainingFixture, SeparateChainingTest)->Arg(1000000)->ThreadRange(1, 32)->UseRealTime()->Unit(benchmark::kMillisecond);

class LinearProbingFixture : public benchmark::Fixture {
   public:
    void SetUp(const ::benchmark::State& state) {
        if (!loggerInitialized) {
            loggerInitialized = true;
            Common::LoggerConfiguration logger_configuration{};
            logger_configuration.severity_level = Common::SeverityLevel::trace;
            Common::InitializeLogger(logger_configuration);
        }

        m_start = 0;
        m_end = state.range(0);
        m_numberOfObjects = m_end - m_start;
        m_range = static_cast<size_t>(
            ceil(static_cast<double>(m_numberOfObjects) / static_cast<double>(state.threads)));

        m_hasher = std::make_shared<Common::XXHasher>();

        m_configuration = std::shared_ptr<HashTables::LinearProbingConfiguration>(
            new HashTables::LinearProbingConfiguration{1.0 / 0.75});

        m_waiting.store(0);
        m_finished.store(false);
    }

    void TearDown(const ::benchmark::State& state) {
        m_hashTable.reset();
        m_hasher.reset();
    }

    void Sync(const ::benchmark::State& state) {
        std::unique_lock<std::mutex> newWaitersLock(m_mutex);
        if (m_finished.load()) {
            m_newWaiterCond.wait(newWaitersLock, [this]() { return !m_finished.load(); });
        }
        newWaitersLock.unlock();

        m_waiting.fetch_add(1);
        if (m_waiting.load() == state.threads) {
            std::unique_lock<std::mutex> masterLock(m_mutex);

            m_finished.store(true);

            Common::XXHasher hasher{};
            HashTables::LinearProbingFactory<Common::Tuple, 3, Common::XXHasher> factory(
                *m_configuration, hasher);
            m_hashTable = factory.New(m_numberOfObjects);

            m_cond.notify_all();

            m_masterCond.wait(masterLock, [this, &state]() {
                if (m_waiting == 1) {
                    m_waiting = 0;
                    return true;
                }

                return false;
            });

            m_finished.store(false);
            masterLock.unlock();

            m_newWaiterCond.notify_all();
            return;
        }

        std::unique_lock<std::mutex> baseLock(m_mutex);
        m_cond.wait(baseLock, [this, &state]() { return m_finished.load(); });
        m_waiting.fetch_sub(1);
        baseLock.unlock();

        m_masterCond.notify_one();
    }

   protected:
    std::shared_ptr<HashTables::LinearProbingConfiguration> m_configuration; 
    size_t m_start, m_end, m_range, m_numberOfObjects;
    std::shared_ptr<Common::XXHasher> m_hasher;
    std::shared_ptr<HashTables::LinearProbingFactory<Common::Tuple, 3, Common::XXHasher>::HashTableType> m_hashTable;
    Common::LoggerType m_logger;

    std::atomic<bool> m_finished;
    std::atomic<size_t> m_waiting;
    std::mutex m_mutex;
    std::condition_variable m_cond;
    std::condition_variable m_masterCond;
    std::condition_variable m_newWaiterCond;
};

BENCHMARK_DEFINE_F(LinearProbingFixture, LinearProbingTest)(benchmark::State& state) {
    Common::Tuple tuple{
        123456789,  // id
        987654321,  // payload
    };

    while (state.KeepRunning()) {
        state.PauseTiming();
        this->Sync(state);
        state.ResumeTiming();

        size_t threadStart = this->m_start + state.thread_index * this->m_range;
        size_t threadEnd = this->m_start + (state.thread_index + 1) * this->m_range;
        if (state.thread_index == (state.threads - 1)) {
            threadEnd = this->m_end;
        }

        //LOG(m_logger, Common::trace) << "Starting " << state.thread_index << "[" << threadStart
        //                             << ", " << threadEnd << "]\n";

        for (int64_t i = threadStart; i != threadEnd; ++i) {
            this->m_hashTable->Insert(i, &tuple);
        }

        // LOG(m_logger, Common::trace) << "Ending " << state.thread_index << "\n";
    }
}

BENCHMARK_REGISTER_F(LinearProbingFixture, LinearProbingTest)
    ->Arg(1000000)
    ->ThreadRange(1, 32)
    ->UseRealTime()
    ->Unit(benchmark::kMillisecond);