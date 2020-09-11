#pragma once

#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include <chrono>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <ostream>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

#include "Configuration.hpp"

namespace Common {
class Parameters {
   public:
    using StorageType = std::map<std::string, std::string>;

    void SetParameter(std::string key, std::string value) { m_values[key] = value; };

    class Iterator {
       public:
        Iterator::Iterator(StorageType::const_iterator iter) : m_iter{iter} {}

        typename Iterator& operator++() {
            this->m_iter++;
            return *this;
        }

        typename Iterator& Iterator::operator++(int) {
            Iterator it = *this;
            ++*this;
            return it;
        }

        const std::pair<const std::string, std::string>& operator*() { return *this->m_iter; }

        bool operator==(const Iterator& iter) { return this->m_iter == iter.m_iter; }

        bool operator!=(const Iterator& iter) { return this->m_iter != iter.m_iter; }

       private:
        StorageType::const_iterator m_iter;
    };

    Iterator begin() const { return Iterator(m_values.begin()); };
    Iterator end() const { return Iterator(m_values.end()); };

   private:
    StorageType m_values;
};

// TODO: It would be nice to have a protobuf of this class to be sharable across networks,
// languages...
class HashJoinTimingResult {
   public:
    HashJoinTimingResult(){};

    HashJoinTimingResult(std::chrono::nanoseconds buildPhase, std::chrono::nanoseconds probePhase,
                         std::chrono::nanoseconds partitioningPhase, const Parameters& parameters)
        : m_buildPhase(buildPhase),
          m_probePhase(probePhase),
          m_partitioningPhase(partitioningPhase),
          m_parameters(parameters) {}

    void SetBuildPhaseDuration(std::chrono::nanoseconds buildPhase) { m_buildPhase = buildPhase; };
    void SetProbePhaseDuration(std::chrono::nanoseconds probePhase) { m_probePhase = probePhase; };
    void SetPartitioningPhaseDuration(std::chrono::nanoseconds partitioningPhase) {
        m_partitioningPhase = partitioningPhase;
    };
    void SetParameters(const Parameters& params) { m_parameters = params; }

    std::chrono::nanoseconds GetBuildPhaseDuration() const { return m_buildPhase; };
    std::chrono::nanoseconds GetProbePhaseDuration() const { return m_probePhase; };
    std::chrono::nanoseconds GetPartitioningPhaseDuration() const { return m_partitioningPhase; };
    const Parameters& GetParameters() const { return m_parameters; }

   private:
    Parameters m_parameters;
    std::chrono::nanoseconds m_buildPhase;
    std::chrono::nanoseconds m_probePhase;
    std::chrono::nanoseconds m_partitioningPhase;
};

class ITimeSegmentMeasurer {
   public:
    virtual std::chrono::nanoseconds GetSegmentDuration() = 0;
    virtual void Start() = 0;
    virtual void End() = 0;
    virtual ~ITimeSegmentMeasurer() = default;
};

class TimeSegmentMeasurer : public ITimeSegmentMeasurer {
   public:
    TimeSegmentMeasurer() {}
    std::chrono::nanoseconds GetSegmentDuration() { return m_duration; }
    void Start() {
        m_start = std::chrono::steady_clock::now();
        m_started = true;
    };
    void End() {
        if (!m_started) {
            std::runtime_error("TimeSegmentMeasurer::End: Start was not called before End.");
        }
        m_duration += std::chrono::steady_clock::now() - m_start;
        m_started = false;
    };

   private:
    bool m_started;
    std::chrono::time_point<std::chrono::steady_clock> m_start;
    std::chrono::nanoseconds m_duration;
};

// IHashJoinTimer provides two interfaces - either direct or indirect
// Direct interface works by setting SetBuildPhaseBegin and then SetBuildPhaseEnd.
// Indirect one works by creating a segment measurer (which itself provides Start and End methods)
// and then passing it back after the measurements.
// Direct should be used when the measured quantity is continuous,
// indirect when the measured quantity is discontinuous.
class IHashJoinTimer {
   public:
    // continuous time segment (not thread-safe)
    virtual void SetBuildPhaseBegin() = 0;
    virtual void SetBuildPhaseEnd() = 0;
    virtual void SetPartitioningPhaseBegin() = 0;
    virtual void SetPartitioningPhaseEnd() = 0;
    virtual void SetProbePhaseBegin() = 0;
    virtual void SetProbePhaseEnd() = 0;

    // discontinuous time segment (thread-safe)
    virtual std::unique_ptr<ITimeSegmentMeasurer> GetSegmentMeasurer() = 0;
    virtual void SetBuildPhaseDuration(std::unique_ptr<ITimeSegmentMeasurer>&& measurer) = 0;
    virtual void SetProbePhaseDuration(std::unique_ptr<ITimeSegmentMeasurer>&& measurer) = 0;
    virtual void SetPartitionPhaseDuration(std::unique_ptr<ITimeSegmentMeasurer>&& measurer) = 0;

    virtual HashJoinTimingResult GetResult() = 0;

    virtual ~IHashJoinTimer() = default;
};

class NoOpHashJoinTimer final : public IHashJoinTimer {
   public:
    void SetBuildPhaseBegin(){};
    void SetBuildPhaseEnd(){};
    void SetPartitioningPhaseBegin(){};
    void SetPartitioningPhaseEnd(){};
    void SetProbePhaseBegin(){};
    void SetProbePhaseEnd(){};

    std::unique_ptr<ITimeSegmentMeasurer> GetSegmentMeasurer() { return nullptr; };
    void SetBuildPhaseDuration(std::unique_ptr<ITimeSegmentMeasurer>&& measurer){};
    void SetProbePhaseDuration(std::unique_ptr<ITimeSegmentMeasurer>&& measurer){};
    void SetPartitionPhaseDuration(std::unique_ptr<ITimeSegmentMeasurer>&& measurer){};

    HashJoinTimingResult GetResult(){};
};

class HashJoinTimer final : public IHashJoinTimer {
   public:
    HashJoinTimer(const Parameters& parameters)
        : m_parameters(parameters),
          m_buildTimeSet(false),
          m_probeTimeSet(false),
          m_partitioningTimeSet(false),
          m_buildTime(0),
          m_probeTime(0),
          m_partitioningTime(0) {}

    void SetBuildPhaseBegin() { m_buildStart = std::chrono::steady_clock::now(); };
    void SetBuildPhaseEnd() {
        if (m_buildTimeSet) {
            std::runtime_error(
                "HashJoinTimer::SetBuildPhaseEnd: build time has been already measured.");
        }
        m_buildTime = std::chrono::steady_clock::now() - m_buildStart;
        m_buildTimeSet = true;
    };
    void SetPartitioningPhaseBegin() { m_partitioningStart = std::chrono::steady_clock::now(); };
    void SetPartitioningPhaseEnd() {
        if (m_partitioningTimeSet) {
            std::runtime_error(
                "HashJoinTimer::SetPartitioningPhaseEnd: probe time has been already measured.");
        }
        m_partitioningTime = std::chrono::steady_clock::now() - m_partitioningStart;
        m_partitioningTimeSet = true;
    };
    void SetProbePhaseBegin() { m_probeStart = std::chrono::steady_clock::now(); };
    void SetProbePhaseEnd() {
        if (m_probeTimeSet) {
            std::runtime_error(
                "HashJoinTimer::SetProbePhaseEnd: partitioning time has been already measured.");
        }
        m_probeTime = std::chrono::steady_clock::now() - m_buildStart;
        m_probeTimeSet = true;
    };

    std::unique_ptr<ITimeSegmentMeasurer> GetSegmentMeasurer() {
        return std::make_unique<TimeSegmentMeasurer>();
    };

    void SetBuildPhaseDuration(std::unique_ptr<ITimeSegmentMeasurer>&& measurer) {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (m_buildTimeSet) {
            std::runtime_error(
                "HashJoinTimer::SetBuildPhaseDuration: build time has been already "
                "measured.");
        }
        m_buildTime = measurer->GetSegmentDuration();
        m_buildTimeSet = true;
    };
    void SetProbePhaseDuration(std::unique_ptr<ITimeSegmentMeasurer>&& measurer) {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (m_probeTimeSet) {
            std::runtime_error(
                "HashJoinTimer::SetProbePhaseDuration: probe time has been already measured.");
        }
        m_probeTime = measurer->GetSegmentDuration();
        m_probeTimeSet = true;
    };
    void SetPartitionPhaseDuration(std::unique_ptr<ITimeSegmentMeasurer>&& measurer) {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (m_partitioningTimeSet) {
            std::runtime_error(
                "HashJoinTimer::SetPartitionPhaseDuration: partitioning time has been already measured.");
        }
        m_partitioningTime = measurer->GetSegmentDuration();
        m_partitioningTimeSet = true;
    };

    HashJoinTimingResult GetResult() {
        return HashJoinTimingResult(m_buildTime, m_probeTime, m_partitioningTime, m_parameters);
    };

   private:
    std::mutex m_mutex;
    bool m_buildTimeSet, m_probeTimeSet, m_partitioningTimeSet;
    std::chrono::nanoseconds m_buildTime, m_probeTime, m_partitioningTime;
    std::chrono::time_point<std::chrono::steady_clock> m_buildStart, m_probeStart,
        m_partitioningStart;

    Parameters m_parameters;
};

template <typename OutputDuration, typename InputDuration>
std::string getStringDuration(InputDuration duration) {
    std::ostringstream s;
    s << std::chrono::duration_cast<OutputDuration>(duration).count();
    return s.str();
}

class ITestResultsFormatter {
   public:
    virtual void Format(std::basic_ostream<char>& stream, const HashJoinTimingResult& result) = 0;
    virtual ~ITestResultsFormatter() = default;
};

class ITestResultsRenderer {
   public:
    virtual void Render(std::shared_ptr<ITestResultsFormatter> formatter,
                        const HashJoinTimingResult& result) = 0;
    virtual ~ITestResultsRenderer() = default;
};

class JSONResultsFormatter final : public ITestResultsFormatter {
   public:
    JSONResultsFormatter(){};

    void Format(std::basic_ostream<char>& stream, const HashJoinTimingResult& results) {
        boost::property_tree::ptree pt = this->GetBaseFormat();

        std::for_each(results.GetParameters().begin(), results.GetParameters().end(),
                      [&pt](const std::pair<std::string, std::string>& element) {
                          pt.add("parameters." + element.first, element.second);
                      });

        pt.add("results.partition",
               getStringDuration<std::chrono::milliseconds,
                                 decltype(results.GetPartitioningPhaseDuration())>(
                   results.GetPartitioningPhaseDuration()) +
                   "ms");
        pt.add(
            "results.build",
            getStringDuration<std::chrono::milliseconds, decltype(results.GetBuildPhaseDuration())>(
                results.GetBuildPhaseDuration()) +
                "ms");
        pt.add(
            "results.probe",
            getStringDuration<std::chrono::milliseconds, decltype(results.GetBuildPhaseDuration())>(
                results.GetProbePhaseDuration()) +
                "ms");

        boost::property_tree::json_parser::write_json(stream, pt);
    }

   private:
    boost::property_tree::ptree GetBaseFormat() {
        boost::property_tree::ptree pt;
        pt.add("id", "hashjointimingresult");

        return pt;
    }
};

class FileTestResultsRenderer final : public ITestResultsRenderer {
   public:
    FileTestResultsRenderer(const OutputConfiguration& config) : m_file(config.File.Name) {}

    void Render(std::shared_ptr<ITestResultsFormatter> formatter,
                const HashJoinTimingResult& result) {
        return formatter->Format(m_file, result);
    }

    ~FileTestResultsRenderer() { m_file.close(); }

   private:
    std::ofstream m_file;
};

inline std::shared_ptr<ITestResultsFormatter> SelectResultsFormatter(const Configuration& config) {
    switch (config.ResultFormat) {
        case ResultsFormat::JSON:
            return std::make_shared<JSONResultsFormatter>();
        default:
            std::stringstream is;
            is << "Unrecognized results format: " << config.ResultFormat << ".";
            throw std::runtime_error(is.str());
    };

    return nullptr;
};

inline std::shared_ptr<ITestResultsRenderer> SelectResultsRenderer(const Configuration& config) {
    switch (config.Output.Type) {
        case OutputType::File:
            return std::make_shared<FileTestResultsRenderer>(config.Output);
        default:
            std::stringstream is;
            is << "Unrecognized output type: " << config.Output.Type << ".";
            throw std::runtime_error(is.str());
    };
}

};  // namespace Common
