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
        Iterator(StorageType::const_iterator iter) : m_iter{iter} {}

        Iterator& operator++() {
            this->m_iter++;
            return *this;
        }

        Iterator operator++(int) {
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
        : m_parameters(parameters),
          m_buildPhase(buildPhase),
          m_probePhase(probePhase),
          m_partitioningPhase(partitioningPhase) {}

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
    virtual std::chrono::nanoseconds GetDuration() = 0;
    virtual void Start() = 0;
    virtual void End() = 0;
    virtual ~ITimeSegmentMeasurer() = default;
};

class TimeSegmentMeasurer : public ITimeSegmentMeasurer {
   public:
    TimeSegmentMeasurer() : m_duration(0) {}
    std::chrono::nanoseconds GetDuration() { return m_duration; }
    void Start() {
        if (m_started) {
            std::runtime_error(
                "TimeSegmentMeasurer::Start: Start has already been called without no subsequent "
                "call to End.");
        }
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

    // discontinuous time segments (thread-safe)
    virtual void SetBuildPhaseDuration(std::chrono::nanoseconds duration) = 0;
    virtual void SetProbePhaseDuration(std::chrono::nanoseconds duration) = 0;
    virtual void SetPartitionPhaseDuration(std::chrono::nanoseconds durationr) = 0;

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

    void SetBuildPhaseDuration(std::chrono::nanoseconds){};
    void SetProbePhaseDuration(std::chrono::nanoseconds){};
    void SetPartitionPhaseDuration(std::chrono::nanoseconds){};

    HashJoinTimingResult GetResult() { return HashJoinTimingResult(); };
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

    void SetBuildPhaseDuration(std::chrono::nanoseconds duration) {
        if (m_buildTimeSet) {
            std::runtime_error(
                "HashJoinTimer::SetBuildPhaseDuration: build time has been already "
                "measured.");
        }
        m_buildTime = duration;
        m_buildTimeSet = true;
    };
    void SetProbePhaseDuration(std::chrono::nanoseconds duration) {
        if (m_probeTimeSet) {
            std::runtime_error(
                "HashJoinTimer::SetProbePhaseDuration: probe time has been already measured.");
        }
        m_probeTime = duration;
        m_probeTimeSet = true;
    };
    void SetPartitionPhaseDuration(std::chrono::nanoseconds duration) {
        if (m_partitioningTimeSet) {
            std::runtime_error(
                "HashJoinTimer::SetPartitionPhaseDuration: partitioning time has been already "
                "measured.");
        }
        m_partitioningTime = duration;
        m_partitioningTimeSet = true;
    };

    HashJoinTimingResult GetResult() {
        return HashJoinTimingResult(m_buildTime, m_probeTime, m_partitioningTime, m_parameters);
    };

   private:
    Parameters m_parameters;
    bool m_buildTimeSet, m_probeTimeSet, m_partitioningTimeSet;
    std::chrono::nanoseconds m_buildTime, m_probeTime, m_partitioningTime;
    std::chrono::time_point<std::chrono::steady_clock> m_buildStart, m_probeStart,
        m_partitioningStart;
};

class IResultsFormatter {
   public:
    virtual void Format(std::basic_ostream<char>& stream, const HashJoinTimingResult& result) = 0;
    virtual ~IResultsFormatter() = default;
};

class IResultsRenderer {
   public:
    virtual void Render(std::shared_ptr<IResultsFormatter> formatter,
                        const HashJoinTimingResult& result) = 0;
    virtual ~IResultsRenderer() = default;
};

class JSONResultsFormatter final : public IResultsFormatter {
   public:
    JSONResultsFormatter(const ResultsFormatConfiguration& config) : m_config(config){};

    void Format(std::basic_ostream<char>& stream, const HashJoinTimingResult& results) {
        boost::property_tree::ptree pt = this->GetBaseFormat();

        std::for_each(results.GetParameters().begin(), results.GetParameters().end(),
                      [&pt](const std::pair<std::string, std::string>& element) {
                          pt.add("parameters." + element.first, element.second);
                      });

        pt.add("results.partition", CastDurationToString(results.GetPartitioningPhaseDuration()));
        pt.add("results.build", CastDurationToString(results.GetBuildPhaseDuration()));
        pt.add("results.probe", CastDurationToString(results.GetProbePhaseDuration()));

        boost::property_tree::json_parser::write_json(stream, pt);
    }

   private:
    boost::property_tree::ptree GetBaseFormat() {
        boost::property_tree::ptree pt;
        pt.add("id", "hashjointimingresult");

        return pt;
    }

    template <typename InputDuration>
    std::string CastDurationToString(InputDuration duration) {
        std::ostringstream s;

        if (m_config.TimeUnit == "ns") {
            s << std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count();
        } else if (m_config.TimeUnit == "us") {
            s << std::chrono::duration_cast<std::chrono::microseconds>(duration).count();
        } else if (m_config.TimeUnit == "ms") {
            s << std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
        } else if (m_config.TimeUnit == "s") {
            s << std::chrono::duration_cast<std::chrono::seconds>(duration).count();
        } else {
            throw std::runtime_error(
                "JSONResultsFormatter::CastDurationToString: unrecognized duration unit: " +
                m_config.TimeUnit);
        }

        return s.str();
    }

    const ResultsFormatConfiguration m_config;
};

class FileResultsRenderer final : public IResultsRenderer {
   public:
    FileResultsRenderer(const OutputConfiguration& config) : m_file(config.File.Name) {}

    void Render(std::shared_ptr<IResultsFormatter> formatter, const HashJoinTimingResult& result) {
        return formatter->Format(m_file, result);
    }

    ~FileResultsRenderer() { m_file.close(); }

   private:
    std::ofstream m_file;
};

inline std::shared_ptr<IResultsFormatter> SelectResultsFormatter(const Configuration& config) {
    switch (config.OutputFormatConfig.Format) {
        case ResultsFormat::JSON:
            return std::make_shared<JSONResultsFormatter>(config.OutputFormatConfig);
        default:
            std::stringstream is;
            is << "Unrecognized results format: " << config.OutputFormatConfig.Format << ".";
            throw std::runtime_error(is.str());
    };
}

inline std::shared_ptr<IResultsRenderer> SelectResultsRenderer(const Configuration& config) {
    switch (config.OutputConfig.Type) {
        case OutputType::File:
            return std::make_shared<FileResultsRenderer>(config.OutputConfig);
        default:
            std::stringstream is;
            is << "Unrecognized output type: " << config.OutputConfig.Type << ".";
            throw std::runtime_error(is.str());
    };
}

}  // namespace Common
