#pragma once

#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include <chrono>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <ostream>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

#include "Configuration.hpp"

namespace Common {
class Parameters {
   public:
    void SetParameter(std::string key, std::string value) { m_values[key] = value; };

    // TODO: iterator
    std::map<std::string, std::string>::const_iterator begin() { return m_values.begin(); };
    std::map<std::string, std::string>::const_iterator end() { return m_values.end(); };

   private:
    // TODO: Make JSON
    std::map<std::string, std::string> m_values;
};

class HashJoinTimingResult {
   public:
    HashJoinTimingResult(){};

    HashJoinTimingResult(std::chrono::nanoseconds buildPhase, std::chrono::nanoseconds probePhase,
                         const Parameters& parameters)
        : m_buildPhase(buildPhase), m_probePhase(probePhase), m_parameters(parameters) {}

    void SetBuildPhaseDuration(std::chrono::nanoseconds buildPhase) { m_buildPhase = buildPhase; };
    void SetProbePhaseDuration(std::chrono::nanoseconds probePhase) { m_probePhase = probePhase; };
    void SetParameters(const Parameters& params) { m_parameters = params; }

    std::chrono::nanoseconds GetBuildPhaseDuration() const { return m_buildPhase; };
    std::chrono::nanoseconds GetProbePhaseDuration() const { return m_probePhase; };
    Parameters GetParameters() const { return m_parameters; }

   private:
    Parameters m_parameters;
    std::chrono::nanoseconds m_buildPhase;
    std::chrono::nanoseconds m_probePhase;
};

class IHashJoinTimer {
   public:
    virtual void SetBuildPhaseBegin() = 0;
    virtual void SetBuildPhaseEnd() = 0;
    virtual void SetProbePhaseBegin() = 0;
    virtual void SetProbePhaseEnd() = 0;
    virtual HashJoinTimingResult GetResult() = 0;
    virtual ~IHashJoinTimer() = default;
};

class NoOpHashJoinTimer : public IHashJoinTimer {
   public:
    void SetBuildPhaseBegin(){};
    void SetBuildPhaseEnd(){};
    void SetProbePhaseBegin(){};
    void SetProbePhaseEnd(){};
    HashJoinTimingResult GetResult() { return HashJoinTimingResult(); };
};

class HashJoinTimer : public IHashJoinTimer {
   public:
    HashJoinTimer(const Parameters& parameters) : m_parameters(parameters) {}

    void SetBuildPhaseBegin() { m_buildStart = std::chrono::steady_clock::now(); };
    void SetBuildPhaseEnd() { m_buildEnd = std::chrono::steady_clock::now(); };
    void SetProbePhaseBegin() { m_probeStart = std::chrono::steady_clock::now(); };
    void SetProbePhaseEnd() { m_probeEnd = std::chrono::steady_clock::now(); };

    HashJoinTimingResult GetResult() {
        return HashJoinTimingResult(m_buildEnd - m_buildStart, m_probeEnd - m_probeStart,
                                    m_parameters);
    };

   private:
    std::chrono::time_point<std::chrono::steady_clock> m_buildStart, m_buildEnd;
    std::chrono::time_point<std::chrono::steady_clock> m_probeStart, m_probeEnd;
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

        pt.add("results.build", getStringDuration<std::chrono::milliseconds,
                                                  decltype(results.GetBuildPhaseDuration())>(
                                    results.GetBuildPhaseDuration()) +
                                    "ms");
        pt.add("results.probe", getStringDuration<std::chrono::milliseconds,
                                                  decltype(results.GetBuildPhaseDuration())>(
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
