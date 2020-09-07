#pragma once

#include <boost/log/sources/record_ostream.hpp>
#include <boost/log/sources/severity_feature.hpp>
#include <boost/log/sources/severity_logger.hpp>
#include <string>

namespace Common {
enum SeverityLevel { trace, debug, info, error, critical };

SeverityLevel SeverityLevelFromString(const std::string& level);

typedef boost::log::sources::severity_logger<SeverityLevel> LoggerType;

struct LoggerConfiguration {
    LoggerConfiguration() : SeverityLevel(info){};

    LoggerConfiguration(std::string level) { 
        SeverityLevel = SeverityLevelFromString(level);
    };

    SeverityLevel SeverityLevel;
};

void InitializeLogger(const LoggerConfiguration& configuration);

LoggerType GetNewLogger();

std::shared_ptr<LoggerType> GetScopedLogger(LoggerType);

void AddComponentAttributeToLogger(LoggerType& logger, std::string componentName);

void AddTableIDToLogger(LoggerType& logger, std::string tableID);

#define LOG(lg, sev) BOOST_LOG_SEV(lg, sev)
}  // namespace Common
