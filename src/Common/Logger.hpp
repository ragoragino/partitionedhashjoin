#pragma once

#include <boost/log/sources/record_ostream.hpp>
#include <boost/log/sources/severity_feature.hpp>
#include <boost/log/sources/severity_logger.hpp>
#include <string>

namespace Common {
enum SeverityLevel { trace, debug, info, error, critical };

typedef boost::log::sources::severity_logger<SeverityLevel> LoggerType;

struct LoggerConfiguration {
    SeverityLevel severity_level;
};

void InitializeLogger(const LoggerConfiguration& configuration);

LoggerType GetNewLogger();

void AddComponentAttributeToLogger(LoggerType& logger, std::string componentName);

#define LOG(lg, sev) BOOST_LOG_SEV(lg, sev)
}  // namespace Common
