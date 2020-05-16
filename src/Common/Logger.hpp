#pragma once

#include <boost/log/sources/record_ostream.hpp>
#include <boost/log/sources/severity_feature.hpp>
#include <boost/log/sources/severity_logger.hpp>

namespace Common {
// TODO: Logger should be an interface, to allow changing it in the future,
// in case we find out that current implementation is unsatisfactory

enum SeverityLevel { trace, debug, info, error, critical };

struct LoggerConfiguration {
    SeverityLevel severity_level;
};

void InitializeLogger(const LoggerConfiguration& configuration);

boost::log::sources::severity_logger<SeverityLevel> GetNewLogger();
}  // namespace Common
