#pragma once

#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/sources/severity_feature.hpp>
#include <boost/log/sources/record_ostream.hpp>

namespace Common {
    namespace Logger {
        enum SeverityLevel 
        {
            trace,
            debug,
            info,
            error,
            critical
        };

        struct Configuration {
            SeverityLevel severity_level;
        };

        void InitializeLogger(const Configuration& configuration);

        boost::log::sources::severity_logger<SeverityLevel> GetNewLogger();
    }
}
