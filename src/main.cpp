#include "Common/Logger.h"

#include <boost/log/core.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/sinks/text_file_backend.hpp>
#include <boost/log/utility/setup/file.hpp>
#include <boost/log/utility/setup/common_attributes.hpp>
#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/sources/record_ostream.hpp>

int main(int argc, char** argv)
{
    Common::Logger::Configuration logger_configuration{};
    logger_configuration.severity_level = Common::Logger::SeverityLevel::trace;

    Common::Logger::InitializeLogger(logger_configuration);

    auto logger = Common::Logger::GetNewLogger();

    BOOST_LOG_SEV(logger, Common::Logger::SeverityLevel::info) << "A regular message";

    return 0;
}