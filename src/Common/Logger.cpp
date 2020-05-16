#include "Logger.hpp"

#include <boost/core/null_deleter.hpp>
#include <boost/log/attributes/clock.hpp>
#include <boost/log/core.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/expressions/formatters/stream.hpp>
#include <boost/log/expressions/keyword.hpp>
#include <boost/log/sinks/sync_frontend.hpp>
#include <boost/log/sinks/text_ostream_backend.hpp>
#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/support/date_time.hpp>
#include <boost/smart_ptr/shared_ptr.hpp>
#include <iomanip>
#include <iostream>
#include <mutex>

// https://theboostcpplibraries.com/boost.log
// https://www.boost.org/doc/libs/1_56_0/libs/log/example/doc/tutorial_filtering.cpp

BOOST_LOG_ATTRIBUTE_KEYWORD(severity, "Severity", Common::SeverityLevel)
BOOST_LOG_ATTRIBUTE_KEYWORD(timestamp, "Timestamp", boost::posix_time::ptime)

namespace Common {
typedef boost::log::sinks::synchronous_sink<boost::log::sinks::text_ostream_backend> sink_t;

std::ostream& operator<<(std::ostream& stream, SeverityLevel level) {
    static const char* strings[] = {"trace", "debug", "info", "error", "critical"};

    if (static_cast<size_t>(level) < sizeof(strings) / sizeof(*strings)) {
        stream << strings[level];
    } else {
        stream << static_cast<size_t>(level);
    }

    return stream;
}

struct LoggerOwner {
    LoggerConfiguration configuration;
    boost::shared_ptr<sink_t> sink;
};

std::unique_ptr<LoggerOwner> default_log_owner;
std::mutex default_configuration_mutex;

void InitializeLogger(const LoggerConfiguration& configuration) {
    const std::lock_guard<std::mutex> lock(default_configuration_mutex);

    if (!default_log_owner) {
        default_log_owner = std::make_unique<LoggerOwner>();
        default_log_owner->configuration = configuration;

        auto backend = boost::make_shared<boost::log::sinks::text_ostream_backend>();
        backend->add_stream(boost::shared_ptr<std::ostream>(&std::clog, boost::null_deleter()));

        default_log_owner->sink = boost::make_shared<sink_t>(backend);

        default_log_owner->sink->set_filter(severity >=
                                            default_log_owner->configuration.severity_level);
        default_log_owner->sink->set_formatter(
            boost::log::expressions::stream
            << severity << " (" << boost::log::expressions::format_date_time(timestamp, "%H:%M:%S")
            << ")"
            << ": " << boost::log::expressions::smessage);

        boost::log::core::get()->add_sink(default_log_owner->sink);
        boost::log::core::get()->add_global_attribute("Timestamp",
                                                      boost::log::attributes::local_clock{});
    } else {
        throw std::runtime_error(
            "Cannot continue with initializing logger, because it is already initialized.");
    }
}

boost::log::sources::severity_logger<SeverityLevel> GetNewLogger() {
    if (!default_log_owner) {
        throw std::runtime_error("Cannot create new logger. Logger was not initialized.");
    }

    return boost::log::sources::severity_logger<SeverityLevel>{};
}
}  // namespace Common
