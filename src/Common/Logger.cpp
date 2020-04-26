#pragma once

#include "./Common/Logger.h"

#include <mutex>
#include <iostream>
#include <iomanip>
#include <boost/smart_ptr/shared_ptr.hpp>
#include <boost/core/null_deleter.hpp>
#include <boost/log/core.hpp>
#include <boost/log/attributes/clock.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/sinks/sync_frontend.hpp>
#include <boost/log/sinks/text_ostream_backend.hpp>
#include <boost/log/sources/severity_channel_logger.hpp>
#include <boost/log/sources/record_ostream.hpp>
#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/expressions/formatters/date_time.hpp>
#include <boost/log/support/date_time.hpp>

BOOST_LOG_ATTRIBUTE_KEYWORD(severity, "Severity", Common::Logger::SeverityLevel)
BOOST_LOG_ATTRIBUTE_KEYWORD(timestamp, "Timestamp", boost::posix_time::ptime)

namespace Common {
    namespace Logger {
        typedef boost::log::sinks::synchronous_sink<boost::log::sinks::text_ostream_backend> sink_t;

        struct LoggerOwner {
            Configuration configuration;
            boost::shared_ptr<sink_t> sink;
        };

        std::unique_ptr<LoggerOwner> default_log_owner;
        std::mutex default_configuration_mutex;

      
        void InitializeLogger(const Configuration& configuration) {
            const std::lock_guard<std::mutex> lock(default_configuration_mutex);

            if (!default_log_owner) {
                default_log_owner = std::make_unique<LoggerOwner>();
                default_log_owner->configuration = configuration;

                auto backend = boost::make_shared<boost::log::sinks::text_ostream_backend>();
                backend->add_stream(boost::shared_ptr<std::ostream>(&std::clog, boost::null_deleter()));

                default_log_owner->sink = boost::make_shared<sink_t>(backend);

                default_log_owner->sink->set_filter(severity > default_log_owner->configuration.severity_level);
                default_log_owner->sink->set_formatter(boost::log::expressions::stream << severity
                    << " (" << boost::log::expressions::format_date_time(timestamp, "%H:%M:%S") << ")" <<
                    ": " << boost::log::expressions::smessage);

                boost::log::core::get()->add_sink(default_log_owner->sink);
                boost::log::core::get()->add_global_attribute("Timestamp", boost::log::attributes::local_clock{});
            }
            else {
                throw std::invalid_argument("Cannot continue with initializing logger, because it is already initialized.");
            }
        }

        boost::log::sources::severity_logger<SeverityLevel> GetNewLogger() {
            if (!default_log_owner) {
                throw std::invalid_argument("Cannot create new logger. Logger was not initialized.");
            }

            boost::log::sources::severity_logger<SeverityLevel> logger{};

            return logger;
        }
    }
}
