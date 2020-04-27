#include "Common/Logger.h"

#include <thread>

void concurrentLoggingTest(int id)
{
    auto logger = Common::Logger::GetNewLogger();

    for (int i = 0; i != 10000; i++) {
        BOOST_LOG_SEV(logger, Common::Logger::SeverityLevel::info) << "ID: " << id << "; An info message: " << i << ";";
    }
}

int main(int argc, char** argv)
{
    Common::Logger::Configuration logger_configuration{};
    logger_configuration.severity_level = Common::Logger::SeverityLevel::info;

    Common::Logger::InitializeLogger(logger_configuration);

    std::thread t1(concurrentLoggingTest, 1);
    std::thread t2(concurrentLoggingTest, 2);

    t1.join();
    t2.join();

    return 0;
}