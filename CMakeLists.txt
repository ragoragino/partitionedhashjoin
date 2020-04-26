# https://pabloariasal.github.io/2018/02/19/its-time-to-do-cmake-right/
# https://rix0r.nl/blog/2015/08/13/cmake-guide/

cmake_minimum_required(VERSION 3.8 FATAL_ERROR)

project(PartitionedHashJoin VERSION 1.0.0 LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 14)

enable_testing()

set(Boost_USE_STATIC_LIBS ON)
set(Boost_USE_STATIC_RUNTIME ON)
find_package(Boost 1.72.0 COMPONENTS program_options log REQUIRED)
find_package(GTest REQUIRED)

add_subdirectory(src)
add_subdirectory(tests)