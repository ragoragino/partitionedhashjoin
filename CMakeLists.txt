# https://pabloariasal.github.io/2018/02/19/its-time-to-do-cmake-right/
# https://rix0r.nl/blog/2015/08/13/cmake-guide/

cmake_minimum_required(VERSION 3.8 FATAL_ERROR)

project(PartitionedHashJoin VERSION 1.0.0 LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 14)

enable_testing()

find_package(Boost 1.72.0 COMPONENTS program_options log REQUIRED)
find_package(GTest 1.11.0 REQUIRED)

message(${Boost_LIBRARY_DIRS})

add_subdirectory(src)
add_subdirectory(tests)
