# https://pabloariasal.github.io/2018/02/19/its-time-to-do-cmake-right/
# https://rix0r.nl/blog/2015/08/13/cmake-guide/

cmake_minimum_required(VERSION 3.8 FATAL_ERROR)

project(PartitionedHashJoin VERSION 1.0.0 LANGUAGES CXX)

option(PHJ_USE_MIMALLOC "Use Mimalloc dynamic library" ON)

set(CMAKE_CXX_STANDARD 17)

find_package(Boost 1.72.0 COMPONENTS program_options log REQUIRED)
find_package(xxhash 0.7 REQUIRED)

string(REPLACE "-O2" "-O0" CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE}")

if(PHJ_USE_MIMALLOC)
	find_package(mimalloc 1.6 REQUIRED)
	if(MSVC)
		set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} /MD")
		set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} /MDd")
	endif()
endif()

add_subdirectory(src)

IF(CMAKE_BUILD_TYPE EQUAL "DEBUG")
	enable_testing()
	find_package(GTest 1.11.0 REQUIRED)
	add_subdirectory(tests)
ENDIF()