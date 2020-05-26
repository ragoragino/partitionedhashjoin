add_library(phjoinlib STATIC
	"Common/Logger.cpp" 
	"Common/Table.cpp" 
	"Common/ThreadPool.cpp" 
	"Common/Random.cpp"
	"DataGenerator/Zipf.cpp" 
	"DataGenerator/Sequential.cpp"
	)

target_compile_features(phjoinlib PUBLIC cxx_std_17)
target_include_directories(phjoinlib PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_include_directories(phjoinlib PUBLIC ${Boost_INCLUDE_DIR})
target_link_libraries(phjoinlib PUBLIC ${Boost_LIBRARIES})

target_link_libraries(phjoinlib PUBLIC xxhash_cpp::xxhash_cpp)

add_executable(phjoin main.cpp )
target_link_libraries(phjoin phjoinlib)
target_compile_features(phjoin PUBLIC cxx_std_17)
target_include_directories(phjoin PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

# Allow profiling when on MSVC
IF(MSVC)
	SET_TARGET_PROPERTIES(phjoin PROPERTIES LINK_FLAGS "/PROFILE")
ENDIF()