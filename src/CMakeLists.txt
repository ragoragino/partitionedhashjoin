add_executable(phjoin main.cpp "Common/Logger.cpp")
target_compile_features(phjoin PUBLIC cxx_std_11)
target_include_directories(phjoin PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_include_directories(phjoin PUBLIC ${Boost_INCLUDE_DIR})
target_link_libraries(phjoin PUBLIC ${Boost_LIBRARIES})