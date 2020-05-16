add_executable(phjointest main.cpp)
target_compile_features(phjointest PRIVATE cxx_std_17)
target_include_directories(phjointest PRIVATE ${GTEST_INCLUDE_DIRS})
target_include_directories(phjointest PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(phjointest PRIVATE GTest::GTest GTest::Main)
target_link_libraries(phjointest PRIVATE phjoinlib)

add_test(
    NAME phjointest
    COMMAND phjointest
)