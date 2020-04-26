add_executable(phjointest main.cpp)
target_compile_features(phjointest PRIVATE cxx_std_11)
target_include_directories(phjointest PRIVATE ${GTEST_INCLUDE_DIRS})
target_link_libraries(phjointest PRIVATE GTest::GTest GTest::Main)

add_test(
    NAME phjointest
    COMMAND phjointest
)