add_executable(phjointest main.cpp)
target_compile_features(phjointest PRIVATE cxx_std_17)
target_include_directories(phjointest PRIVATE ${GTEST_INCLUDE_DIRS})
target_include_directories(phjointest PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(phjointest PRIVATE GTest::GTest GTest::Main)
target_link_libraries(phjointest PRIVATE phjoinlib)
if(PHJ_USE_MIMALLOC)
	target_compile_definitions(phjointest PRIVATE PHJ_USE_MIMALLOC)

	# Copy all Mimalloc DLLs to the directory containing phjointest executable
	if(WIN32)
		add_custom_command(
			TARGET phjointest POST_BUILD
			COMMAND ${CMAKE_COMMAND} -E copy_directory
					${PHJ_MIMALLOC_DLL_DIR}
					$<TARGET_FILE_DIR:phjointest>)
	endif()
endif()

add_test(
    NAME phjointest
    COMMAND phjointest
)