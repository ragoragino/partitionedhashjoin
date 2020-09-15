add_library(phjoinlib STATIC
	"Common/Logger.cpp" 
	"Common/Table.cpp" 
	"Common/ThreadPool.cpp" 
	"Common/Random.cpp"
	"Common/Allocator.cpp" 
	"Common/IThreadPool.cpp" 
	"Common/Configuration.cpp"
	 
	"DataGenerator/Zipf.cpp" 
	"DataGenerator/Sequential.cpp"
	"HashTables/LinearProbing.cpp"
	"HashTables/SeparateChaining.cpp")
target_compile_features(phjoinlib PUBLIC cxx_std_17)
target_include_directories(phjoinlib PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(phjoinlib PUBLIC ${Boost_INCLUDE_DIR})
target_link_libraries(phjoinlib PUBLIC ${Boost_LIBRARIES})
target_link_libraries(phjoinlib PUBLIC xxHash::xxhash)
target_link_libraries(phjoinlib PUBLIC mimalloc)

add_executable(phjoin "main.cpp")
target_link_libraries(phjoin PRIVATE phjoinlib)
if(PHJ_USE_MIMALLOC)
	target_compile_definitions(phjoin PRIVATE PHJ_USE_MIMALLOC)

	# Copy all Mimalloc DLLs to the directory containing phjoin executable
	if(WIN32)
		add_custom_command(
			TARGET phjoin POST_BUILD
			COMMAND ${CMAKE_COMMAND} -E copy_directory
					${PHJ_MIMALLOC_DLL_DIR}
					$<TARGET_FILE_DIR:phjoin>)
	endif()
endif()

# Allow profiling when on MSVC
if(MSVC)
	set_target_properties(phjoin PROPERTIES LINK_FLAGS "/PROFILE")
endif()