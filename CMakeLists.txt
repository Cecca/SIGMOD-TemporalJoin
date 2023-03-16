cmake_minimum_required(VERSION 3.5)
project(DurableJoin)

set(CMAKE_CXX_STANDARD 11)
include_directories(include)

set(SOURCE_FILES 
	main.cpp
	data_loader.h solution.h solution.cpp tables.h utility.h utility.cpp interval_tree.h)

add_executable(DurableJoin ${SOURCE_FILES})

add_executable(MyMain 
	mymain.cpp
	data_loader.h solution.h solution.cpp tables.h utility.h utility.cpp interval_tree.h)

