cmake_minimum_required(VERSION 3.11)
project(omcs)
include(FetchContent)

# No in-source build
if("${CMAKE_SOURCE_DIR}" STREQUAL "${CMAKE_BINARY_DIR}")
  message(FATAL_ERROR "In-source builds are not allowed.")
endif("${CMAKE_SOURCE_DIR}" STREQUAL "${CMAKE_BINARY_DIR}")

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "Debug" CACHE STRING "" FORCE)
  message("-- No build mode chosen - using Debug by default")
endif(NOT CMAKE_BUILD_TYPE)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wfatal-errors -pedantic -fPIC -march=native")
set(CMAKE_ENABLE_COMPILE_COMMANDS "ON")

if (${CMAKE_BUILD_TYPE} STREQUAL "Debug")
  message("-- Debug mode, sanitizers enabled")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=undefined")
elseif (${CMAKE_BUILD_TYPE} STREQUAL "RelWithDebInfo")
  string(REPLACE "-O2" "-O3 -Ofast" CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO}")
elseif (${CMAKE_BUILD_TYPE} STREQUAL "Release")
  message("-- Release mode, all optimization enabled")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -Ofast")
  find_package(PkgConfig REQUIRED)
  pkg_check_modules(JEMALLOC jemalloc)
  pkg_search_module(JEMALLOC REQUIRED jemalloc)
  include_directories(${JEMALLOC_INCLUDE_DIRS})
endif ()

enable_testing()

include_directories(${CMAKE_SOURCE_DIR})

list(APPEND omcs_impls OMCS_LOCK)
list(APPEND omcs_impl_suffixes "")

add_subdirectory(indexes)
add_subdirectory(latches)
add_subdirectory(wrappers)
