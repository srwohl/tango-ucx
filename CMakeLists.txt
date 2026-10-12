# SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-ucx project
# SPDX-License-Identifier: LGPL-3.0-or-later

cmake_minimum_required(VERSION 3.18)
project(tango-ucx VERSION 0.1.0 LANGUAGES CXX)

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)
include(CTest)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_package(CUDAToolkit QUIET)
option(TANGO_UCX_CUDA "Build CUDA source and receive support when the toolkit is available" ${CUDAToolkit_FOUND})
set(TANGO_UCX_SANITIZERS "" CACHE STRING "Comma-separated -fsanitize list")
option(TANGO_UCX_PROFILING "Keep frame pointers and debug symbols, so perf can attribute time" OFF)

find_package(Threads REQUIRED)
find_package(ucx 1.21 CONFIG REQUIRED)
find_package(nlohmann_json 3.12 CONFIG REQUIRED)
find_package(Tango CONFIG REQUIRED)
if(TANGO_UCX_CUDA)
    find_package(CUDAToolkit REQUIRED)
endif()
if(BUILD_TESTING)
    find_package(Catch2 3 CONFIG REQUIRED)
endif()

if(TANGO_UCX_SANITIZERS)
    add_compile_options(-fsanitize=${TANGO_UCX_SANITIZERS} -fno-omit-frame-pointer)
    add_link_options(-fsanitize=${TANGO_UCX_SANITIZERS})
endif()
if(TANGO_UCX_PROFILING)
    add_compile_options(-fno-omit-frame-pointer -g)
endif()

add_subdirectory(src/transport)
add_subdirectory(src/tango)
if(TANGO_UCX_CUDA)
    add_subdirectory(src/cuda)
endif()
add_subdirectory(python)
if(BUILD_TESTING)
    add_subdirectory(tests)
endif()

install(TARGETS tango-ucx-transport tango-ucx EXPORT tango-ucx-targets)
if(TANGO_UCX_CUDA)
    install(TARGETS tango-ucx-cuda EXPORT tango-ucx-targets
        OBJECTS DESTINATION "${CMAKE_INSTALL_LIBDIR}/tango-ucx")
endif()
install(DIRECTORY include/ DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
    FILES_MATCHING PATTERN "*.h" PATTERN "*.hpp")
set(_config_dir "${CMAKE_INSTALL_LIBDIR}/cmake/tango-ucx")
install(EXPORT tango-ucx-targets NAMESPACE tango-ucx:: DESTINATION "${_config_dir}")
configure_package_config_file(cmake/tango-ucx-config.cmake.in
    "${CMAKE_CURRENT_BINARY_DIR}/tango-ucx-config.cmake" INSTALL_DESTINATION "${_config_dir}")
write_basic_package_version_file("${CMAKE_CURRENT_BINARY_DIR}/tango-ucx-config-version.cmake"
    COMPATIBILITY SameMinorVersion)
install(FILES "${CMAKE_CURRENT_BINARY_DIR}/tango-ucx-config.cmake"
    "${CMAKE_CURRENT_BINARY_DIR}/tango-ucx-config-version.cmake" DESTINATION "${_config_dir}")
