# SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
# SPDX-License-Identifier: LGPL-3.0-or-later

cmake_minimum_required(VERSION 3.18)
project(tango-bulk VERSION 0.1.0 LANGUAGES CXX)

include(GNUInstallDirs)
include(CTest)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(TANGO_BULK_CUDA "Build CUDA receive support when the toolkit is available" OFF)
set(TANGO_BULK_SANITIZERS "" CACHE STRING "Comma-separated -fsanitize list")

find_package(Threads REQUIRED)
find_package(ucx 1.21 CONFIG REQUIRED)
find_package(nlohmann_json 3.12 CONFIG REQUIRED)
find_package(Tango CONFIG REQUIRED)
if(BUILD_TESTING)
    find_package(Catch2 3 CONFIG REQUIRED)
endif()

if(TANGO_BULK_SANITIZERS)
    add_compile_options(-fsanitize=${TANGO_BULK_SANITIZERS} -fno-omit-frame-pointer)
    add_link_options(-fsanitize=${TANGO_BULK_SANITIZERS})
endif()

add_subdirectory(src/transport)
add_subdirectory(src/tango)
if(BUILD_TESTING)
    add_subdirectory(tests)
endif()

install(TARGETS tango-bulk-transport tango-bulk EXPORT tango-bulk-targets)
install(DIRECTORY include/ DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
    FILES_MATCHING PATTERN "*.h" PATTERN "*.hpp")
install(EXPORT tango-bulk-targets NAMESPACE tango-bulk::
    DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/tango-bulk")
