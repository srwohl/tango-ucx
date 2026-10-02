# SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-ucx project
# SPDX-License-Identifier: LGPL-3.0-or-later

cmake_minimum_required(VERSION 3.18)
project(tango-ucx-examples LANGUAGES CXX)

find_package(tango-ucx CONFIG REQUIRED)

# Writes an opaque UCX stream's batches as archive chunks; archive.h is the chunk format.
add_executable(archive archive.cpp)
target_link_libraries(archive PRIVATE tango-ucx::ucx)
