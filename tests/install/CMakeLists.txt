# SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-ucx project
# SPDX-License-Identifier: LGPL-3.0-or-later

# A project outside the tree: the install tests configure and build it against the installed
# package, and run it.
cmake_minimum_required(VERSION 3.18)
project(tango-ucx-consumer LANGUAGES CXX)

find_package(tango-ucx CONFIG REQUIRED)

add_executable(consumer consumer.cpp)
target_link_libraries(consumer PRIVATE tango-ucx::ucx)
