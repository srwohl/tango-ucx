# SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
# SPDX-License-Identifier: LGPL-3.0-or-later

add_library(tango-bulk INTERFACE)
add_library(tango-bulk::bulk ALIAS tango-bulk)
set_target_properties(tango-bulk PROPERTIES EXPORT_NAME bulk)
target_include_directories(tango-bulk INTERFACE
    "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>"
    "$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>")
target_compile_features(tango-bulk INTERFACE cxx_std_17)
target_link_libraries(tango-bulk INTERFACE
    tango-bulk-transport Tango::Tango)
