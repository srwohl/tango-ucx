# SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
# SPDX-License-Identifier: LGPL-3.0-or-later

add_library(tango-bulk STATIC device.cpp subscribe.cpp)
add_library(tango-bulk::bulk ALIAS tango-bulk)
set_target_properties(tango-bulk PROPERTIES EXPORT_NAME bulk)
target_include_directories(tango-bulk PUBLIC
    "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>"
    "$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>"
    PRIVATE "${PROJECT_SOURCE_DIR}/src/transport")
target_compile_features(tango-bulk PUBLIC cxx_std_20)
target_link_libraries(tango-bulk PUBLIC tango-bulk-transport Tango::Tango)
