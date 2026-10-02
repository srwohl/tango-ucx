# SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-ucx project
# SPDX-License-Identifier: LGPL-3.0-or-later

add_library(tango-ucx STATIC device.cpp subscribe.cpp)
add_library(tango-ucx::ucx ALIAS tango-ucx)
set_target_properties(tango-ucx PROPERTIES EXPORT_NAME ucx)
target_include_directories(tango-ucx PUBLIC
    "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>"
    "$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>"
    PRIVATE "${PROJECT_SOURCE_DIR}/src/transport")
target_compile_features(tango-ucx PUBLIC cxx_std_20)
target_link_libraries(tango-ucx PUBLIC tango-ucx-transport Tango::Tango)
