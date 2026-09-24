# SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
# SPDX-License-Identifier: LGPL-3.0-or-later

add_library(tango-bulk-transport STATIC
    wire.cpp memory.cpp ucx.cpp publisher.cpp subscription.cpp batch.cpp)
find_library(UCX_UCM_LIBRARY NAMES ucm REQUIRED)
add_library(tango-bulk::transport ALIAS tango-bulk-transport)
set_target_properties(tango-bulk-transport PROPERTIES EXPORT_NAME transport)
target_include_directories(tango-bulk-transport PUBLIC
    "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>"
    "$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}")
target_compile_features(tango-bulk-transport PUBLIC cxx_std_20)
target_link_libraries(tango-bulk-transport PUBLIC ucx::ucp ucx::ucs Threads::Threads PRIVATE "${UCX_UCM_LIBRARY}"
    nlohmann_json::nlohmann_json)
