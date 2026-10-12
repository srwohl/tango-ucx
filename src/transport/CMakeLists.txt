# SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-ucx project
# SPDX-License-Identifier: LGPL-3.0-or-later

add_library(tango-ucx-transport STATIC
    wire.cpp memory.cpp ucp.cpp topology.cpp publisher.cpp subscription.cpp batch.cpp)
# UCX's package has no target for libucm. A named one keeps this build's path to it out of
# the exported targets; the package's config finds it again.
find_library(UCX_UCM_LIBRARY NAMES ucm HINTS "${ucx_DIR}/../.." REQUIRED)
add_library(tango-ucx::ucm UNKNOWN IMPORTED GLOBAL)
set_target_properties(tango-ucx::ucm PROPERTIES IMPORTED_LOCATION "${UCX_UCM_LIBRARY}")
add_library(tango-ucx::transport ALIAS tango-ucx-transport)
set_target_properties(tango-ucx-transport PROPERTIES EXPORT_NAME transport)
target_include_directories(tango-ucx-transport PUBLIC
    "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>"
    "$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}")
target_compile_features(tango-ucx-transport PUBLIC cxx_std_20)
target_link_libraries(tango-ucx-transport PUBLIC ucx::ucp ucx::ucs Threads::Threads
    PRIVATE tango-ucx::ucm nlohmann_json::nlohmann_json)
