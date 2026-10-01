# SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
# SPDX-License-Identifier: LGPL-3.0-or-later

find_package(Python3 COMPONENTS Interpreter Development.Module REQUIRED)
find_package(pybind11 CONFIG REQUIRED)

pybind11_add_module(_core MODULE _core.cpp)
target_include_directories(_core PRIVATE "${PROJECT_SOURCE_DIR}/src/transport")
target_link_libraries(_core PRIVATE tango-bulk-transport)
if(TANGO_BULK_CUDA)
    target_link_libraries(_core PRIVATE tango-bulk-cuda) # memory="cuda:N" needs the runtime
endif()
set_target_properties(_core PROPERTIES
    LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/python/tango_bulk")

set(_python_build_package "${CMAKE_BINARY_DIR}/python/tango_bulk")
file(MAKE_DIRECTORY "${_python_build_package}")
configure_file(tango_bulk/__init__.py "${_python_build_package}/__init__.py" COPYONLY)
configure_file(tango_bulk/_async.py "${_python_build_package}/_async.py" COPYONLY)
configure_file(tango_bulk/subscription.py "${_python_build_package}/subscription.py" COPYONLY)

set(_python_install_dir
    "${CMAKE_INSTALL_LIBDIR}/python${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR}/site-packages/tango_bulk")
install(TARGETS _core LIBRARY DESTINATION "${_python_install_dir}")
install(FILES tango_bulk/__init__.py tango_bulk/_async.py tango_bulk/subscription.py
    DESTINATION "${_python_install_dir}")
