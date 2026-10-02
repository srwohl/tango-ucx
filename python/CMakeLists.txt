# SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-ucx project
# SPDX-License-Identifier: LGPL-3.0-or-later

find_package(Python3 COMPONENTS Interpreter Development.Module REQUIRED)
find_package(pybind11 CONFIG REQUIRED)

pybind11_add_module(_core MODULE _core.cpp)
target_include_directories(_core PRIVATE "${PROJECT_SOURCE_DIR}/src/transport")
target_link_libraries(_core PRIVATE tango-ucx-transport)
if(TANGO_UCX_CUDA)
    target_link_libraries(_core PRIVATE tango-ucx-cuda) # memory="cuda:N" needs the runtime
endif()
set_target_properties(_core PROPERTIES
    LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/python/tango_ucx")

set(_python_build_package "${CMAKE_BINARY_DIR}/python/tango_ucx")
file(MAKE_DIRECTORY "${_python_build_package}")
configure_file(tango_ucx/__init__.py "${_python_build_package}/__init__.py" COPYONLY)
configure_file(tango_ucx/_async.py "${_python_build_package}/_async.py" COPYONLY)
configure_file(tango_ucx/subscription.py "${_python_build_package}/subscription.py" COPYONLY)

set(_python_install_dir
    "${CMAKE_INSTALL_LIBDIR}/python${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR}/site-packages/tango_ucx")
install(TARGETS _core LIBRARY DESTINATION "${_python_install_dir}")
install(FILES tango_ucx/__init__.py tango_ucx/_async.py tango_ucx/subscription.py
    DESTINATION "${_python_install_dir}")
