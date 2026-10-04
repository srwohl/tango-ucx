# SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-ucx project
# SPDX-License-Identifier: LGPL-3.0-or-later

include(Catch)
function(add_ucx_test name source label)
    add_executable(${name} ${source})
    target_include_directories(${name} PRIVATE "${PROJECT_SOURCE_DIR}/src/transport" "${CMAKE_CURRENT_SOURCE_DIR}")
    target_link_libraries(${name} PRIVATE tango-ucx-transport Catch2::Catch2WithMain nlohmann_json::nlohmann_json)
    catch_discover_tests(${name} PROPERTIES LABELS "${label}")
endfunction()

add_ucx_test(test_plan transport/test_plan.cpp unit)
add_ucx_test(test_wire transport/test_wire.cpp unit)
add_ucx_test(test_ucp transport/test_ucp.cpp ucx)
add_ucx_test(test_publisher transport/test_publisher.cpp ucx)
add_ucx_test(test_subscription transport/test_subscription.cpp ucx)
add_ucx_test(test_failure transport/test_failure.cpp ucx)
add_ucx_test(test_tables transport/test_tables.cpp ucx)
add_ucx_test(test_opaque transport/test_opaque.cpp ucx)
add_ucx_test(test_pull transport/test_pull.cpp ucx)

add_executable(fixture_device tango/fixture_device.cpp)
target_link_libraries(fixture_device PRIVATE tango-ucx)
add_executable(test_tango tango/test_tango.cpp)
target_include_directories(test_tango PRIVATE "${PROJECT_SOURCE_DIR}/src/transport" "${CMAKE_CURRENT_SOURCE_DIR}")
target_link_libraries(test_tango PRIVATE tango-ucx Catch2::Catch2WithMain nlohmann_json::nlohmann_json)
target_compile_definitions(test_tango PRIVATE TANGO_UCX_FIXTURE_DEVICE="$<TARGET_FILE:fixture_device>")
add_dependencies(test_tango fixture_device)
catch_discover_tests(test_tango PROPERTIES LABELS tango)

find_package(Python3 COMPONENTS Interpreter REQUIRED)
# An uninstrumented python loads _core, so the ASan runtime is preloaded. CPython's own
# allocations at exit would drown a leak report, so leaks are checked in the C++ tests only.
set(python_sanitizer_env "")
if(TANGO_UCX_SANITIZERS MATCHES "address")
    execute_process(COMMAND "${CMAKE_CXX_COMPILER}" -print-file-name=libasan.so
        OUTPUT_VARIABLE asan_runtime OUTPUT_STRIP_TRAILING_WHITESPACE)
    file(REAL_PATH "${asan_runtime}" asan_runtime)
    set(python_sanitizer_env "LD_PRELOAD=${asan_runtime}" "ASAN_OPTIONS=detect_leaks=0")
endif()
add_test(NAME test_python
    COMMAND "${CMAKE_COMMAND}" -E env ${python_sanitizer_env}
        "PYTHONPATH=${CMAKE_BINARY_DIR}/python"
        "TANGO_UCX_FIXTURE_DEVICE=$<TARGET_FILE:fixture_device>"
        "${Python3_EXECUTABLE}" -m pytest "${PROJECT_SOURCE_DIR}/python/tests")
set_tests_properties(test_python PROPERTIES LABELS python)

# A project outside the tree builds against the installed package, and Python imports the
# installed one. A consumer built without the sanitizer would not link a sanitized archive.
if(NOT TANGO_UCX_SANITIZERS)
    set(stage "${CMAKE_BINARY_DIR}/stage")
    add_test(NAME install_stage
        COMMAND "${CMAKE_COMMAND}" --install "${CMAKE_BINARY_DIR}" --prefix "${stage}")
    add_test(NAME install_consumer
        COMMAND "${CMAKE_CTEST_COMMAND}" --build-and-test "${CMAKE_CURRENT_SOURCE_DIR}/install"
            "${stage}/consumer" --build-generator "${CMAKE_GENERATOR}" --build-options
            "-DCMAKE_PREFIX_PATH=${stage}" "-DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}"
            --test-command consumer)
    add_test(NAME install_python
        COMMAND "${CMAKE_COMMAND}" -E env "PYTHONPATH=${stage}/${TANGO_UCX_PYTHON_SITE}"
            "${Python3_EXECUTABLE}" -c
            "import tango_ucx; assert tango_ucx.__file__.startswith('${stage}/'), tango_ucx.__file__")
    add_test(NAME install_remove COMMAND "${CMAKE_COMMAND}" -E rm -rf "${stage}")
    set_tests_properties(install_stage PROPERTIES FIXTURES_SETUP stage LABELS install)
    set_tests_properties(install_remove PROPERTIES FIXTURES_CLEANUP stage LABELS install)
    set_tests_properties(install_consumer install_python PROPERTIES
        FIXTURES_REQUIRED stage LABELS install)
endif()

if(TANGO_UCX_CUDA)
    add_subdirectory(gpu)
endif()
