# SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
# SPDX-License-Identifier: LGPL-3.0-or-later

include(Catch)
function(add_bulk_test name source label)
    add_executable(${name} ${source})
    target_include_directories(${name} PRIVATE "${PROJECT_SOURCE_DIR}/src/transport" "${CMAKE_CURRENT_SOURCE_DIR}")
    target_link_libraries(${name} PRIVATE tango-bulk-transport Catch2::Catch2WithMain nlohmann_json::nlohmann_json)
    catch_discover_tests(${name} PROPERTIES LABELS "${label}")
endfunction()

add_bulk_test(test_plan transport/test_plan.cpp unit)
add_bulk_test(test_wire transport/test_wire.cpp unit)
add_bulk_test(test_ucx transport/test_ucx.cpp ucx)
add_bulk_test(test_publisher transport/test_publisher.cpp ucx)
add_bulk_test(test_subscription transport/test_subscription.cpp ucx)
add_bulk_test(test_failure transport/test_failure.cpp ucx)
add_bulk_test(test_tables transport/test_tables.cpp ucx)

add_executable(fixture_device tango/fixture_device.cpp)
target_link_libraries(fixture_device PRIVATE tango-bulk)
add_executable(test_tango tango/test_tango.cpp)
target_include_directories(test_tango PRIVATE "${PROJECT_SOURCE_DIR}/src/transport" "${CMAKE_CURRENT_SOURCE_DIR}")
target_link_libraries(test_tango PRIVATE tango-bulk Catch2::Catch2WithMain nlohmann_json::nlohmann_json)
target_compile_definitions(test_tango PRIVATE TANGO_BULK_FIXTURE_DEVICE="$<TARGET_FILE:fixture_device>")
add_dependencies(test_tango fixture_device)
catch_discover_tests(test_tango PROPERTIES LABELS tango)

find_package(Python3 COMPONENTS Interpreter REQUIRED)
add_test(NAME test_python
    COMMAND "${CMAKE_COMMAND}" -E env
        "PYTHONPATH=${CMAKE_BINARY_DIR}/python"
        "TANGO_BULK_FIXTURE_DEVICE=$<TARGET_FILE:fixture_device>"
        "${Python3_EXECUTABLE}" -m pytest "${PROJECT_SOURCE_DIR}/python/tests")
set_tests_properties(test_python PROPERTIES LABELS python)

if(TANGO_BULK_CUDA)
    add_subdirectory(gpu)
endif()
