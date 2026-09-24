# SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
# SPDX-License-Identifier: LGPL-3.0-or-later

include(Catch)
function(add_bulk_test name source label)
    add_executable(${name} ${source})
    target_include_directories(${name} PRIVATE "${PROJECT_SOURCE_DIR}/src/transport" "${CMAKE_CURRENT_SOURCE_DIR}")
    target_link_libraries(${name} PRIVATE tango-bulk-transport Catch2::Catch2WithMain)
    catch_discover_tests(${name} PROPERTIES LABELS "${label}")
endfunction()

add_bulk_test(test_plan transport/test_plan.cpp unit)
add_bulk_test(test_wire transport/test_wire.cpp unit)
add_bulk_test(test_ucx transport/test_ucx.cpp ucx)
add_bulk_test(test_publisher transport/test_publisher.cpp ucx)
add_bulk_test(test_subscription transport/test_subscription.cpp ucx)
add_bulk_test(test_failure transport/test_failure.cpp ucx)
