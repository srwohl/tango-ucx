# SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
# SPDX-License-Identifier: LGPL-3.0-or-later

# pytest exits 5 when no test carries the gpu marker; CTest reports that as skipped.
add_test(NAME test_python_gpu
    COMMAND "${CMAKE_COMMAND}" -E env
        "PYTHONPATH=${CMAKE_BINARY_DIR}/python"
        "TANGO_BULK_FIXTURE_DEVICE=$<TARGET_FILE:fixture_device>"
        "${Python3_EXECUTABLE}" -m pytest -m gpu "${PROJECT_SOURCE_DIR}/python/tests")
set_tests_properties(test_python_gpu PROPERTIES LABELS gpu SKIP_RETURN_CODE 5)

add_executable(test_runtime test_runtime.cpp)
target_include_directories(test_runtime PRIVATE "${PROJECT_SOURCE_DIR}/src/transport")
target_link_libraries(test_runtime PRIVATE tango-bulk-cuda CUDA::cuda_driver Catch2::Catch2WithMain)
catch_discover_tests(test_runtime PROPERTIES LABELS gpu)

add_executable(test_receive test_receive.cpp)
target_include_directories(test_receive PRIVATE "${PROJECT_SOURCE_DIR}/src/transport" "${PROJECT_SOURCE_DIR}/tests/transport")
target_link_libraries(test_receive PRIVATE tango-bulk-cuda CUDA::cudart Catch2::Catch2WithMain nlohmann_json::nlohmann_json)
catch_discover_tests(test_receive PROPERTIES LABELS gpu)
