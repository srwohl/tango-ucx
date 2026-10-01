# SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
# SPDX-License-Identifier: LGPL-3.0-or-later

# pytest exits 5 when no test carries the gpu marker; CTest reports that as skipped.
add_test(NAME test_python_gpu
    COMMAND "${CMAKE_COMMAND}" -E env
        "PYTHONPATH=${CMAKE_BINARY_DIR}/python"
        "TANGO_BULK_FIXTURE_DEVICE=$<TARGET_FILE:fixture_device>"
        "${Python3_EXECUTABLE}" -m pytest -m gpu "${PROJECT_SOURCE_DIR}/python/tests")
set_tests_properties(test_python_gpu PROPERTIES LABELS gpu SKIP_RETURN_CODE 5)
