# SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
# SPDX-License-Identifier: LGPL-3.0-or-later

add_library(_core INTERFACE)
target_link_libraries(_core INTERFACE
    tango-bulk-transport pybind11::module Python3::Module)
