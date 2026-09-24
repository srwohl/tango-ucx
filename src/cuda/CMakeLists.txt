# SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
# SPDX-License-Identifier: LGPL-3.0-or-later

add_library(tango-bulk-cuda INTERFACE)
add_library(tango-bulk::cuda ALIAS tango-bulk-cuda)
target_link_libraries(tango-bulk-cuda INTERFACE
    tango-bulk-transport CUDA::cudart)
