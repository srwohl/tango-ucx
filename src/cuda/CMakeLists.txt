# SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
# SPDX-License-Identifier: LGPL-3.0-or-later

# An object library: runtime.cpp registers itself from a static initializer, which a static
# archive would drop because nothing refers to it.
add_library(tango-bulk-cuda OBJECT runtime.cpp)
add_library(tango-bulk::cuda ALIAS tango-bulk-cuda)
set_target_properties(tango-bulk-cuda PROPERTIES EXPORT_NAME cuda)
target_include_directories(tango-bulk-cuda PRIVATE "${PROJECT_SOURCE_DIR}/src/transport")
target_compile_features(tango-bulk-cuda PUBLIC cxx_std_20)
target_link_libraries(tango-bulk-cuda PUBLIC tango-bulk-transport CUDA::cudart)
