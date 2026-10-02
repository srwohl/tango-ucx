# SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-ucx project
# SPDX-License-Identifier: LGPL-3.0-or-later

# An object library: runtime.cpp registers itself from a static initializer, which a static
# archive would drop because nothing refers to it.
add_library(tango-ucx-cuda OBJECT runtime.cpp)
add_library(tango-ucx::cuda ALIAS tango-ucx-cuda)
set_target_properties(tango-ucx-cuda PROPERTIES EXPORT_NAME cuda)
target_include_directories(tango-ucx-cuda PRIVATE "${PROJECT_SOURCE_DIR}/src/transport")
target_compile_features(tango-ucx-cuda PUBLIC cxx_std_20)
target_link_libraries(tango-ucx-cuda PUBLIC tango-ucx-transport CUDA::cudart)
