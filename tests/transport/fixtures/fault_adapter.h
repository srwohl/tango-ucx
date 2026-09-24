// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
// SPDX-License-Identifier: LGPL-3.0-or-later
#pragma once

#include <transport.h>

namespace TangoBulk::test
{
struct FaultAdapter
{
    static transport::Closer lose_close_fence()
    {
        return [](std::string_view, Deadline) { return false; };
    }
};
} // namespace TangoBulk::test
