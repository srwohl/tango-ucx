// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-ucx project
// SPDX-License-Identifier: LGPL-3.0-or-later
#include <tango-ucx/tango.h>

int main()
{
    auto *subscribe = &TangoUcx::every; // from the Tango archive, and so cppTango is linked
    TangoUcx::Batch batch;              // from the transport archive
    return subscribe && batch.frames() == 0 ? 0 : 1;
}
