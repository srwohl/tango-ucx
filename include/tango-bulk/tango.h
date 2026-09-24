// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
// SPDX-License-Identifier: LGPL-3.0-or-later
#pragma once

#include <tango-bulk/bulk.h>

#include <string_view>
#include <vector>

namespace Tango
{
class Attr;
class Command;
class DeviceImpl;
class DeviceProxy;
} // namespace Tango

namespace TangoBulk
{
// <prefix>Open and <prefix>Close; subscribers find them by the markers in their type descriptions.
void install_bulk_commands(std::vector<Tango::Command *> &, std::string_view prefix = "Bulk");
void install_bulk_attributes(std::vector<Tango::Attr *> &);
void attach_publisher(Tango::DeviceImpl &, Publisher &);
// Returns once no bulk command or attribute read is inside the publisher.
void detach_publisher(Tango::DeviceImpl &);
// Finds the bulk commands, reads BulkStream, opens a session and waits for the publisher's Ready.
Subscription every(Tango::DeviceProxy &, SubscriptionOptions = {});
Subscription latest(Tango::DeviceProxy &, SubscriptionOptions = {});
} // namespace TangoBulk
