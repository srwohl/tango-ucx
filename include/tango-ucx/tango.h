// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-ucx project
// SPDX-License-Identifier: LGPL-3.0-or-later
#pragma once

#include <tango-ucx/ucx.h>

#include <string_view>
#include <vector>

namespace Tango
{
class Attr;
class Command;
class DeviceImpl;
class DeviceProxy;
} // namespace Tango

namespace TangoUcx
{
// <prefix>Open and <prefix>Close; subscribers find them by the markers in their type descriptions.
void install_ucx_commands(std::vector<Tango::Command *> &, std::string_view prefix = "Ucx");
void install_ucx_attributes(std::vector<Tango::Attr *> &);
void attach_publisher(Tango::DeviceImpl &, Publisher &);
// Returns once no UCX command or attribute read is inside the publisher.
void detach_publisher(Tango::DeviceImpl &);
// Finds the UCX commands, reads UcxStream, opens a session and waits for the publisher's Ready.
Subscription every(Tango::DeviceProxy &, SubscriptionOptions = {});
Subscription latest(Tango::DeviceProxy &, SubscriptionOptions = {});
// A puller of the device's set: it takes `range` frames at each of its turns. A range is whole
// batches, no more than the receive ring holds, and the same for every puller of the set.
Subscription pull(Tango::DeviceProxy &, std::uint32_t range, SubscriptionOptions = {});
} // namespace TangoUcx
