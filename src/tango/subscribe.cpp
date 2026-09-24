// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
// SPDX-License-Identifier: LGPL-3.0-or-later
#include <tango-bulk/tango.h>
#include <transport.h>
#include <wire.h>

#include <tango/tango.h>

#include <algorithm>
#include <format>
#include <memory>
#include <stdexcept>

namespace TangoBulk
{
namespace
{
struct Commands
{
    std::string open, close;
};
// The bulk commands, whatever the device named them.
Commands find_commands(Tango::DeviceProxy &proxy)
{
    std::unique_ptr<Tango::CommandInfoList> list(proxy.command_list_query());
    Commands found;
    for(const auto &c : *list)
    {
        int major = transport::marker_major(c.in_type_desc);
        if(major < 0 || c.in_type != Tango::DEV_STRING)
            continue;
        if(major != wire::k_major)
            throw std::runtime_error(
                std::format("{} speaks tango-bulk/{}; this library tango-bulk/{}", c.cmd_name,
                            major, wire::k_major));
        auto &name = c.in_type_desc.ends_with(" open") ? found.open : found.close;
        if(!name.empty())
            throw std::runtime_error("the device has more than one set of bulk commands");
        name = c.cmd_name;
    }
    if(found.open.empty() || found.close.empty())
        throw std::runtime_error("the device has no bulk commands");
    return found;
}
std::string command(Tango::DeviceProxy &proxy, const std::string &name, std::string_view document)
{
    Tango::DeviceData in;
    in << std::string(document);
    std::string reply;
    proxy.command_inout(name, in) >> reply;
    return reply;
}
Subscription subscribe(Tango::DeviceProxy &proxy, bool every, SubscriptionOptions options)
{
    auto names = find_commands(proxy);
    std::string stream;
    proxy.read_attribute("BulkStream") >> stream;
    auto pending =
        transport::prepare(transport::parse_bulk_stream(stream), every, std::move(options));
    auto reply = command(proxy, names.open, pending.open_message());
    // Close uses its own copy of the proxy: it may run after the caller's proxy is gone, and it
    // sets the timeout from the close deadline without touching the caller's.
    auto link = std::make_shared<Tango::DeviceProxy>(proxy);
    transport::Closer closer = [link, name = names.close](std::string_view document, Deadline d)
    {
        auto left = std::chrono::duration_cast<std::chrono::milliseconds>(
            d - std::chrono::steady_clock::now());
        link->set_timeout_millis(int(std::clamp<std::int64_t>(left.count(), 1, 60'000)));
        wire::Reply r;
        return wire::parse(command(*link, name, document), r) && r.status == wire::Ok;
    };
    return std::move(pending).start(
        reply, std::chrono::steady_clock::now() + std::chrono::seconds(5), std::move(closer));
}
} // namespace

Subscription every(Tango::DeviceProxy &proxy, SubscriptionOptions options)
{
    return subscribe(proxy, true, std::move(options));
}
Subscription latest(Tango::DeviceProxy &proxy, SubscriptionOptions options)
{
    return subscribe(proxy, false, std::move(options));
}
} // namespace TangoBulk
