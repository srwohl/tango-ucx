// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-ucx project
// SPDX-License-Identifier: LGPL-3.0-or-later
#include <tango-ucx/tango.h>
#include <transport.h>
#include <wire.h>

#include <tango/tango.h>

#include <algorithm>
#include <format>
#include <memory>
#include <stdexcept>

namespace TangoUcx
{
namespace
{
struct Commands
{
    std::string open, close;
};
// The UCX commands, whatever the device named them.
Commands discover(Tango::DeviceProxy &proxy)
{
    std::unique_ptr<Tango::CommandInfoList> list(proxy.command_list_query());
    Commands found;
    int other = -1;
    for(const auto &c : *list)
    {
        int major = transport::marker_major(c.in_type_desc);
        if(major < 0 || c.in_type != Tango::DEV_STRING || c.out_type != Tango::DEV_STRING)
            continue;
        if(major != wire::k_major)
        {
            other = major;
            continue;
        }
        std::string *name = c.in_type_desc == wire::k_open_marker    ? &found.open
                            : c.in_type_desc == wire::k_close_marker ? &found.close
                                                                     : nullptr;
        if(!name)
            continue;
        if(!name->empty())
            throw std::runtime_error(proxy.dev_name() + " has two sets of UCX commands");
        *name = c.cmd_name;
    }
    if(found.open.empty() || found.close.empty())
        throw std::runtime_error(
            other < 0 ? proxy.dev_name() + " has no UCX commands"
                      : proxy.dev_name() + " speaks tango-ucx/" + std::to_string(other) +
                            "; this library speaks tango-ucx/" + std::to_string(wire::k_major));
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
Subscription subscribe(Tango::DeviceProxy &proxy, bool every, SubscriptionOptions options,
                       std::uint32_t range = 0)
{
    auto names = discover(proxy);
    std::string stream;
    proxy.read_attribute("UcxStream") >> stream;
    auto pending =
        transport::prepare(transport::parse_ucx_stream(stream), every, std::move(options), range);
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
Subscription pull(Tango::DeviceProxy &proxy, std::uint32_t range, SubscriptionOptions options)
{
    if(!range)
        throw std::invalid_argument("a puller's range is at least one frame");
    return subscribe(proxy, false, std::move(options), range);
}
} // namespace TangoUcx
