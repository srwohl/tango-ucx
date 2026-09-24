// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
// SPDX-License-Identifier: LGPL-3.0-or-later
#include "tango/device_server.h"
#include <catch2/catch_test_macros.hpp>
#include <tango-bulk/tango.h>
#include <transport.h>
#include <wire.h>

#include <tango/tango.h>

#include <atomic>
#include <thread>

using namespace TangoBulk;
using namespace std::chrono_literals;

namespace
{
Deadline in(std::chrono::seconds s = 10s)
{
    return std::chrono::steady_clock::now() + s;
}
Tango::DeviceProxy device()
{
    return Tango::DeviceProxy(test::DeviceServer::instance().device());
}
Tango::DevLong publish(Tango::DeviceProxy &p, Tango::DevLong n)
{
    Tango::DeviceData d;
    d << n;
    Tango::DevLong done = 0;
    p.command_inout("Publish", d) >> done;
    return done;
}
template <class T> T attribute(Tango::DeviceProxy &p, const char *name)
{
    T v{};
    auto a = p.read_attribute(name);
    a.reset_exceptions(Tango::DeviceAttribute::isempty_flag);
    a >> v;
    return v;
}
// Cancels the grants issued before the last Open; see Device::drain.
void drain(Tango::DeviceProxy &p)
{
    Tango::DevLong n = 0;
    p.command_inout("Drain") >> n;
    REQUIRE(n > 0);
}
// BulkSessions is refreshed every 256 loop turns: wait for rows that contain `text`.
std::vector<std::string> sessions_with(Tango::DeviceProxy &p, const std::string &text)
{
    auto rows = attribute<std::vector<std::string>>(p, "BulkSessions");
    for(auto until = in(); std::chrono::steady_clock::now() < until;
        std::this_thread::sleep_for(10ms))
    {
        rows = attribute<std::vector<std::string>>(p, "BulkSessions");
        if(!rows.empty() && rows[0].find(text) != std::string::npos)
            break;
    }
    return rows;
}
std::string status(std::string_view reply)
{
    wire::Reply r;
    return wire::parse(reply, r) ? std::string(wire::status_name(r.status)) : "unparsed";
}
} // namespace

TEST_CASE("a subscriber finds renamed bulk commands by their markers")
{
    auto proxy = device();
    std::unique_ptr<Tango::CommandInfoList> commands(proxy.command_list_query());
    std::map<std::string, Tango::CommandInfo> by_name;
    for(const auto &c : *commands)
        by_name[c.cmd_name] = c;
    CHECK_FALSE(by_name.contains("BulkOpen"));
    REQUIRE(by_name.contains("StreamOpen"));
    REQUIRE(by_name.contains("StreamClose"));
    for(auto [name, marker] : {std::pair{"StreamOpen", wire::k_open_marker},
                               std::pair{"StreamClose", wire::k_close_marker}})
    {
        CHECK(by_name[name].in_type == Tango::DEV_STRING);
        CHECK(by_name[name].out_type == Tango::DEV_STRING);
        CHECK(by_name[name].in_type_desc == marker);
        CHECK(transport::marker_major(by_name[name].in_type_desc) == wire::k_major);
    }
    auto sub = every(proxy);
    CHECK(sub.description().shape == std::vector<std::uint64_t>{4096});
    sub.close(in());
}

TEST_CASE("every and latest subscriptions receive a device server's frames")
{
    auto proxy = device();
    auto all = every(proxy, {.batch = 4, .max_wait = 20ms});
    auto newest = latest(proxy);
    drain(proxy);
    REQUIRE(publish(proxy, 12) == 12);
    std::uint64_t next = 0;
    while(next < 12)
    {
        auto batch = all.read(in());
        REQUIRE(batch);
        for(std::size_t i = 0; i < batch->frames(); ++i, ++next)
        {
            CHECK(batch->index(i) == next);
            auto *p = static_cast<const std::uint8_t *>(batch->payload()) + i * 4096;
            CHECK((p[0] == next && p[4095] == next));
        }
    }
    auto shown = newest.read(in());
    REQUIRE(shown);
    CHECK(shown->index(0) < 12);
    proxy.command_inout("Finish");
    CHECK_FALSE(all.read(in()));
    CHECK(all.outcome() == Outcome::End);
    all.close(in());
    shown.reset();
    newest.close(in());
}

TEST_CASE("BulkClose answers once the session row is free")
{
    auto proxy = device(); // two session rows
    auto first = every(proxy);
    auto second = latest(proxy);
    for(int i = 0; i < 5; ++i)
    {
        first.close(in());
        first = every(proxy); // refused with no-session if the row were still retiring
    }
    drain(proxy);
    CHECK(publish(proxy, 1) == 1);
    CHECK(first.read(in()));
    first.close(in());
    second.close(in());
}

TEST_CASE("the bulk attributes describe the stream and its sessions")
{
    auto proxy = device();
    auto offer = transport::parse_bulk_stream(attribute<std::string>(proxy, "BulkStream"));
    CHECK(offer.description.element == Element::U8);
    CHECK(offer.description.shape == std::vector<std::uint64_t>{4096});
    CHECK(offer.description.field_bytes == 8);
    CHECK(offer.description.field_dtype == "<u8");
    CHECK(offer.limits.allow_every);
    CHECK(offer.limits.max_sessions == 2);
    for(auto name : {"BulkStream", "BulkSessions", "BulkPublished", "BulkSkipped", "BulkPressure",
                     "BulkFailedEvery"})
        CHECK_FALSE(proxy.attribute_query(name).description.empty());

    auto sub = every(proxy, {.label = "archiver"});
    drain(proxy);
    REQUIRE(publish(proxy, 3) == 3);
    auto rows = sessions_with(proxy, "sent=3");
    REQUIRE(rows.size() == 1);
    INFO(rows[0]);
    CHECK(rows[0].starts_with("1|archiver|every|ready|depth="));
    CHECK(rows[0].find("|skipped=0|") != std::string::npos);
    CHECK(attribute<Tango::DevULong64>(proxy, "BulkPublished") == 3);
    CHECK(attribute<Tango::DevULong64>(proxy, "BulkSkipped") == 0);
    CHECK(attribute<Tango::DevULong>(proxy, "BulkPressure") == 0);
    CHECK(attribute<Tango::DevULong>(proxy, "BulkFailedEvery") == 0);
    sub.close(in());
}

TEST_CASE("detach_publisher waits for the bulk commands inside the publisher")
{
    auto proxy = device();
    auto before = transport::parse_bulk_stream(attribute<std::string>(proxy, "BulkStream"));
    std::atomic<bool> stop{false};
    std::atomic<int> replies{0}, unavailable{0}, other{0};
    auto hammer = [&]
    {
        auto own = device();
        auto doc = wire::format(wire::CloseMessage{12345});
        while(!stop)
            try
            {
                Tango::DeviceData d;
                d << doc;
                std::string reply;
                own.command_inout("StreamClose", d) >> reply;
                ++(status(reply) == "unknown-session" ? replies : other);
            }
            catch(const Tango::DevFailed &e)
            {
                ++(std::string(e.errors[0].reason) == "BulkUnavailable" ? unavailable : other);
            }
    };
    std::jthread a(hammer), b(hammer);
    for(int i = 0; i < 20; ++i)
        proxy.command_inout("Restart");
    stop = true;
    a.join();
    b.join();
    CHECK(replies > 0);
    CHECK(other == 0);
    INFO("replies " << replies << ", unavailable " << unavailable);
    auto after = transport::parse_bulk_stream(attribute<std::string>(proxy, "BulkStream"));
    CHECK(after.incarnation != before.incarnation);
    auto sub = every(proxy);
    drain(proxy);
    REQUIRE(publish(proxy, 1) == 1);
    CHECK(sub.read(in()));
    sub.close(in());
}
