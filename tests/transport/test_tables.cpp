// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-ucx project
// SPDX-License-Identifier: LGPL-3.0-or-later
#include "fixtures/fault_adapter.h"
#include <catch2/catch_test_macros.hpp>
#include <tables.h>

#include <poll.h>

using namespace TangoUcx;
using namespace TangoUcx::transport;
using namespace std::chrono_literals;

namespace
{
bool readable(int fd)
{
    pollfd p{fd, POLLIN, 0};
    return ::poll(&p, 1, 0) == 1;
}
wire::Request close_request(std::uint64_t session)
{
    wire::Request r;
    r.close.session = session;
    return r;
}
} // namespace

TEST_CASE("IndexRing keeps FIFO order across its wrap, and its fd is readable while it holds one")
{
    IndexRing ring;
    ring.reserve(3);
    CHECK_FALSE(readable(ring.fd.fd()));
    for(std::uint32_t i = 0; i < 4; ++i)
    {
        ring.push(i);
        ring.push(i + 10);
        CHECK(readable(ring.fd.fd()));
        CHECK(ring.try_pop() == int(i));
        CHECK(ring.try_pop() == int(i + 10));
        CHECK_FALSE(readable(ring.fd.fd()));
        CHECK(ring.try_pop() == -1);
    }
    auto start = std::chrono::steady_clock::now();
    CHECK(ring.pop_until(start + 20ms, [] { return false; }) == -1);
    CHECK(std::chrono::steady_clock::now() - start >= 20ms);
    std::atomic<bool> ended{false};
    std::jthread t(
        [&]
        {
            std::this_thread::sleep_for(20ms);
            ended = true;
            ring.signal();
        });
    CHECK(ring.pop_until(test::deadline(), [&] { return ended.load(); }) == -1);
}

TEST_CASE("IndexRing waits without a deadline, leaving the lock to the thread that pushes")
{
    IndexRing ring;
    ring.reserve(1);
    std::atomic<int> checks{0};
    std::jthread t(
        [&]
        {
            std::this_thread::sleep_for(100ms);
            CHECK(checks.load() <= 2);
            ring.push(7);
        });
    CHECK(ring.pop_until(Deadline::max(), [&] { return ++checks, false; }) == 7);
}

TEST_CASE("ControlTable returns the loop's reply to the caller that posted the request")
{
    ControlTable table;
    std::jthread loop(
        [&](std::stop_token stop)
        {
            while(!stop.stop_requested())
                table.drain([&](ControlEntry &e)
                            { table.reply(e, {wire::Status(e.in.close.session)}); });
        });
    for(auto s : {wire::UnknownSession, wire::NoSession, wire::Ok})
        CHECK(table.call(close_request(s), test::deadline()).status == s);
}

TEST_CASE("ControlTable answers busy when all eight entries wait for the loop")
{
    ControlTable table;
    std::vector<std::jthread> callers;
    for(int i = 0; i < 8; ++i)
        callers.emplace_back(
            [&]
            { CHECK(table.call(close_request(1), test::deadline(1s)).status == wire::Timeout); });
    REQUIRE(test::eventually(
        [&]
        {
            return std::ranges::all_of(table.entries, [](const ControlEntry &e)
                                       { return e.state == ControlEntry::Posted; });
        }));
    CHECK(table.call(close_request(1), test::deadline()).status == wire::Busy);
}

TEST_CASE("ControlTable frees an entry whose caller gave up once the loop replies")
{
    ControlTable table;
    std::atomic<ControlEntry *> taken{nullptr};
    std::jthread loop(
        [&]
        {
            while(!taken)
                table.drain([&](ControlEntry &e) { taken = &e; });
        });
    CHECK(table.call(close_request(1), std::chrono::steady_clock::now() + 50ms).status ==
          wire::Timeout);
    loop.join();
    table.reply(*taken, {wire::Ok}); // late: nobody waits for it any more
    CHECK(taken.load()->state == ControlEntry::Free);
}

TEST_CASE("ControlTable frees an entry whose caller gave up before the loop took it")
{
    ControlTable table;
    for(int i = 0; i < 9; ++i) // one more than the table holds
        CHECK(table.call(close_request(1), std::chrono::steady_clock::now() + 1ms).status ==
              wire::Timeout);
    std::jthread loop(
        [&](std::stop_token stop)
        {
            while(!stop.stop_requested())
                table.drain([&](ControlEntry &e) { table.reply(e, {wire::Ok}); });
        });
    CHECK(table.call(close_request(1), test::deadline()).status == wire::Ok);
}

TEST_CASE("HealthCopy never makes the loop wait for a thread reading it")
{
    HealthCopy<int> copy;
    copy.write(1);
    {
        std::lock_guard reading(copy.m);
        std::jthread([&] { copy.write(2); }).join(); // returns at once; the sample is dropped
    }
    CHECK(copy.read() == 1);
    copy.write(3);
    CHECK(copy.read() == 3);
}

TEST_CASE("Quarantine keeps every owner it is given and counts their bytes")
{
    Quarantine q;
    Owner a, b;
    q.keep(a, 100);
    q.keep(b, 20);
    CHECK(q.total == 120);
    CHECK(q.head == &b);
    CHECK(b.kept_next == &a);
    CHECK(a.kept_next == nullptr);
}

TEST_CASE("a Credit above the frames sent ends the session with End(protocol)")
{
    Publisher publisher(test::small_description(), test::small_limits(1));
    test::RawPeer peer;
    REQUIRE(peer.open(publisher, true, 4) == wire::Ok);
    REQUIRE(peer.progress_until([&] { return peer.count(wire::Ready) == 1; }));
    peer.send(wire::Credit, wire::encode(wire::CreditMessage{peer.session, 1}));
    REQUIRE(peer.progress_until([&] { return peer.count(wire::End) == 1; }));
    wire::EndMessage end;
    REQUIRE(wire::decode(peer.got.back().second, end));
    CHECK(end.session == peer.session);
    CHECK(end.reason == wire::Protocol);
    CHECK(test::eventually([&] { return publisher.health().failed_every == 1; }));
}

TEST_CASE("a Credit below one already received is ignored")
{
    Publisher publisher(test::small_description(), test::small_limits(1));
    test::RawPeer peer;
    REQUIRE(peer.open(publisher, true, 4) == wire::Ok);
    test::publish_one(publisher);
    test::publish_one(publisher);
    REQUIRE(peer.progress_until([&] { return peer.count(wire::Frame) == 2; }));
    peer.send(wire::Credit, wire::encode(wire::CreditMessage{peer.session, 2}));
    peer.send(wire::Credit, wire::encode(wire::CreditMessage{peer.session, 1}));
    // Room stays at the depth: four more frames fit, where a credit of 1 would leave room for 3.
    for(int i = 0; i < 4; ++i)
        test::publish_one(publisher);
    CHECK(peer.progress_until([&] { return peer.count(wire::Frame) == 6; }));
    CHECK(peer.count(wire::End) == 0);
    CHECK(publisher.health().failed_every == 0);
}

TEST_CASE("Credit follows only the released prefix, whatever order batches are released in")
{
    Publisher publisher(test::small_description(), test::small_limits(1));
    auto sub = test::subscribe(publisher, true);
    std::vector<std::optional<Batch>> held;
    for(int i = 0; i < 3; ++i)
    {
        test::publish_one(publisher);
        held.push_back(sub.read(test::deadline()));
        REQUIRE(held.back());
    }
    auto room = [&] { return test::first_row(publisher, "room"); };
    REQUIRE(test::eventually([&] { return room() + 3 == test::first_row(publisher, "depth"); }));
    auto depth = test::first_row(publisher, "depth");
    held[2].reset();
    held[1].reset();
    std::this_thread::sleep_for(200ms); // nothing to credit while the oldest batch is held
    CHECK(room() == depth - 3);
    held[0].reset();
    CHECK(test::eventually([&] { return room() == depth; }));
    sub.close(test::deadline());
}

TEST_CASE("sequences wrap the receive ring and frames keep their order")
{
    Publisher publisher(test::small_description(), test::small_limits(1));
    auto options = test::small_options();
    options.budget = 128ull << 10;
    auto sub = test::subscribe(publisher, true, options);
    REQUIRE(test::eventually([&] { return test::first_row(publisher, "depth") != ~0ull; }));
    auto depth = test::first_row(publisher, "depth");
    REQUIRE(depth < 100);
    for(std::uint64_t i = 0; i < 3 * depth; ++i)
    {
        test::publish_one(publisher);
        auto batch = sub.read(test::deadline());
        REQUIRE(batch);
        CHECK(batch->index(0) == i);
    }
    sub.close(test::deadline());
}
