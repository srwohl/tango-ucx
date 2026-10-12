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
    IndexRing ring(IndexRing::Fd::Lazy);
    ring.reserve(3);
    CHECK_FALSE(readable(ring.watch()));
    for(std::uint32_t i = 0; i < 4; ++i)
    {
        ring.push(i);
        ring.push(i + 10);
        CHECK(readable(ring.fd.fd()));
        CHECK(ring.try_pop() == int(i));
        CHECK(readable(ring.fd.fd())); // removing one index leaves the ring's fd readable
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

TEST_CASE("a grant ring needs no fd to wake an acquire or signal that it has finished")
{
    IndexRing ring(IndexRing::Fd::None);
    ring.reserve(1);
    CHECK(ring.fd.fd() == -1);
    std::jthread producer(
        [&]
        {
            std::this_thread::sleep_for(20ms);
            ring.push(7);
        });
    CHECK(ring.pop_until(test::deadline(), [] { return false; }) == 7);
    std::atomic<bool> finished{false};
    std::jthread finisher(
        [&]
        {
            std::this_thread::sleep_for(20ms);
            finished = true;
            ring.signal();
        });
    CHECK(ring.pop_until(test::deadline(), [&] { return finished.load(); }) == -1);
}

// A subscription's delivered batches: read sleeps on the queue, and only fd() makes it polled.
TEST_CASE("a Lazy ring writes no fd until it is watched, and then is readable while it holds one")
{
    IndexRing ring(IndexRing::Fd::Lazy);
    ring.reserve(2);
    ring.push(1);
    CHECK_FALSE(readable(ring.fd.fd()));
    CHECK(ring.try_pop() == 1);
    ring.push(2);
    CHECK(readable(ring.watch())); // what was queued before the watch counts
    ring.push(3);
    CHECK(ring.try_pop() == 2);
    CHECK(readable(ring.fd.fd()));
    CHECK(ring.try_pop() == 3);
    CHECK_FALSE(readable(ring.fd.fd()));
    ring.signal();
    CHECK(readable(ring.fd.fd())); // an outcome, for a caller that polls
}

TEST_CASE("a Lazy ring that was signalled before it is watched is readable from the watch")
{
    IndexRing ring(IndexRing::Fd::Lazy);
    ring.reserve(1);
    ring.signal();
    CHECK(readable(ring.watch()));
}

// A loop's wait for what another thread does: a posted slot, a released batch.
TEST_CASE("a Wake writes its fd only for a loop that has parked")
{
    Wake wake;
    wake.notify();
    CHECK_FALSE(readable(wake.fd()));
    wake.park();
    CHECK_FALSE(readable(wake.fd()));
    wake.notify();
    CHECK(readable(wake.fd()));
    wake.unpark();
    CHECK_FALSE(readable(wake.fd()));
    wake.notify();
    CHECK_FALSE(readable(wake.fd()));
}

// An acquire or a read that comes after the end must not take the wake of one that waits, and
// nor must a clear: that one would sleep to its deadline. The window is short, so many rounds.
TEST_CASE(
    "a caller that comes after a signal, and a clear, leave the wake to the caller that waits")
{
    std::uint32_t delay = 1;
    for(int round = 0; round < 4000; ++round)
    {
        IndexRing ring;
        ring.reserve(2);
        std::atomic<bool> ended{false}, woken{false};
        auto done = [&] { return ended.load(); };
        auto start = std::chrono::steady_clock::now();
        std::jthread waiter(
            [&]
            {
                (void)ring.pop_until(start + 2s, done);
                woken = std::chrono::steady_clock::now() - start < 1s;
            });
        delay = delay * 1664525u + 1013904223u; // the waiter is at a different point each round
        for(auto until = start + std::chrono::microseconds(delay >> 24);
            std::chrono::steady_clock::now() < until;)
        {
        }
        ended = true;
        ring.signal();
        if(round % 2)
            ring.clear();
        else
            CHECK(ring.pop_until(start + 2s, done) == -1);
        waiter.join();
        REQUIRE(woken);
    }
}

TEST_CASE("a signal wakes every caller that waits for an index, and a pop that does not wait "
          "leaves the wake to them")
{
    IndexRing ring(IndexRing::Fd::None);
    ring.reserve(2);
    std::atomic<bool> ended{false};
    std::atomic<int> returned{0};
    {
        std::jthread first(
            [&]
            { returned += ring.pop_until(test::deadline(), [&] { return ended.load(); }) == -1; });
        std::jthread second(
            [&]
            { returned += ring.pop_until(test::deadline(), [&] { return ended.load(); }) == -1; });
        std::this_thread::sleep_for(50ms); // both wait
        ended = true;
        ring.signal();
        CHECK(ring.try_pop() == -1); // takes no index, and no wake either
    }
    CHECK(returned == 2);
    // A caller that comes after the end is told without a signal of its own.
    auto start = std::chrono::steady_clock::now();
    CHECK(ring.pop_until(test::deadline(), [&] { return ended.load(); }) == -1);
    CHECK(std::chrono::steady_clock::now() - start < 1s);
}

TEST_CASE("a deadline that has passed still gives an index that is queued, and never waits")
{
    IndexRing ring(IndexRing::Fd::None);
    ring.reserve(1);
    ring.push(5);
    auto past = std::chrono::steady_clock::now() - 1s;
    CHECK(ring.pop_until(past, [] { return false; }) == 5);
    auto start = std::chrono::steady_clock::now();
    CHECK(ring.pop_until(past, [] { return false; }) == -1);
    CHECK(std::chrono::steady_clock::now() - start < 1s);
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

// Two RDMA rails. Health is where a run shows that it used both.
TEST_CASE("a subscription keeps the lanes of two rails whole")
{
    constexpr std::string_view lanes = "rc_mlx5/mlx5_2:1,rc_mlx5/mlx5_3:1";
    decltype(Sub::transport) kept;
    kept.set(lanes);
    CHECK(kept.view() == lanes);
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
