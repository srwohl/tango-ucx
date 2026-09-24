// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
// SPDX-License-Identifier: LGPL-3.0-or-later
#include "fixtures/fault_adapter.h"
#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <thread>

using namespace TangoBulk;

TEST_CASE("every and latest subscriptions observe the same published frame")
{
    Publisher publisher(test::small_description(), test::small_limits());
    auto every = test::subscribe(publisher, true);
    auto latest = test::subscribe(publisher, false);
    test::cancel_earlier_grants(publisher);
    test::publish_one(publisher, std::byte{0x3c}, 1234);
    auto eb = every.read(test::deadline());
    auto lb = latest.read(test::deadline());
    auto eh = every.health();
    auto lh = latest.health();
    auto ph = publisher.health();
    INFO("every frames=" << eh.frames << " failed=" << eh.failure << " latest frames=" << lh.frames
                         << " failed=" << lh.failure << " publisher frames=" << ph.frames
                         << " slots held=" << ph.slots_held);
    REQUIRE(eb);
    REQUIRE(lb);
    REQUIRE(eb->frames() == 1);
    REQUIRE(lb->frames() == 1);
    CHECK(eb->index(0) == lb->index(0));
    CHECK(eb->index(0) == 0);
    CHECK(static_cast<const std::byte *>(eb->payload())[0] == std::byte{0x3c});
    CHECK(static_cast<const std::byte *>(lb->payload())[0] == std::byte{0x3c});
    every.close(test::deadline());
    latest.close(test::deadline());
    publisher.finish();
}

TEST_CASE("an Open whose session id a row already holds is refused")
{
    Publisher publisher(test::small_description(), test::small_limits());
    auto pending = transport::prepare(test::offer_of(publisher), true, test::small_options());
    auto open = pending.open_message();
    auto first = transport::handle(publisher, open);
    wire::Reply again;
    REQUIRE(wire::parse(transport::handle(publisher, open), again));
    CHECK(again.status == wire::Malformed);
    auto sub = std::move(pending).start(first, test::deadline(), test::closer_for(publisher));
    test::cancel_earlier_grants(publisher);
    test::publish_one(publisher);
    auto batch = sub.read(test::deadline());
    REQUIRE(batch);
    CHECK(batch->frames() == 1);
    // The publisher thread refreshes Health only every 256 turns: wait for a copy with the row.
    auto health = publisher.health();
    for(auto until = test::deadline();
        health.every == 0 && std::chrono::steady_clock::now() < until; health = publisher.health())
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    CHECK(health.every == 1);
    batch.reset();
    sub.close(test::deadline());
    publisher.finish();
}

TEST_CASE("a malformed or refused bulk command gets a status, not an exception")
{
    Publisher publisher(test::small_description(), test::small_limits());
    wire::Reply reply;
    REQUIRE(wire::parse(transport::handle(publisher, "not json"), reply));
    CHECK(reply.status == wire::Malformed);
    REQUIRE(wire::parse(
        transport::handle(publisher, R"({"interface":"tango-bulk-close/4.0","session":1})"),
        reply));
    CHECK(reply.status == wire::Version);
    REQUIRE(wire::parse(transport::handle(publisher, wire::format(wire::CloseMessage{99})), reply));
    CHECK(reply.status == wire::UnknownSession);
    publisher.finish();
}

TEST_CASE("a mid-stream every row gets consecutive frames from the grants issued after its Open")
{
    Publisher publisher(test::small_description(), test::small_limits(1));
    // With no row yet, the loop grants every free slot.
    auto granted = test::granted(publisher);
    auto options = test::small_options();
    options.budget = 256ull << 10;
    auto pending = transport::prepare(test::offer_of(publisher), true, options);
    auto open = pending.open_message();
    wire::Request request;
    REQUIRE(wire::parse(open, request) == wire::Ok);
    auto depth = request.open->depth;
    REQUIRE(depth < granted);
    auto sub = std::move(pending).start(transport::handle(publisher, open), test::deadline(),
                                        test::closer_for(publisher));
    // The earlier grants reserved no room in the row; only the next `depth` grants did.
    for(std::uint32_t i = 0; i < granted + depth; ++i)
        test::publish_one(publisher);
    for(std::uint32_t i = 0; i < depth; ++i)
    {
        auto batch = sub.read(test::deadline());
        INFO("frame " << i << " of " << depth << ", failure: " << sub.health().failure);
        REQUIRE(batch);
        CHECK(batch->index(0) == granted + i);
    }
    CHECK(sub.health().failure.empty());
    sub.close(test::deadline());
    publisher.finish();
}

TEST_CASE("a new every subscription waits behind the grants issued before its Open")
{
    Description d{Element::U8, {1 << 20}, 0, {}};
    auto limits = test::small_limits(1);
    limits.budget = 12ull << 20;
    Publisher publisher(d, limits);
    auto S = test::granted(publisher); // with no session, every source slot is granted ahead
    std::atomic<std::uint64_t> published{0};
    std::jthread device( // a 10 Hz device
        [&](std::stop_token stop)
        {
            for(auto tick = std::chrono::steady_clock::now(); !stop.stop_requested();
                std::this_thread::sleep_until(tick += std::chrono::milliseconds(100)))
                if(auto slot = publisher.acquire(tick + std::chrono::seconds(1)))
                    std::move(*slot).publish(), ++published;
        });
    std::this_thread::sleep_for(std::chrono::milliseconds(250));
    auto before = published.load();
    auto opened = std::chrono::steady_clock::now();
    auto sub = test::subscribe(publisher, true, {.budget = 8ull << 20});
    auto first = sub.read(test::deadline(std::chrono::seconds(30)));
    auto waited = std::chrono::steady_clock::now() - opened;
    REQUIRE(first);
    auto behind = first->index(0) - before; // frames published after the Open that it never got
    INFO("S=" << S << " behind=" << behind
              << " waited_ms=" << waited / std::chrono::milliseconds(1));
    CHECK(behind + 2 >= S);
    CHECK(waited >= (behind - 1) * std::chrono::milliseconds(100));
    first.reset();
    sub.close(test::deadline());
}

TEST_CASE("an every row without room makes the device wait, and Health names it for as long")
{
    Publisher publisher(test::small_description(), test::small_limits(1)); // pressure_timeout 0
    auto sub = test::subscribe(publisher, true, {.budget = 128ull << 10, .label = "slow"});
    test::cancel_earlier_grants(publisher);
    REQUIRE(test::eventually([&] { return test::first_row(publisher, "depth") != ~0ull; }));
    for(auto n = test::first_row(publisher, "depth"); n; --n)
        test::publish_one(publisher);
    CHECK_FALSE(publisher.acquire(test::deadline(std::chrono::seconds(2))));
    auto h = publisher.health();
    CHECK(test::eventually([&] { return (h = publisher.health()).pressure; }));
    CHECK(h.pressure_from == 1); // the row's BulkSessions ordinal
    CHECK(h.slots_free > 0);     // the device waits for room, not for memory
    CHECK(h.failed_every == 0);
    CHECK(transport::session_rows(publisher)[0].starts_with("1|slow|every|ready|"));
    CHECK(test::first_row(publisher, "room") == 0);
    CHECK(sub.read(test::deadline())); // one batch released: room for one more frame
    CHECK(publisher.acquire(test::deadline()));
    sub.close(test::deadline());
}

TEST_CASE("eviction ends a stalled every session with End(evicted), and latest is never evicted")
{
    auto limits = test::small_limits(2);
    limits.pressure_timeout = std::chrono::milliseconds(300);
    Publisher publisher(test::small_description(), limits);
    auto stalled = test::subscribe(publisher, true, {.budget = 128ull << 10});
    auto idle = test::subscribe(publisher, false, {.budget = 128ull << 10});
    test::cancel_earlier_grants(publisher);
    std::uint64_t depth = 0;
    REQUIRE(
        test::eventually([&] { return (depth = test::first_row(publisher, "depth")) != ~0ull; }));
    for(auto n = depth; n; --n)
        test::publish_one(publisher);
    auto waiting = std::chrono::steady_clock::now();
    CHECK(publisher.acquire(test::deadline())); // granted again once the row is evicted
    CHECK(std::chrono::steady_clock::now() - waiting >= limits.pressure_timeout);
    CHECK(test::eventually([&] { return publisher.health().failed_every == 1; }));
    for(auto n = depth; n; --n)
        CHECK(stalled.read(test::deadline())); // what landed before End is still delivered
    CHECK_FALSE(stalled.read(test::deadline()));
    CHECK(stalled.outcome() == Outcome::Failed);
    CHECK(stalled.health().failure.starts_with("evicted"));
    CHECK(publisher.health().latest == 1);
    stalled.close(test::deadline());
    idle.close(test::deadline());
}

TEST_CASE("a latest row is paced by max_rate, and every frame it misses is a skip")
{
    Publisher publisher(test::small_description(), test::small_limits(1));
    auto sub = test::subscribe(publisher, false, {.budget = 1ull << 20, .max_rate = 10});
    std::uint64_t n = 0;
    auto start = std::chrono::steady_clock::now();
    for(; std::chrono::steady_clock::now() - start < std::chrono::seconds(1); ++n)
        test::publish_one(publisher);
    auto elapsed = std::chrono::steady_clock::now() - start;
    REQUIRE(test::eventually(
        [&]
        {
            return test::first_row(publisher, "sent") + test::first_row(publisher, "skipped") == n;
        }));
    auto sent = test::first_row(publisher, "sent");
    INFO("published " << n << ", sent " << sent);
    CHECK(sent >= 2);
    CHECK(sent <= elapsed / std::chrono::milliseconds(100) + 1);
    CHECK(publisher.health().skipped == n - sent);
    sub.close(test::deadline());
}

TEST_CASE("frames are routed in grant order, and a cancelled slot uses no frame index")
{
    Publisher publisher(test::small_description(), test::small_limits(1));
    auto sub = test::subscribe(publisher, true);
    test::cancel_earlier_grants(publisher);
    auto fill = [](Slot &s, int v) { std::memset(s.payload().data(), v, s.payload().size()); };
    auto a = publisher.acquire(test::deadline());
    auto b = publisher.acquire(test::deadline());
    auto c = publisher.acquire(test::deadline());
    REQUIRE((a && b && c));
    fill(*a, 0xa);
    fill(*b, 0xb);
    std::move(*b).publish();
    c.reset(); // cancelled
    CHECK_FALSE(sub.read(std::chrono::steady_clock::now() + std::chrono::milliseconds(200)));
    std::move(*a).publish();
    test::publish_one(publisher, std::byte{0xd});
    for(int i : {0xa, 0xb, 0xd})
    {
        auto batch = sub.read(test::deadline());
        REQUIRE(batch);
        CHECK(static_cast<const std::uint8_t *>(batch->payload())[0] == i);
        CHECK(batch->index(0) == std::uint64_t(i == 0xa ? 0 : i == 0xb ? 1 : 2));
    }
    sub.close(test::deadline());
}

TEST_CASE("refused Opens get every-refused, incarnation, no-session and finished")
{
    auto limits = test::small_limits(1);
    limits.allow_every = false;
    Publisher publisher(test::small_description(), limits);
    auto pending = transport::prepare(test::offer_of(publisher), false, test::small_options());
    wire::Request r;
    REQUIRE(wire::parse(pending.open_message(), r) == wire::Ok);
    auto status = [&](auto change)
    {
        auto m = *r.open;
        change(m);
        wire::Reply reply;
        wire::parse(transport::handle(publisher, wire::format(m)), reply);
        return reply.status;
    };
    CHECK(status([](auto &m) { m.every = true; }) == wire::EveryRefused);
    CHECK(status([](auto &m) { ++m.incarnation; }) == wire::Incarnation);
    CHECK(status([](auto &) {}) == wire::Ok);
    CHECK(status([](auto &m) { ++m.session; }) == wire::NoSession);
    publisher.finish();
    CHECK(status([](auto &m) { m.session += 2; }) == wire::Finished);
}

TEST_CASE("Ready reaches the subscriber at Open, and BulkClose is answered once the row is free")
{
    Publisher publisher(test::small_description(), test::small_limits(1));
    {
        test::RawPeer peer;
        REQUIRE(peer.open(publisher, true, 4) == wire::Ok);
        REQUIRE(peer.progress_until([&] { return peer.count(wire::Ready) == 1; }));
        wire::SessionMessage ready;
        CHECK((wire::decode(peer.got[0].second, ready) && ready.session == peer.session));
        wire::Reply reply;
        wire::parse(transport::handle(publisher, wire::format(wire::CloseMessage{peer.session})),
                    reply);
        CHECK(reply.status == wire::Ok);
    }
    for(int i = 0; i < 5; ++i) // one row: each Open would be refused if the Close came early
        test::subscribe(publisher, true).close(test::deadline());
}

TEST_CASE("End after finish follows the completion of every frame send")
{
    Publisher publisher({Element::U8, {1 << 20}, 0, {}}, test::small_limits(1));
    auto sub = test::subscribe(publisher, true, {.budget = 16ull << 20});
    test::cancel_earlier_grants(publisher);
    for(int i = 0; i < 3; ++i)
        test::publish_one(publisher);
    publisher.finish();
    for(std::uint64_t i = 0; i < 3; ++i)
    {
        auto batch = sub.read(test::deadline());
        REQUIRE(batch);
        CHECK(batch->index(0) == i);
    }
    CHECK_FALSE(sub.read(test::deadline()));
    CHECK(sub.outcome() == Outcome::End);
    sub.close(test::deadline());
}
