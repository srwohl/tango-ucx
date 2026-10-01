// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
// SPDX-License-Identifier: LGPL-3.0-or-later
#include "fixtures/fault_adapter.h"
#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <format>
#include <thread>

using namespace TangoBulk;

TEST_CASE("every and latest subscriptions observe the same published frame")
{
    Publisher publisher(test::small_description(), test::small_limits());
    auto every = test::subscribe(publisher, true);
    auto latest = test::subscribe(publisher, false);
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

TEST_CASE("no slot is granted before bulk_start, and no every Open is accepted after it")
{
    Publisher publisher(test::small_description(), test::small_limits(3));
    auto archiver = test::subscribe(publisher, true);
    CHECK_FALSE(publisher.acquire(test::deadline(std::chrono::seconds(1))));
    CHECK(publisher.health().slots_held == 0);
    CHECK_FALSE(publisher.health().started); // why the device got no slot
    publisher.bulk_start();
    CHECK(publisher.health().started);
    test::RawPeer late;
    CHECK(late.open(publisher, true, 4) == wire::Started);
    auto preview = test::subscribe(publisher, false); // latest joins a started publisher
    for(int i = 0; i < 3; ++i)
        test::publish_one(publisher);
    for(std::uint64_t i = 0; i < 3; ++i)
    {
        auto batch = archiver.read(test::deadline());
        REQUIRE(batch);
        CHECK(batch->index(0) == i); // from the first frame, with no grant ahead of its Open
    }
    CHECK(preview.read(test::deadline()));
    archiver.close(test::deadline());
    preview.close(test::deadline());
}

TEST_CASE("BulkSessions lists all 64 sessions with their 64-byte labels")
{
    test::RawPeer peer; // one worker behind every session
    Publisher publisher(test::small_description(), test::small_limits(64));
    auto label = [](std::uint64_t session) { return std::format("{:064}", session); };
    for(std::uint64_t session = 1; session <= 64; ++session)
    {
        wire::OpenMessage m{};
        m.session = session;
        m.incarnation = test::offer_of(publisher).incarnation;
        m.depth = 4;
        m.batch = 1;
        m.label_bytes = 64;
        std::ranges::copy(label(session), m.label.begin());
        m.address_bytes = std::uint16_t(peer.worker.address().size());
        std::ranges::copy(peer.worker.address(), m.address.begin());
        wire::Reply reply;
        REQUIRE(wire::parse(transport::handle(publisher, wire::format(m)), reply));
        REQUIRE(reply.status == wire::Ok);
    }
    auto sessions = test::sessions(publisher);
    REQUIRE(test::eventually([&] { return (sessions = test::sessions(publisher)).size() == 64; }));
    for(std::uint64_t i = 0; i < 64; ++i)
    {
        CHECK(sessions[i]["ordinal"] == i + 1);
        CHECK(sessions[i]["label"] == label(i + 1));
        CHECK(sessions[i]["delivery"] == "latest");
    }
}

TEST_CASE("an every Open that races bulk_start is refused, or gets every frame from the first")
{
    int joined = 0;
    for(int round = 0; round < 12; ++round)
    {
        Publisher publisher(test::small_description(), test::small_limits(1));
        auto pending = transport::prepare(test::offer_of(publisher), true, test::small_options());
        std::jthread device( // starts and publishes at once, while the Open is on its way
            [&]
            {
                std::this_thread::sleep_for(std::chrono::microseconds(40 * round));
                for(int i = 0; i < 3; ++i)
                    test::publish_one(publisher);
            });
        auto text = transport::handle(publisher, pending.open_message());
        device.join();
        wire::Reply reply;
        REQUIRE(wire::parse(text, reply));
        if(reply.status == wire::Started)
            continue;
        auto sub = std::move(pending).start(text, test::deadline(), test::closer_for(publisher));
        for(std::uint64_t i = 0; i < 3; ++i)
        {
            auto batch = sub.read(test::deadline());
            REQUIRE(batch);
            CHECK(batch->index(0) == i);
        }
        ++joined;
    }
    INFO("joined in " << joined << " of 12 rounds");
    SUCCEED();
}

TEST_CASE("an every row without room makes the device wait, and Health names it for as long")
{
    Publisher publisher(test::small_description(), test::small_limits(1)); // pressure_timeout 0
    auto sub = test::subscribe(publisher, true, {.budget = 128ull << 10, .label = "slow"});
    REQUIRE(test::eventually([&] { return test::first_row(publisher, "depth") != ~0ull; }));
    for(auto n = test::first_row(publisher, "depth"); n; --n)
        test::publish_one(publisher);
    CHECK_FALSE(publisher.acquire(test::deadline(std::chrono::seconds(2))));
    auto h = publisher.health();
    CHECK(test::eventually([&] { return (h = publisher.health()).pressure; }));
    CHECK(h.pressure_from == 1); // the row's BulkSessions ordinal
    CHECK(h.slots_free > 0);     // the device waits for room, not for memory
    CHECK(h.failed_every == 0);
    auto row = test::sessions(publisher).at(0);
    CHECK((row["ordinal"] == 1 && row["label"] == "slow" && row["delivery"] == "every" &&
           row["state"] == "ready"));
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

TEST_CASE("after finish, a full every row that is still draining is not evicted, and ends End")
{
    auto limits = test::small_limits(1);
    limits.pressure_timeout = std::chrono::milliseconds(300);
    Publisher publisher(test::small_description(), limits);
    auto sub = test::subscribe(publisher, true, {.budget = 128ull << 10});
    REQUIRE(test::eventually([&] { return test::first_row(publisher, "depth") != ~0ull; }));
    auto depth = test::first_row(publisher, "depth");
    for(auto n = depth; n; --n)
        test::publish_one(publisher); // the row is full
    publisher.finish();
    std::this_thread::sleep_for(std::chrono::seconds(1)); // past the pressure timeout
    for(std::uint64_t i = 0; i < depth; ++i)
    {
        auto batch = sub.read(test::deadline());
        INFO("frame " << i << ", failure: " << sub.health().failure);
        REQUIRE(batch);
        CHECK(batch->index(0) == i);
    }
    CHECK_FALSE(sub.read(test::deadline()));
    CHECK(sub.outcome() == Outcome::End);
    CHECK(publisher.health().failed_every == 0);
    sub.close(test::deadline());
}

TEST_CASE("frames are routed in grant order, and a cancelled slot uses no frame index")
{
    Publisher publisher(test::small_description(), test::small_limits(1));
    auto sub = test::subscribe(publisher, true);
    publisher.bulk_start();
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
