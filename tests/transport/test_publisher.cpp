// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
// SPDX-License-Identifier: LGPL-3.0-or-later
#include "fixtures/pair.h"
#include <catch2/catch_test_macros.hpp>

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
    auto pending = transport::prepare(transport::offer(publisher), true, test::small_options());
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
    auto pending = transport::prepare(transport::offer(publisher), true, options);
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
