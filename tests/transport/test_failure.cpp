// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-ucx project
// SPDX-License-Identifier: LGPL-3.0-or-later
#include "fixtures/fault_adapter.h"
#include "fixtures/fork.h"
#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>
#include <tables.h>

#include <csignal>
#include <cstdlib>
#include <sys/wait.h>
#include <thread>
#include <vector>

using namespace TangoUcx;

TEST_CASE("forced close preserves ownership around a large in-flight receive")
{
    Description d{Element::U8, {1ull << 20}, 0, {}};
    PublisherLimits limits;
    limits.budget = 5ull << 20;
    limits.max_sessions = 1;
    limits.allow_every = true;
    Publisher publisher(d, limits);
    auto publisher_pinned = transport::pinned_bytes();
    auto quarantined_before = transport::quarantine().total.load();
    SubscriptionOptions options;
    options.budget = 3ull << 20;
    options.batch = 1;
    options.max_wait = std::chrono::microseconds(0);
    auto sub = test::subscribe(publisher, true, options, test::FaultAdapter::lose_close_fence());
    publisher.ucx_start();
    for(std::uint64_t i = 0; i < 2; ++i)
    {
        auto slot = publisher.acquire(test::deadline());
        REQUIRE(slot);
        std::memset(slot->payload().data(), 0x49, slot->payload().size());
        std::move(*slot).publish(i + 5);
    }
    auto until = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while(std::chrono::steady_clock::now() < until && sub.health().slots_held < 2)
        std::this_thread::yield();
    CHECK(sub.health().slots_held == 2);
    sub.close(std::chrono::steady_clock::now());
    auto quarantined_after = transport::quarantine().total.load();
    CHECK((quarantined_after > quarantined_before || sub.outcome() == Outcome::Cancelled));
    CHECK(
        (transport::pinned_bytes() == publisher_pinned || quarantined_after > quarantined_before));
    publisher.finish();
}

namespace
{
// Runs a case once for each way UCX finds a dead peer: over shared memory it checks the peer's
// process, over tcp it sees the socket close.
std::string peer_transport()
{
    auto tls = GENERATE(as<std::string>{}, "all", "tcp");
    ::setenv("TANGO_UCX_UCX_TLS", tls.c_str(), 1);
    return tls;
}
} // namespace

TEST_CASE("a subscriber killed with SIGKILL is detected through the UCX peer endpoint")
{
    auto tls = peer_transport();
    test::Forked child;
    if(child.pid == 0)
    {
        test::ForkOffer fo{};
        if(!test::read_all(child.in(), &fo, sizeof(fo)))
            ::_exit(2);
        transport::Offer o{test::small_description(), test::small_limits(fo.sessions),
                           fo.incarnation};
        o.limits.allow_every = fo.every != 0;
        try
        {
            auto pending = transport::prepare(o, true, test::small_options());
            if(!test::write_text(child.out(), pending.open_message()))
                ::_exit(3);
            auto reply = test::read_text(child.in());
            if(reply.empty())
                ::_exit(4);
            auto sub =
                std::move(pending).start(reply, test::deadline(std::chrono::seconds(10)), {});
            if(!test::write_text(child.out(), sub.health().transport))
                ::_exit(5);
            for(;;)
                ::pause();
        }
        catch(...)
        {
            ::_exit(6);
        }
    }
    Publisher publisher(test::small_description(), test::small_limits(2));
    auto o = test::offer_of(publisher);
    test::ForkOffer fo{o.incarnation, o.limits.max_sessions, std::uint8_t(o.limits.allow_every)};
    REQUIRE(test::write_all(child.out(), &fo, sizeof(fo)));
    auto open = test::read_text(child.in());
    REQUIRE_FALSE(open.empty());
    REQUIRE(test::write_text(child.out(), transport::handle(publisher, open)));
    auto transport = test::read_text(child.in());
    REQUIRE_FALSE(transport.empty());
    INFO("UCX_TLS=" << tls << ", subscriber transport=" << transport);
    if(tls == "tcp")
        CHECK(transport.find("tcp") != std::string::npos);
    REQUIRE(child.kill());
    publisher.ucx_start();
    auto until = test::deadline(std::chrono::seconds(12));
    Health h;
    std::uint32_t sends = 0;
    while(std::chrono::steady_clock::now() < until && sends < 1024)
    {
        h = publisher.health();
        if(h.failed_every)
            break;
        auto slot =
            publisher.acquire(std::chrono::steady_clock::now() + std::chrono::milliseconds(20));
        if(slot)
        {
            std::memset(slot->payload().data(), 0x31, slot->payload().size());
            std::move(*slot).publish(sends);
            ++sends;
        }
    }
    while(!h.failed_every && std::chrono::steady_clock::now() < until)
    {
        h = publisher.health();
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    INFO("published after subscriber death=" << sends << " failed every=" << h.failed_every);
    CHECK(h.failed_every == 1);
    publisher.finish();
}

TEST_CASE("a publisher killed with SIGKILL ends an every subscription Failed after its last frame")
{
    auto tls = peer_transport();
    test::Forked child;
    if(child.pid == 0)
    {
        try
        {
            Publisher publisher(test::small_description(), test::small_limits(1));
            auto o = test::offer_of(publisher);
            test::ForkOffer fo{o.incarnation, o.limits.max_sessions,
                               std::uint8_t(o.limits.allow_every)};
            if(!test::write_all(child.out(), &fo, sizeof(fo)))
                ::_exit(2);
            auto open = test::read_text(child.in());
            if(open.empty() || !test::write_text(child.out(), transport::handle(publisher, open)))
                ::_exit(3);
            publisher.ucx_start();
            for(;;) // until killed; an every session holds the device while its ring is full
                if(auto slot = publisher.acquire(test::deadline()))
                    std::move(*slot).publish();
        }
        catch(...)
        {
            ::_exit(4);
        }
    }
    test::ForkOffer fo{};
    REQUIRE(test::read_all(child.in(), &fo, sizeof(fo)));
    transport::Offer o{test::small_description(), test::small_limits(fo.sessions), fo.incarnation};
    o.limits.allow_every = fo.every != 0;
    auto pending = transport::prepare(o, true, test::small_options());
    REQUIRE(test::write_text(child.out(), pending.open_message()));
    auto reply = test::read_text(child.in());
    REQUIRE_FALSE(reply.empty());
    auto sub = std::move(pending).start(reply, test::deadline(), {});
    INFO("UCX_TLS=" << tls << ", subscriber transport=" << sub.health().transport);
    std::vector<std::uint64_t> indices;
    auto take = [&](const Batch &batch)
    {
        for(std::size_t i = 0; i < batch.frames(); ++i)
            indices.push_back(batch.index(i));
    };
    while(indices.size() < 3)
    {
        auto batch = sub.read(test::deadline());
        REQUIRE(batch);
        take(*batch);
    }
    REQUIRE(child.kill());
    auto until = test::deadline(std::chrono::seconds(12));
    while(!sub.outcome() && std::chrono::steady_clock::now() < until)
        if(auto batch = sub.read(until))
            take(*batch);
    REQUIRE(sub.outcome() == Outcome::Failed);
    auto h = sub.health();
    INFO("failure=" << h.failure);
    CHECK_FALSE(h.failure.empty());
    CHECK(h.last_index == indices.back());
    for(std::size_t i = 1; i < indices.size(); ++i)
        CHECK(indices[i] == indices[i - 1] + 1);
    sub.close(test::deadline(std::chrono::seconds(1)));
}

// Over shared memory, a peer that destroyed its worker while its process lives fires no PEER
// error, and eager sends to it still complete Ok: only credit shows it is gone.
TEST_CASE("a latest subscription gone without its Close keeps its session row, with no room")
{
    Publisher publisher(test::small_description(), test::small_limits(1));
    auto sub = test::subscribe(publisher, false, {.budget = 128ull << 10},
                               test::FaultAdapter::lose_close_fence());
    test::publish_one(publisher);
    REQUIRE(sub.read(test::deadline()));
    sub.close(test::deadline()); // no UcxClose: forced close, worker and context destroyed
    REQUIRE(test::eventually([&] { return test::first_row(publisher, "depth") != ~0ull; }));
    for(auto n = 2 * test::first_row(publisher, "depth"); n; --n)
        test::publish_one(publisher); // a latest row never holds the device
    CHECK(test::eventually([&] { return test::first_row(publisher, "room") == 0; }));
    std::this_thread::sleep_for(std::chrono::seconds(3)); // past the 2 s keepalive interval
    auto row = test::sessions(publisher).at(0);
    INFO(row.dump());
    CHECK((row["delivery"] == "latest" && row["state"] == "ready"));
    CHECK(publisher.health().latest == 1);
    test::RawPeer another;
    CHECK(another.open(publisher, false, 4) == wire::NoSession); // until the publisher is destroyed
}

TEST_CASE("an every subscription gone without its Close holds the device until it is evicted")
{
    for(auto timeout : {std::chrono::milliseconds(0), std::chrono::milliseconds(300)})
    {
        auto limits = test::small_limits(1);
        limits.pressure_timeout = timeout;
        Publisher publisher(test::small_description(), limits);
        auto sub = test::subscribe(publisher, true, {.budget = 128ull << 10},
                                   test::FaultAdapter::lose_close_fence());
        test::publish_one(publisher);
        REQUIRE(sub.read(test::deadline()));
        auto room = [&] { return test::first_row(publisher, "room"); };
        REQUIRE(test::eventually([&] { return room() == test::first_row(publisher, "depth"); }));
        sub.close(test::deadline());
        for(auto n = room(); n; --n)
            test::publish_one(publisher);
        auto waiting = std::chrono::steady_clock::now();
        auto slot = publisher.acquire(test::deadline(std::chrono::seconds(4)));
        auto h = publisher.health();
        INFO("pressure_timeout " << timeout.count() << " ms");
        if(timeout.count() == 0)
        {
            CHECK_FALSE(slot); // the device waits for as long as the publisher lives
            CHECK((h.pressure && h.pressure_from == 1 && h.failed_every == 0));
            CHECK(room() == 0);
            continue;
        }
        CHECK(slot);
        CHECK(std::chrono::steady_clock::now() - waiting >= timeout);
        CHECK(test::eventually([&] { return publisher.health().failed_every == 1; }));
        CHECK(test::eventually([&] { return test::sessions(publisher).empty(); }));
    }
}
