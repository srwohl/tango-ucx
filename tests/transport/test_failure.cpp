// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
// SPDX-License-Identifier: LGPL-3.0-or-later
#include "fixtures/fault_adapter.h"
#include "fixtures/fork.h"
#include <catch2/catch_test_macros.hpp>
#include <tables.h>

#include <array>
#include <csignal>
#include <cstdlib>
#include <string_view>
#include <sys/wait.h>
#include <thread>

using namespace TangoBulk;

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

TEST_CASE("a subscriber killed with SIGKILL is detected through the UCX peer endpoint")
{
    ::setenv("UCX_TLS", "tcp", 1); // Use a process-independent transport for the fixture.
    int to_child[2], to_parent[2];
    REQUIRE(::pipe(to_child) == 0);
    REQUIRE(::pipe(to_parent) == 0);
    auto pid = ::fork();
    REQUIRE(pid >= 0);
    struct ChildGuard
    {
        pid_t pid;
        ~ChildGuard()
        {
            if(pid > 0)
            {
                int status = 0;
                ::kill(pid, SIGKILL);
                ::waitpid(pid, &status, 0);
            }
        }
    } child_guard{pid};
    if(pid == 0)
    {
        ::close(to_child[1]);
        ::close(to_parent[0]);
        test::ForkOffer fo{};
        if(!test::read_all(to_child[0], &fo, sizeof(fo)))
            ::_exit(2);
        Description d = test::small_description();
        PublisherLimits l = test::small_limits(fo.sessions);
        l.allow_every = fo.every != 0;
        SubscriptionOptions opts = test::small_options();
        transport::Offer o{d, l, fo.incarnation};
        try
        {
            auto pending = transport::prepare(o, true, opts);
            auto open = pending.open_message();
            if(!test::write_text(to_parent[1], open))
                ::_exit(3);
            auto reply = test::read_text(to_child[0]);
            if(reply.empty())
                ::_exit(4);
            auto sub = std::move(pending).start(
                reply, std::chrono::steady_clock::now() + std::chrono::seconds(10), {});
            auto health = sub.health();
            auto transport_bytes = std::uint8_t(health.transport.size());
            char ready = 'R';
            if(!test::write_all(to_parent[1], &ready, 1) ||
               !test::write_all(to_parent[1], &transport_bytes, sizeof(transport_bytes)) ||
               !test::write_all(to_parent[1], health.transport.data(), transport_bytes))
                ::_exit(5);
            for(;;)
                ::pause();
        }
        catch(...)
        {
            ::_exit(6);
        }
    }
    ::close(to_child[0]);
    ::close(to_parent[1]);
    Publisher publisher(test::small_description(), test::small_limits(2));
    auto o = test::offer_of(publisher);
    test::ForkOffer fo{o.incarnation, o.limits.max_sessions, std::uint8_t(o.limits.allow_every)};
    REQUIRE(test::write_all(to_child[1], &fo, sizeof(fo)));
    auto open = test::read_text(to_parent[0]);
    REQUIRE_FALSE(open.empty());
    auto reply = transport::handle(publisher, open);
    REQUIRE_FALSE(reply.empty());
    REQUIRE(test::write_text(to_child[1], reply));
    char ready = 0;
    REQUIRE(test::read_all(to_parent[0], &ready, 1));
    REQUIRE(ready == 'R');
    std::uint8_t transport_bytes = 0;
    REQUIRE(test::read_all(to_parent[0], &transport_bytes, sizeof(transport_bytes)));
    std::array<char, 32> transport{};
    REQUIRE(transport_bytes < transport.size());
    REQUIRE(test::read_all(to_parent[0], transport.data(), transport_bytes));
    INFO("subscriber transport=" << transport.data());
    CHECK(std::string_view(transport.data()).find("tcp") != std::string_view::npos);
    REQUIRE(::kill(pid, SIGKILL) == 0);
    int status = 0;
    REQUIRE(::waitpid(pid, &status, 0) == pid);
    CHECK(WIFSIGNALED(status));
    CHECK(WTERMSIG(status) == SIGKILL);
    auto until = std::chrono::steady_clock::now() + std::chrono::seconds(12);
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
    ::close(to_child[1]);
    ::close(to_parent[0]);
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
    sub.close(test::deadline()); // no BulkClose: forced close, worker and context destroyed
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

TEST_CASE("an every subscription gone before its first frame fails that frame's send")
{
    Publisher publisher(test::small_description(), test::small_limits(1));
    auto sub = test::subscribe(publisher, true, test::small_options(),
                               test::FaultAdapter::lose_close_fence());
    sub.close(test::deadline());
    test::publish_one(publisher); // UCX cannot attach the gone worker's shared memory
    CHECK(test::eventually([&] { return publisher.health().failed_every == 1; }));
    CHECK(test::eventually([&] { return test::sessions(publisher).empty(); }));
}
