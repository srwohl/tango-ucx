// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-ucx project
// SPDX-License-Identifier: LGPL-3.0-or-later
#include "fixtures/pair.h"
#include <catch2/catch_test_macros.hpp>
#include <memory.h>
#include <ucp.h>

#include <ctime>
#include <fstream>
#include <future>
#include <string>
#include <thread>

using namespace TangoUcx;

namespace
{
// Reads until the subscription reports its outcome; the frame indices it received.
std::vector<std::uint64_t> read_to_outcome(Subscription &sub, std::optional<Outcome> &outcome)
{
    std::vector<std::uint64_t> indices;
    for(auto until = test::deadline(); std::chrono::steady_clock::now() < until;)
    {
        if(auto batch = sub.read(test::deadline(std::chrono::seconds(1))))
            for(std::size_t i = 0; i < batch->frames(); ++i)
                indices.push_back(batch->index(i));
        else if((outcome = sub.outcome()))
            break;
    }
    return indices;
}
wire::Status open_status(Publisher &publisher, const std::string &open)
{
    wire::Reply reply;
    REQUIRE(wire::parse(transport::handle(publisher, open), reply));
    return reply.status;
}
std::size_t shared_memory_mappings()
{
    std::ifstream maps("/proc/self/maps");
    std::size_t count = 0;
    for(std::string line; std::getline(maps, line);)
        count += line.find("/SYSV") != line.npos || line.find("/dev/shm/") != line.npos;
    return count;
}
} // namespace

TEST_CASE("a publisher made with limits only has no instance until begin")
{
    Publisher publisher(test::small_limits());
    CHECK_FALSE(publisher.acquire(test::deadline(std::chrono::seconds(0))));
    CHECK_THROWS_AS(publisher.description(), std::logic_error);
    CHECK_THROWS_AS(transport::ucx_stream(publisher), std::runtime_error);
    CHECK(publisher.health().frames == 0);
    publisher.begin(test::small_description());
    CHECK(publisher.description().shape == test::small_description().shape);
    auto every = test::subscribe(publisher, true);
    test::publish_one(publisher, std::byte{0x11});
    auto batch = every.read(test::deadline());
    REQUIRE(batch);
    CHECK(batch->index(0) == 0);
    batch.reset();
    every.close(test::deadline());
}

TEST_CASE("begin makes a new instance: a new instance ID, frame indices from 0, and End for the "
          "earlier instance's sessions")
{
    Publisher publisher(test::small_description(), test::small_limits(3));
    auto first_id = test::offer_of(publisher).instance_id;
    auto earlier = test::subscribe(publisher, true);
    // Prepared against the first instance and sent after the second has begun.
    auto stale = transport::prepare(test::offer_of(publisher), true, test::small_options());
    for(int i = 0; i < 3; ++i)
        test::publish_one(publisher);

    // The earlier subscriber has read none of them yet: begin still lets it receive all three.
    publisher.begin(test::small_description());
    auto second_id = test::offer_of(publisher).instance_id;
    CHECK(second_id != first_id);
    CHECK(publisher.health().frames == 0);
    CHECK_FALSE(publisher.health().started);
    CHECK(open_status(publisher, stale.open_message()) == wire::InstanceId);

    std::optional<Outcome> outcome;
    auto indices = read_to_outcome(earlier, outcome);
    CHECK(indices == std::vector<std::uint64_t>{0, 1, 2});
    REQUIRE(outcome);
    CHECK(*outcome == Outcome::End);

    auto later = test::subscribe(publisher, true);
    test::publish_one(publisher);
    test::publish_one(publisher);
    publisher.finish();
    indices = read_to_outcome(later, outcome);
    CHECK(indices == std::vector<std::uint64_t>{0, 1});
    REQUIRE(outcome);
    CHECK(*outcome == Outcome::End);
}

TEST_CASE("the arena is kept across instances whose rings fit in it")
{
    auto limits = test::small_limits(1);
    limits.budget = 4ull << 20;
    Description big{Element::U8, {256 << 10}, 0, {}}; // 256 KiB frames: the budget sets the ring
    Publisher publisher(big, limits);
    auto pinned = transport::pinned_bytes();
    for(int i = 0; i < 3; ++i)
    {
        publisher.begin(big);
        CHECK(transport::pinned_bytes() == pinned);
    }
    // Another description of the same size, then a smaller ring: both fit in the arena there is.
    publisher.begin({Element::U16, {128 << 10}, 0, {}});
    CHECK(transport::pinned_bytes() == pinned);
    publisher.begin({Element::U8, {1024}, 0, {}}); // 1,024 slots of 1 KiB
    CHECK(transport::pinned_bytes() == pinned);
    auto every = test::subscribe(publisher, true, {.budget = 1ull << 20});
    test::publish_one(publisher, std::byte{0x42});
    auto batch = every.read(test::deadline());
    REQUIRE(batch);
    CHECK(static_cast<const std::byte *>(batch->payload())[1023] == std::byte{0x42});
    batch.reset();
    every.close(test::deadline());
}

TEST_CASE("closed sessions keep none of their peers' shared memory mapped across instances")
{
    Publisher publisher(test::small_limits());
    auto run = [&]
    {
        publisher.begin(test::small_description());
        auto every = test::subscribe(publisher, true);
        test::publish_one(publisher);
        test::publish_one(publisher);
        publisher.finish();
        std::optional<Outcome> outcome;
        CHECK(read_to_outcome(every, outcome).size() == 2);
        every.close(test::deadline());
    };
    run();
    auto mapped = shared_memory_mappings();
    for(int i = 0; i < 10; ++i)
        run();
    CHECK(test::eventually([&] { return shared_memory_mappings() <= mapped; },
                           std::chrono::seconds(2)));
}

TEST_CASE("a refused description leaves the active instance as it was")
{
    Publisher publisher(test::small_description(), test::small_limits());
    auto id = test::offer_of(publisher).instance_id;
    CHECK_THROWS_AS(publisher.begin({Element::U8, {}, 0, {}}), std::invalid_argument);
    auto text = test::small_description();
    text.application_text = std::string(4096, 'x');
    CHECK_THROWS_AS(publisher.begin(text), std::invalid_argument);
    CHECK(test::offer_of(publisher).instance_id == id);
    auto every = test::subscribe(publisher, true);
    test::publish_one(publisher);
    CHECK(every.read(test::deadline()));
    every.close(test::deadline());
}

TEST_CASE("begin waits for a slot the device still holds, and returns once it is cancelled")
{
    Publisher publisher(test::small_description(), test::small_limits());
    publisher.ucx_start();
    auto slot = publisher.acquire(test::deadline());
    REQUIRE(slot);
    auto started = std::chrono::steady_clock::now();
    auto begun =
        std::async(std::launch::async, [&] { publisher.begin(test::small_description()); });
    CHECK(begun.wait_for(std::chrono::milliseconds(200)) == std::future_status::timeout);
    slot.reset(); // cancelled
    begun.get();
    CHECK(std::chrono::steady_clock::now() - started < std::chrono::seconds(1));
    CHECK_FALSE(publisher.health().started);
}

TEST_CASE("a publisher with no instance waits without polling")
{
    if(!transport::ucp::idle_waits())
        return; // TANGO_UCX_IDLE=spin: the loop only polls
    Publisher publisher(test::small_limits());
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    auto cpu = std::clock();
    auto start = std::chrono::steady_clock::now();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    std::chrono::duration<double> wall = std::chrono::steady_clock::now() - start;
    auto used = double(std::clock() - cpu) / CLOCKS_PER_SEC;
    INFO("CPU time " << used << " s in " << wall.count() << " s");
    CHECK(used < 0.05 * wall.count());
    // A control request still gets its answer at once.
    auto asked = std::chrono::steady_clock::now();
    wire::Reply reply;
    REQUIRE(wire::parse(transport::handle(publisher, wire::format(wire::CloseMessage{7})), reply));
    CHECK(reply.status == wire::UnknownSession);
    CHECK(std::chrono::steady_clock::now() - asked < std::chrono::milliseconds(50));
}
