// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-ucx project
// SPDX-License-Identifier: LGPL-3.0-or-later
#include "fixtures/fork.h"
#include "fixtures/pair.h"
#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>

#include <cstdlib>
#include <deque>
#include <format>
#include <map>
#include <random>

using namespace TangoUcx;
using namespace std::chrono_literals;

namespace
{
constexpr std::uint32_t k_batch = 4, k_range = 8;
SubscriptionOptions options(std::uint64_t budget)
{
    auto o = test::small_options();
    o.batch = k_batch;
    o.budget = budget;
    return o;
}
Subscription pull(Publisher &publisher, std::uint64_t budget = 4ull << 20)
{
    auto pending = transport::prepare(test::offer_of(publisher), false, options(budget), k_range);
    auto reply = transport::handle(publisher, pending.open_message());
    return std::move(pending).start(reply, test::deadline(), test::closer_for(publisher));
}
// The frames no puller received, as UcxSessions states them. It is sampled with Health.
std::uint64_t missing(const Publisher &publisher)
{
    return nlohmann::json::parse(transport::ucx_sessions(publisher)).at("missing");
}
bool missing_is(const Publisher &publisher, std::uint64_t n)
{
    return test::eventually([&] { return missing(publisher) == n; });
}
// The frame indices a puller receives until its outcome, each with how often. No batch spans
// two ranges, and each array frame is the one test::publish_one made from its index.
using Received = std::map<std::uint64_t, int>;
void drain(Subscription &puller, Received &received)
{
    while(auto batch = puller.read(test::deadline()))
        for(std::size_t i = 0; i < batch->frames(); ++i)
        {
            CHECK(batch->index(i) / k_range == batch->index(0) / k_range);
            CHECK(batch->payload(i)[0] == std::byte(batch->index(i)));
            ++received[batch->index(i)];
        }
}
} // namespace

TEST_CASE("two pullers take the ranges in turn, each frame goes to exactly one, and no batch "
          "spans two ranges")
{
    Publisher publisher(test::small_description(), test::small_limits(4));
    Subscription pullers[] = {pull(publisher), pull(publisher)};
    auto all = test::subscribe(publisher, true); // fan-out beside the set: its own copy of each
    auto newest = test::subscribe(publisher, false);
    CHECK(test::eventually([&] { return publisher.health().every == 3; })); // with the pullers
    CHECK(test::sessions(publisher)[0]["delivery"] == "pull");
    const std::uint64_t frames = 6 * k_range + 3; // the last range is short
    // Two frames, and max_wait cuts a short batch: the first puller's batches no longer start
    // at multiples of the batch size, so only the cut at a range's end keeps them in one range.
    for(std::uint64_t i = 0; i < 2; ++i)
        test::publish_one(publisher, std::byte(i));
    auto first = pullers[0].read(test::deadline());
    REQUIRE(first);
    REQUIRE(first->frames() == 2);
    for(std::uint64_t i = 2; i < frames; ++i)
    {
        if(i == 13) // a cancelled slot uses no frame index, and moves no range
            publisher.acquire(test::deadline()).reset();
        test::publish_one(publisher, std::byte(i));
    }
    publisher.finish(); // with grants queued for both pullers
    Received received{{0, 1}, {1, 1}};
    for(std::size_t p = 0; p < 2; ++p)
    {
        Received own;
        drain(pullers[p], own);
        CHECK(pullers[p].outcome() == Outcome::End);
        for(auto [index, times] : own) // range r is frames r * k_range onward, and goes round
            CHECK(index / k_range % 2 == p);
        received.merge(own);
    }
    REQUIRE(received.size() == frames);
    for(auto [index, times] : received)
        CHECK(times == 1);
    for(std::uint64_t next = 0; auto batch = all.read(test::deadline()); ++next)
        CHECK(batch->index(0) == next);
    CHECK(all.outcome() == Outcome::End);
    CHECK(missing_is(publisher, 0));
}

TEST_CASE("a puller without room for a range is passed over; the device waits when none has "
          "room, and the puller longest without a frame is evicted")
{
    auto limits = test::small_limits(2);
    limits.pressure_timeout = 1s;
    Publisher publisher(test::small_description(), limits);
    auto stalled = pull(publisher, 128ull << 10), roomy = pull(publisher, 512ull << 10);
    REQUIRE(test::eventually([&] { return test::sessions(publisher).size() == 2; }));
    const std::uint64_t depth = test::sessions(publisher)[0]["depth"],
                        held = depth - depth % k_range;
    auto publish = [&](std::uint64_t index, std::chrono::milliseconds wait)
    {
        auto slot = publisher.acquire(std::chrono::steady_clock::now() + wait);
        if(slot)
        {
            slot->payload()[0] = std::byte(index);
            std::move(*slot).publish();
        }
        return bool(slot);
    };
    publisher.ucx_start();
    std::uint64_t published = 0;
    // Neither reads. The first fills, and from then on every range goes to the second: the
    // device does not wait while any puller has room.
    for(; published < 2 * held + 4 * k_range; ++published)
        REQUIRE(publish(published, 1s));
    nlohmann::json rows;
    CHECK(test::eventually(
        [&] { return (rows = test::sessions(publisher))[1]["sent"] == published - held; }));
    INFO(rows.dump());
    CHECK((rows[0]["sent"] == held && rows[0]["passes"] >= 4 && rows[1]["passes"] == 0));
    CHECK_FALSE(publisher.health().pressure);
    // Now the second fills too. The device waits for the one longest without a frame.
    while(publish(published, 300ms))
        ++published;
    auto h = publisher.health();
    CHECK(test::eventually([&] { return (h = publisher.health()).pressure; }));
    CHECK((h.pressure_from == 1 && h.failed_every == 0));
    CHECK(test::eventually([&] { return publisher.health().failed_every == 1; }));
    Received received;
    std::thread reader([&] { drain(roomy, received); }); // room again, before it is evicted too
    for(auto more = published + 2 * k_range; published < more; ++published)
        REQUIRE(publish(published, 5s));
    publisher.finish();
    reader.join();
    // What the evicted puller held is missing, and the one that is left is told.
    CHECK(missing_is(publisher, held));
    CHECK(received.size() == published - held);
    CHECK(roomy.outcome() == Outcome::Failed);
    CHECK(roomy.health().failure == std::format("{} frames reached no puller", held));
    Received own; // what landed before End(evicted) is still delivered
    drain(stalled, own);
    CHECK(own.size() == held);
    CHECK(stalled.health().failure.starts_with("evicted"));
    received.merge(own);
    CHECK(received.size() == published);
}

TEST_CASE("a puller that closes: what is left of its range goes to the next puller, what was in "
          "its ring is missing, and the other ends Failed with the count")
{
    Publisher publisher(test::small_description(), test::small_limits(2));
    auto leaving = pull(publisher), staying = pull(publisher);
    // Range 0 goes to the first, range 1 to the second, and half of range 2 to the first again.
    for(std::uint64_t i = 0; i < 2 * k_range + k_batch; ++i)
        test::publish_one(publisher, std::byte(i));
    // It reads and releases range 0. The four frames of range 2 stay in its ring.
    for(std::uint64_t last = 0; last != k_range - 1;)
    {
        auto batch = leaving.read(test::deadline());
        REQUIRE(batch);
        last = batch->index(batch->frames() - 1);
    }
    REQUIRE(test::eventually(
        [&]
        {
            auto row = test::sessions(publisher)[0];
            return row["sent"] == 12 && row["room"] == row["depth"].get<std::uint64_t>() - 4;
        }));
    leaving.close(test::deadline());
    for(std::uint64_t i = 2 * k_range + k_batch; i < 4 * k_range; ++i)
        test::publish_one(publisher, std::byte(i));
    publisher.finish();
    Received received;
    drain(staying, received);
    Received expected; // range 1, the rest of range 2, and range 3
    for(std::uint64_t i = k_range; i < 4 * k_range; ++i)
        if(i < 2 * k_range || i >= 2 * k_range + k_batch)
            expected[i] = 1;
    CHECK(received == expected);
    CHECK(staying.outcome() == Outcome::Failed);
    CHECK(staying.health().failure == "4 frames reached no puller");
    CHECK(missing_is(publisher, 4)); // none that was never sent to it
    CHECK(publisher.health().failed_every == 0);
}

TEST_CASE("a pull Open is refused after the start, where every is refused, and with a range "
          "that is not whole batches, exceeds its ring or differs from the set's")
{
    Publisher publisher(test::small_description(), test::small_limits(3));
    auto pending = transport::prepare(test::offer_of(publisher), false, options(1 << 20), k_range);
    wire::Request r;
    REQUIRE(wire::parse(pending.open_message(), r) == wire::Ok);
    auto status = [&](Publisher &to, auto change)
    {
        auto m = *r.open;
        m.instance_id = test::offer_of(to).instance_id;
        change(m);
        wire::Reply reply;
        wire::parse(transport::handle(to, wire::format(m)), reply);
        return reply.status;
    };
    CHECK(status(publisher, [](auto &m) { m.range = k_range + 1; }) == wire::Range);
    CHECK(status(publisher, [](auto &m) { m.range = m.depth + k_batch; }) == wire::Range);
    CHECK(status(publisher, [](auto &) {}) == wire::Ok); // the set's first: its range is the set's
    CHECK(status(publisher, [](auto &m) { ++m.session, m.range = 2 * k_range; }) == wire::Range);
    CHECK(status(publisher, [](auto &m) { ++m.session; }) == wire::Ok);
    publisher.ucx_start();
    CHECK(status(publisher, [](auto &m) { m.session += 2; }) == wire::Started);
    auto limits = test::small_limits(1);
    limits.allow_every = false;
    Publisher refusing(test::small_description(), limits);
    CHECK(status(refusing, [](auto &) {}) == wire::EveryRefused);
    CHECK_THROWS(transport::prepare(test::offer_of(refusing), false, options(1 << 20), k_range));
}

// The simulation of a set: nothing here is in step with the publisher's loop.
TEST_CASE("a set under random lengths, releases and cancelled slots, with a puller that closes: "
          "each frame reaches exactly one puller, whole, or is counted missing")
{
    bool opaque = GENERATE(false, true);
    std::mt19937_64 random(11);
    Publisher publisher(opaque ? test::opaque_description() : test::small_description(),
                        test::small_limits(3));
    std::vector<Subscription> pullers; // rings of a few ranges: passes, and waits within a range
    for(int p = 0; p < 3; ++p)
        pullers.push_back(pull(publisher, opaque ? 1ull << 20 : 128ull << 10));
    REQUIRE(test::eventually([&] { return test::sessions(publisher).size() == 3; }));
    const auto idle = test::sessions(publisher)[2]["room"]; // of a ring that holds nothing
    Received received;
    std::vector<std::size_t> lengths; // of each frame index
    std::deque<Batch> kept;
    auto take = [&](Subscription &from, Deadline by)
    {
        auto batch = from.read(by);
        if(!batch)
            return false;
        for(std::size_t i = 0; i < batch->frames(); ++i)
        {
            auto index = batch->index(i);
            CHECK(index / k_range == batch->index(0) / k_range);
            CHECK((opaque ? test::is_opaque_frame(*batch, i, lengths.at(index), index)
                          : batch->payload(i)[0] == std::byte(index)));
            ++received[index];
        }
        kept.push_back(std::move(*batch));
        return true;
    };
    const std::uint64_t frames = 600;
    publisher.ucx_start();
    for(std::uint64_t i = 0; i < frames;)
    {
        auto by = std::chrono::steady_clock::now() + 2ms;
        if(random() % 16 == 0)
            publisher.acquire(by).reset(); // a cancelled slot
        auto bytes = 1 + random() % (64 << 10);
        if(auto slot = opaque ? std::nullopt : publisher.acquire(by))
        {
            slot->payload()[0] = std::byte(i);
            std::move(*slot).publish();
            lengths.push_back(0), ++i;
        }
        else if(opaque && test::publish_opaque(publisher, bytes, i, by))
            lengths.push_back(bytes), ++i;
        take(pullers[random() % pullers.size()], std::chrono::steady_clock::now());
        while(kept.size() > random() % 8)
            kept.pop_front(); // released in any amounts, and after any wait
        if(i != frames / 2 || pullers.size() < 3)
            continue;
        // The third puller leaves with nothing in its ring: all it was sent is read and credited.
        REQUIRE(test::eventually(
            [&]
            {
                for(kept.clear(); take(pullers[2], std::chrono::steady_clock::now() + 20ms);)
                    kept.clear();
                return test::sessions(publisher)[2]["room"] == idle;
            }));
        pullers.pop_back();
    }
    publisher.finish();
    kept.clear();
    for(auto &puller : pullers)
        while(take(puller, test::deadline()))
            kept.clear();
    // A slot granted for a range of the puller that left finds no taker when the others are full.
    CHECK(missing_is(publisher, frames - received.size()));
    auto lost = missing(publisher);
    INFO((opaque ? "opaque" : "array") << ", missing " << lost);
    for(auto [index, times] : received)
        CHECK(times == 1);
    for(auto &puller : pullers)
        CHECK(puller.outcome() == (lost ? Outcome::Failed : Outcome::End));
}

TEST_CASE("a puller killed with SIGKILL: the frames not yet sent to it go to the puller that is "
          "left, which ends Failed with the count of those it held")
{
    auto tls = GENERATE(as<std::string>{}, "all", "tcp");
    ::setenv("TANGO_UCX_UCX_TLS", tls.c_str(), 1);
    test::Forked child;
    if(child.pid == 0)
    {
        test::ForkOffer fo{};
        if(!test::read_all(child.in(), &fo, sizeof(fo)))
            ::_exit(2);
        try
        {
            auto pending = transport::prepare(
                {test::small_description(), test::small_limits(fo.sessions), fo.instance_id}, false,
                options(128ull << 10), k_range);
            if(!test::write_text(child.out(), pending.open_message()))
                ::_exit(3);
            auto sub =
                std::move(pending).start(test::read_text(child.in()), test::deadline(10s), {});
            if(!test::write_text(child.out(), "started"))
                ::_exit(4);
            for(;;) // it never reads: what it is sent stays in its ring
                ::pause();
        }
        catch(...)
        {
            ::_exit(5);
        }
    }
    Publisher publisher(test::small_description(), test::small_limits(2));
    test::ForkOffer fo{test::offer_of(publisher).instance_id, 2, 1};
    REQUIRE(test::write_all(child.out(), &fo, sizeof(fo)));
    auto open = test::read_text(child.in());
    REQUIRE_FALSE(open.empty());
    REQUIRE(test::write_text(child.out(), transport::handle(publisher, open)));
    REQUIRE(test::read_text(child.in()) == "started");
    auto staying = pull(publisher); // its ring holds every frame of this test
    std::uint64_t published = 0;
    for(; published < 2 * k_range; ++published) // range 0 is the child's
        test::publish_one(publisher, std::byte(published));
    REQUIRE(test::eventually([&] { return test::sessions(publisher)[0]["sent"] == k_range; }));
    REQUIRE(child.kill());
    // Until UCX reports the peer, its ranges are still sent to it; once its ring would be full
    // it is passed over. The device is never held.
    for(auto until = test::deadline(12s);
        !publisher.health().failed_every && std::chrono::steady_clock::now() < until;)
        if(published < 1024)
            test::publish_one(publisher, std::byte(published++));
        else
            std::this_thread::sleep_for(10ms);
    REQUIRE(publisher.health().failed_every == 1);
    for(auto more = published + 2 * k_range; published < more; ++published)
        test::publish_one(publisher, std::byte(published));
    publisher.finish();
    Received received;
    drain(staying, received);
    auto lost = published - received.size();
    INFO("UCX_TLS=" << tls << ", published " << published << ", missing " << lost);
    CHECK(lost >= k_range);
    CHECK(missing_is(publisher, lost)); // each frame reached it, or is counted
    CHECK(staying.outcome() == Outcome::Failed);
    CHECK(staying.health().failure == std::format("{} frames reached no puller", lost));
    CHECK(test::eventually([&] { return publisher.health().every == 1; }));
}
