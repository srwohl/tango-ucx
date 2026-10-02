// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-ucx project
// SPDX-License-Identifier: LGPL-3.0-or-later
#include "fixtures/fault_adapter.h"
#include <catch2/catch_test_macros.hpp>
#include <tables.h>

#include <atomic>

using namespace TangoUcx;
using namespace std::chrono_literals;

namespace
{
// A subscription whose publisher is a RawPeer, which sends Frame and End by hand.
struct FakePublisher
{
    test::RawPeer peer;
    Subscription sub;
    FakePublisher()
    {
        auto pending = transport::prepare({test::small_description(), test::small_limits(), 7},
                                          true, test::small_options());
        auto reply = peer.accept(pending);
        sub = std::move(pending).start(reply, test::deadline(), {});
    }
    void frame(std::uint64_t sequence, std::uint64_t session_offset = 0)
    {
        wire::FrameMessage f{peer.session + session_offset, sequence, 128, sequence, 1};
        f.record_bytes = 8;
        std::array<std::byte, 48> header;
        std::array<std::byte, 128> payload{};
        wire::encode_into(header, f);
        peer.send(wire::Frame, header, payload);
    }
    void end(std::uint64_t sequences)
    {
        peer.send(wire::End, wire::encode(wire::EndMessage{peer.session, sequences, sequences}));
    }
};
} // namespace

TEST_CASE("a held batch keeps its host storage alive after close")
{
    Publisher publisher(test::small_description(), test::small_limits());
    auto sub = test::subscribe(publisher, true);
    test::publish_one(publisher, std::byte{0x6d});
    auto batch = sub.read(test::deadline());
    REQUIRE(batch);
    REQUIRE(batch->frames() == 1);
    auto *payload = static_cast<const std::byte *>(batch->payload());
    REQUIRE(payload);
    CHECK(payload[0] == std::byte{0x6d});
    sub.close(test::deadline());
    CHECK(payload[0] == std::byte{0x6d});
    batch.reset();
    publisher.finish();
}

TEST_CASE("End is a bound and does not complete before an earlier frame lands")
{
    using namespace TangoUcx::transport;
    Description d = test::small_description();
    PublisherLimits limits = test::small_limits();
    Offer offer{d, limits, 1};
    auto sub = std::make_unique<Sub>(offer, true, test::small_options());
    sub->plan.sizes = sizes_of(d);
    sub->plan.D = 2;
    sub->plan.batch = 1;
    sub->rings = std::make_shared<Rings>();
    sub->rings->plan = sub->plan;
    sub->rings->payload = new std::byte[256];
    sub->rings->storage = std::shared_ptr<void>(sub->rings->payload, [](void *p)
                                                { delete[] static_cast<std::byte *>(p); });
    sub->rings->records.resize(2 * sub->plan.sizes.record_stride);
    sub->rings->batches = std::vector<BatchRow>(2);
    sub->rings->released.reserve(2);
    sub->rings->busy.assign(1, 0);
    sub->rings->landed.assign(1, 0);
    sub->rings->returned.assign(1, 0);
    sub->queue.reserve(2);
    sub->has_end = true;
    sub->end = {1, 1, 1, TangoUcx::wire::FinishedReason};
    sub->end_at_ns = 1'000'000'000LL;
    sub->decide(2'000'000'000LL);
    CHECK(sub->outcome.load() == 0);
    wire::put64(sub->rings->record(0), 0);
    sub->landed(0);
    REQUIRE(sub->assemble(2'000'000'001LL));
    sub->decide(2'000'000'002LL);
    CHECK(sub->outcome.load() == int(Outcome::End));
}

TEST_CASE("batches are cut at their size, at the ring's wrap, after max_wait and at End")
{
    Publisher publisher({Element::U8, {64 << 10}, 8, "<u8"}, test::small_limits(1));
    auto options = SubscriptionOptions{.budget = 672ull << 10, .batch = 4, .max_wait = 1s};
    auto pending = transport::prepare(test::offer_of(publisher), true, options);
    wire::Request r;
    REQUIRE(wire::parse(pending.open_message(), r) == wire::Ok);
    REQUIRE(r.open->depth == 8);
    auto sub = std::move(pending).start(transport::handle(publisher, pending.open_message()),
                                        test::deadline(), test::closer_for(publisher));
    std::uint64_t next = 0;
    // size; max_wait; the wrap at slot 8, then max_wait; End
    for(auto [publish, sizes] : {std::pair{4, std::vector{4}}, {3, {3}}, {3, {1, 2}}, {2, {2}}})
    {
        for(int i = 0; i < publish; ++i)
            test::publish_one(publisher);
        if(next == 10)
            publisher.finish();
        for(auto size : sizes)
        {
            auto batch = sub.read(test::deadline());
            REQUIRE(batch);
            CHECK(batch->frames() == std::size_t(size));
            for(std::size_t i = 0; i < batch->frames(); ++i)
                CHECK(batch->index(i) == next++);
        }
    }
    CHECK_FALSE(sub.read(test::deadline()));
    CHECK(sub.outcome() == Outcome::End);
    sub.close(test::deadline());
}

TEST_CASE("a latest batch is replaced only while it is unread")
{
    Publisher publisher(test::small_description(), test::small_limits(1));
    auto sub = test::subscribe(publisher, false);
    test::publish_one(publisher, std::byte{1});
    auto held = sub.read(test::deadline());
    REQUIRE(held);
    for(std::uint64_t i : {2, 3}) // each is sent, and each replaces the one before it unread
    {
        test::publish_one(publisher, std::byte(i));
        REQUIRE(test::eventually([&] { return test::first_row(publisher, "sent") == i; }));
    }
    REQUIRE(test::eventually([&] { return sub.health().frames == 3; }));
    auto newest = sub.read(test::deadline());
    REQUIRE(newest);
    CHECK(newest->index(0) == 2);
    CHECK_FALSE(sub.read(std::chrono::steady_clock::now() + 100ms));
    CHECK(static_cast<const std::uint8_t *>(held->payload())[0] == 1);
    sub.close(test::deadline());
}

TEST_CASE("a copy holds no receive slots")
{
    Publisher publisher(test::small_description(), test::small_limits(1));
    auto sub = test::subscribe(publisher, true);
    test::publish_one(publisher, std::byte{0x77});
    auto batch = sub.read(test::deadline());
    REQUIRE(batch);
    auto copy = batch->copy();
    batch.reset();
    CHECK(test::eventually(
        [&] { return test::first_row(publisher, "room") == test::first_row(publisher, "depth"); }));
    CHECK(copy.frames() == 1);
    CHECK(copy.index(0) == 0);
    CHECK(static_cast<const std::uint8_t *>(copy.payload())[127] == 0x77);
    sub.close(test::deadline());
}

TEST_CASE("End that arrives before its frames still ends End")
{
    FakePublisher fake;
    fake.end(2);
    fake.frame(0);
    fake.frame(1);
    CHECK(fake.sub.read(test::deadline()));
    CHECK(fake.sub.read(test::deadline()));
    CHECK_FALSE(fake.sub.read(test::deadline()));
    CHECK(fake.sub.outcome() == Outcome::End);
}

TEST_CASE("a sequence below End that never arrives ends Failed after the grace")
{
    FakePublisher fake;
    fake.frame(1);
    fake.end(2);
    auto start = std::chrono::steady_clock::now();
    CHECK_FALSE(fake.sub.read(test::deadline()));
    CHECK(std::chrono::steady_clock::now() - start >= 2s);
    CHECK(fake.sub.outcome() == Outcome::Failed);
    CHECK(fake.sub.health().failure == "a sequence below End never arrived");
}

TEST_CASE("a frame for another session fails the subscription")
{
    FakePublisher fake;
    fake.frame(0, 1);
    CHECK_FALSE(fake.sub.read(test::deadline()));
    CHECK(fake.sub.outcome() == Outcome::Failed);
    CHECK(fake.sub.health().failure.find("invalid frame") != std::string::npos);
}

TEST_CASE("frames arriving while a subscription closes are ignored, and nothing counts failed")
{
    Publisher publisher(test::small_description(), test::small_limits(1));
    auto sub = test::subscribe(publisher, true);
    publisher.ucx_start();
    std::jthread device(
        [&](std::stop_token stop)
        {
            while(!stop.stop_requested())
                if(auto slot = publisher.acquire(std::chrono::steady_clock::now() + 10ms))
                    std::move(*slot).publish();
        });
    for(int i = 0; i < 20; ++i)
        CHECK(sub.read(test::deadline()));
    sub.close(test::deadline());
    CHECK(sub.outcome() == Outcome::Cancelled);
    CHECK(test::eventually([&] { return test::sessions(publisher).empty(); }));
    CHECK(publisher.health().failed_every == 0);
}

TEST_CASE("a start that fails after an accepted Open closes the session the Open made")
{
    Publisher publisher(test::small_description(), test::small_limits(1)); // one session row
    auto pending = transport::prepare(test::offer_of(publisher), true, test::small_options());
    wire::Reply reply;
    REQUIRE(wire::parse(transport::handle(publisher, pending.open_message()), reply));
    REQUIRE(reply.status == wire::Ok);
    reply.address_bytes = 0; // without the publisher's address, start cannot make its endpoint
    CHECK_THROWS(std::move(pending).start(wire::format(reply), test::deadline(),
                                          test::closer_for(publisher)));
    auto sub = test::subscribe(publisher, true); // refused with no-session if the row were kept
    test::publish_one(publisher);
    CHECK(sub.read(test::deadline()));
    sub.close(test::deadline());
}

TEST_CASE("a start with no Ready by its deadline sends Close for its session")
{
    test::RawPeer peer; // answers the Open but never sends Ready
    auto pending = transport::prepare({test::small_description(), test::small_limits(), 7}, true,
                                      test::small_options());
    wire::Request open;
    REQUIRE(wire::parse(pending.open_message(), open) == wire::Ok);
    wire::Reply reply{wire::Ok};
    reply.address_bytes = std::uint16_t(peer.worker.address().size());
    std::ranges::copy(peer.worker.address(), reply.address.begin());
    std::vector<std::uint64_t> closed;
    auto closer = [&](std::string_view doc, Deadline)
    {
        wire::Request r;
        if(wire::parse(doc, r) == wire::Ok && !r.open)
            closed.push_back(r.close.session);
        return false;
    };
    CHECK_THROWS(std::move(pending).start(wire::format(reply), test::deadline(1s), closer));
    CHECK(closed == std::vector{open.open->session});
}

TEST_CASE("a publisher that closes tells its subscriptions why they end")
{
    std::optional<Publisher> publisher(std::in_place, test::small_description(),
                                       test::small_limits(1));
    auto sub = test::subscribe(*publisher, true, test::small_options(),
                               test::FaultAdapter::lose_close_fence());
    publisher.reset();
    CHECK_FALSE(sub.read(test::deadline()));
    CHECK(sub.outcome() == Outcome::Failed);
    CHECK(sub.health().failure == "the publisher closed");
}
