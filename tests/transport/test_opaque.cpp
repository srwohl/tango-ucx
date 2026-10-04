// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-ucx project
// SPDX-License-Identifier: LGPL-3.0-or-later
#include "fixtures/fault_adapter.h"
#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <tables.h>

#include <cstdlib>
#include <deque>
#include <random>
#include <thread>

using namespace TangoUcx;
using namespace std::chrono_literals;

namespace
{
constexpr std::uint64_t k_max = 1ull << 20;
PublisherLimits limits(std::uint32_t sessions = 1)
{
    auto l = test::small_limits(sessions);
    l.budget = 16ull << 20;
    return l;
}
SubscriptionOptions options(std::uint64_t budget = 8ull << 20, std::uint32_t batch = 1)
{
    auto o = test::small_options();
    o.budget = budget;
    o.batch = batch;
    return o;
}
// A subscription to an opaque stream, and the bytes of the packed ring its Open asked for.
struct Opened
{
    Subscription sub;
    std::uint64_t ring = 0;
};
Opened open(Publisher &publisher, bool every, SubscriptionOptions o)
{
    auto pending = transport::prepare(test::offer_of(publisher), every, std::move(o));
    wire::Request r;
    REQUIRE(wire::parse(pending.open_message(), r) == wire::Ok);
    auto reply = transport::handle(publisher, pending.open_message());
    return {std::move(pending).start(reply, test::deadline(), test::closer_for(publisher)),
            r.open->ring_bytes};
}
// The length of frame i: 1 to 64 KiB, the fixture's maximum, in no order.
std::size_t length(std::uint64_t i)
{
    return 1 + std::size_t(i * 2654435761u % (64 << 10));
}
Deadline soon()
{
    return std::chrono::steady_clock::now() + 1s;
}
// A subscription to an opaque stream whose publisher is a RawPeer, which sends frames by hand.
struct FakePublisher
{
    test::RawPeer peer;
    Subscription sub;
    std::uint64_t ring = 0;
    FakePublisher()
    {
        auto pending = transport::prepare({test::opaque_description(), test::small_limits(), 7},
                                          true, test::small_options());
        wire::Request r;
        REQUIRE(wire::parse(pending.open_message(), r) == wire::Ok);
        ring = r.open->ring_bytes;
        auto reply = peer.accept(pending);
        sub = std::move(pending).start(reply, test::deadline(), {});
    }
    // The frame its sequence makes, with the position in the packed ring that its header states.
    void frame(std::uint64_t sequence, std::size_t bytes, std::uint64_t position,
               bool packed = true)
    {
        wire::FrameMessage f{peer.session, sequence, bytes, sequence, 1};
        f.record_bytes = 8;
        f.packed = packed;
        f.position = position;
        std::array<std::byte, 56> header;
        std::vector<std::byte> payload(bytes);
        for(std::size_t i = 0; i < bytes; ++i)
            payload[i] = test::opaque_byte(sequence, i);
        peer.send(wire::Frame, {header.data(), wire::encode_into(header, f)}, payload);
    }
    std::string failure()
    {
        while(sub.read(test::deadline())) // what landed before the bad frame is still delivered
        {
        }
        CHECK(sub.outcome() == Outcome::Failed);
        return sub.health().failure;
    }
};
} // namespace

TEST_CASE("opaque frames of 1 byte, an eager size, a rendezvous size and the maximum keep every "
          "byte and their records")
{
    Publisher publisher(test::opaque_description(k_max), limits(2));
    auto every = test::subscribe(publisher, true, options());
    auto latest = test::subscribe(publisher, false, options());
    CHECK(every.description().max_payload_bytes == k_max);
    CHECK(every.description().shape.empty());
    std::uint64_t index = 0;
    for(std::size_t bytes : {std::size_t(1), std::size_t(1000), std::size_t(300 << 10),
                             std::size_t(k_max), std::size_t(7), std::size_t(k_max - 1)})
    {
        REQUIRE(test::publish_opaque(publisher, bytes, 40 + index));
        for(auto *sub : {&every, &latest})
        {
            auto batch = sub->read(test::deadline());
            REQUIRE(batch);
            REQUIRE(batch->frames() == 1);
            CHECK(batch->index(0) == index);
            CHECK(test::is_opaque_frame(*batch, 0, bytes, 40 + index));
            CHECK_THROWS_AS(batch->payload(), std::logic_error); // no packed array of equal frames
            CHECK_THROWS_AS(batch->payload(1), std::out_of_range);
        }
        ++index;
    }
    publisher.finish();
    CHECK_FALSE(every.read(test::deadline()));
    CHECK(every.outcome() == Outcome::End);
}

TEST_CASE("an opaque frame of zero bytes or over the maximum is refused, and the slot stays held")
{
    Publisher publisher(test::opaque_description(4096), limits());
    auto sub = test::subscribe(publisher, true, options());
    publisher.ucx_start();
    auto slot = publisher.acquire(test::deadline());
    REQUIRE(slot);
    CHECK(slot->payload().size() == 4096); // the slot is the most a frame may have
    CHECK_THROWS_AS(std::move(*slot).publish_bytes(0), std::invalid_argument);
    CHECK_THROWS_AS(std::move(*slot).publish_bytes(4097), std::invalid_argument);
    CHECK_THROWS_AS(std::move(*slot).publish(), std::logic_error); // it would send the whole slot
    CHECK_FALSE(sub.read(std::chrono::steady_clock::now() + 100ms));
    slot->payload()[0] = std::byte{0x42};
    std::move(*slot).publish_bytes(1); // the same slot, still the device's
    auto batch = sub.read(test::deadline());
    REQUIRE(batch);
    CHECK(batch->index(0) == 0); // the refusals used no frame index
    CHECK((batch->payload(0).size() == 1 && batch->payload(0)[0] == std::byte{0x42}));

    Publisher array(test::small_description(), limits());
    array.ucx_start();
    auto fixed = array.acquire(test::deadline());
    REQUIRE(fixed);
    CHECK_THROWS_AS(std::move(*fixed).publish_bytes(128), std::logic_error);
}

TEST_CASE("an array batch gives each frame's bytes too")
{
    Publisher publisher(test::small_description(), test::small_limits(1));
    auto sub = test::subscribe(publisher, true, {.budget = 4ull << 20, .batch = 2, .max_wait = 1s});
    test::publish_one(publisher, std::byte{1});
    test::publish_one(publisher, std::byte{2});
    auto batch = sub.read(test::deadline());
    REQUIRE(batch);
    REQUIRE(batch->frames() == 2);
    CHECK(batch->payload(1).data() == static_cast<const std::byte *>(batch->payload()) + 128);
    CHECK((batch->payload(1).size() == 128 && batch->payload(1)[127] == std::byte{2}));
}

// UCX does not promise that headers arrive in the order sent. With two eager rails it delivers a
// small frame while an earlier one is still in fragments, and a subscriber that placed frames by
// their order of arrival failed within some tens of frames. A host with one network device runs
// one rail both times.
TEST_CASE("frames of every size sent at once arrive whole and in sequence, each with its record, "
          "on one eager rail and on two")
{
    auto rails = GENERATE(as<std::string>{}, "1", "2");
    ::setenv("TANGO_UCX_UCX_TLS", "tcp", 1);
    ::setenv("TANGO_UCX_UCX_MAX_EAGER_RAILS", rails.c_str(), 1);
    Publisher publisher(test::opaque_description(k_max), limits());
    auto sub = test::subscribe(publisher, true, options(32ull << 20));
    ::unsetenv("TANGO_UCX_UCX_TLS");
    ::unsetenv("TANGO_UCX_UCX_MAX_EAGER_RAILS");
    // One eager packet, eager fragments and rendezvous, in no order; the frame starts with its seed.
    auto bytes = [](std::uint64_t i)
    {
        const std::size_t most[] = {512, 512, 512, 16'000, 16'000, 200'000, 200'000, k_max};
        auto h = (i + 1) * 0x9e3779b97f4a7c15ull;
        return 8 + std::size_t(h >> 20 & 0xfffff) % (most[h >> 61] - 7);
    };
    const std::uint64_t frames = 4000;
    publisher.ucx_start();
    std::jthread device(
        [&]
        {
            for(std::uint64_t i = 0; i < frames; ++i)
            {
                auto slot = publisher.acquire(test::deadline());
                if(!slot)
                    break;
                std::memcpy(slot->payload().data(), &i, sizeof(i));
                std::memcpy(slot->fields().data(), &i, sizeof(i));
                std::move(*slot).publish_bytes(bytes(i), i);
            }
            publisher.finish();
        });
    std::uint64_t next = 0;
    for(auto batch = sub.read(test::deadline()); batch; batch = sub.read(test::deadline()), ++next)
    {
        auto frame = batch->payload(0);
        auto *record = static_cast<const std::byte *>(batch->records());
        REQUIRE((batch->index(0) == next && frame.size() == bytes(next)));
        REQUIRE((wire::get64(frame.data()) == next && wire::get64(record + 16) == next));
    }
    INFO(rails << " eager rails over " << sub.health().transport << ": " << sub.health().failure);
    CHECK(next == frames);
    CHECK(sub.outcome() == Outcome::End);
}

TEST_CASE("a packed ring takes frames whose headers arrive out of sequence, each where its "
          "publisher states")
{
    FakePublisher fake;
    const std::size_t bytes[] = {100, 2000, 7, 300}; // one directly after the other
    const std::uint64_t at[] = {0, 100, 2100, 2107};
    for(auto q : {2, 0, 3, 1})
        fake.frame(q, bytes[q], at[q]);
    const std::byte *first = nullptr;
    for(std::uint64_t q = 0; q < 4; ++q)
    {
        auto batch = fake.sub.read(test::deadline());
        REQUIRE(batch);
        REQUIRE(batch->index(0) == q); // delivered in sequence
        auto frame = batch->payload(0);
        first = q ? first : frame.data();
        bool same = frame.size() == bytes[q] && frame.data() == first + at[q];
        for(std::size_t i = 0; same && i < frame.size(); ++i)
            same = frame[i] == test::opaque_byte(q, i);
        CHECK(same);
    }
    CHECK_FALSE(fake.sub.outcome());
}

TEST_CASE("grants within a packed ring's room always fit, whatever the lengths and the releases")
{
    using transport::place;
    std::mt19937_64 random(7);
    for(auto [ring, max] : {std::pair{100ull, 30ull}, {64, 32}, {59, 29}, {1000, 7}, {4096, 1365}})
    {
        struct Live
        {
            std::uint64_t at, bytes;
        };
        std::deque<Live> live; // sent and not credited, oldest first
        std::uint64_t sent = 0, credited = 0, grants = 0, wraps = 0, frames = 0;
        for(int step = 0; step < 50'000; ++step)
            switch(random() % 3)
            {
                case 0: // the publisher grants a slot while there is room
                    grants += transport::ring_room(sent - credited, grants, max, ring);
                    break;
                case 1: // the device publishes a granted slot, with any length
                {
                    if(!grants)
                        break;
                    auto bytes = random() % 4 ? 1 + random() % max : max;
                    auto at = place(sent, bytes, ring);
                    REQUIRE(at % ring + bytes <= ring);     // it does not straddle the end
                    REQUIRE(at + bytes - credited <= ring); // what the subscriber checks
                    for(const auto &l : live)               // and it is on no frame still held
                        REQUIRE((at % ring + bytes <= l.at % ring ||
                                 l.at % ring + l.bytes <= at % ring));
                    wraps += at != sent;
                    live.push_back({at, bytes});
                    sent = at + bytes;
                    --grants;
                    ++frames;
                    break;
                }
                default: // the subscriber credits its oldest frame
                    if(!live.empty())
                    {
                        credited = live.front().at + live.front().bytes;
                        live.pop_front();
                    }
            }
        INFO("ring " << ring << ", max " << max << ": " << frames << " frames, " << wraps
                     << " wraps");
        CHECK((frames > 5'000 && wraps > 50));
        CHECK(transport::ring_room(0, 0, max, ring)); // an empty ring always grants
    }
}

TEST_CASE("a held batch keeps its bytes while frames of other lengths fill the packed ring, and "
          "the device waits until it is released")
{
    Publisher publisher(test::opaque_description(), limits());
    auto [sub, ring] = open(publisher, true, options(2ull << 20));
    REQUIRE(test::publish_opaque(publisher, 50'000, 0));
    auto held = sub.read(test::deadline());
    REQUIRE(held);
    std::uint64_t sent = 1, bytes = 50'000;
    for(; test::publish_opaque(publisher, length(sent), sent, soon()); ++sent)
        bytes += length(sent);
    // The device waits for bytes: within two frames of the maximum and one wrap of a full ring.
    CHECK((bytes > ring - 3 * (64 << 10) && bytes <= ring));
    Health h;
    CHECK(test::eventually([&] { return (h = publisher.health()).pressure; }));
    CHECK((h.pressure_from == 1 && h.slots_free > 0));
    CHECK(test::first_row(publisher, "room") == 0);
    for(std::uint64_t i = 1; i < sent; ++i)
    {
        auto batch = sub.read(test::deadline());
        REQUIRE(batch);
        CHECK(test::is_opaque_frame(*batch, 0, length(i), i));
    }
    // Every later batch is released, and nothing is credited past the held one.
    CHECK_FALSE(test::publish_opaque(publisher, 1, sent, soon()));
    CHECK(test::is_opaque_frame(*held, 0, 50'000, 0));
    held.reset();
    for(auto end = sent + 8 * ring / (64 << 10); sent < end; ++sent) // about four laps
    {
        REQUIRE(test::publish_opaque(publisher, length(sent), sent));
        auto batch = sub.read(test::deadline());
        REQUIRE(batch);
        REQUIRE(test::is_opaque_frame(*batch, 0, length(sent), sent));
    }
    REQUIRE(test::publish_opaque(publisher, 12'345, sent));
    held = sub.read(test::deadline());
    REQUIRE(held);
    sub.close(test::deadline());
    CHECK(test::is_opaque_frame(*held, 0, 12'345, sent)); // a held batch outlives the close
}

TEST_CASE("a latest subscription to an opaque stream skips what its packed ring has no room for")
{
    Publisher publisher(test::opaque_description(), limits());
    auto [sub, ring] = open(publisher, false, options(2ull << 20));
    REQUIRE(test::publish_opaque(publisher, 40'000, 0));
    auto held = sub.read(test::deadline());
    REQUIRE(held);
    std::uint64_t sent = 1;
    for(auto end = 4 * ring / (64 << 10); sent < end; ++sent) // two rings' worth: never a wait
        REQUIRE(test::publish_opaque(publisher, length(sent), sent, soon()));
    CHECK(test::eventually([&] { return publisher.health().skipped > 0; }));
    CHECK(publisher.health().pressure == false);
    CHECK(test::is_opaque_frame(*held, 0, 40'000, 0));
    held.reset();
    auto unread = sub.read(test::deadline()); // the newest that landed while the ring had room
    REQUIRE(unread);
    CHECK(test::is_opaque_frame(*unread, 0, length(unread->index(0)), unread->index(0)));
    unread.reset();
    std::optional<Batch> next;
    for(auto until = test::deadline(); !next && std::chrono::steady_clock::now() < until; ++sent)
    {
        REQUIRE(test::publish_opaque(publisher, length(sent), sent));
        next = sub.read(std::chrono::steady_clock::now() + 50ms); // once Credit returned the ring
    }
    REQUIRE(next);
    CHECK(test::is_opaque_frame(*next, 0, length(next->index(0)), next->index(0)));
    CHECK(sub.health().skipped > 0);
}

TEST_CASE("an opaque batch the packed ring cannot hold is cut when the ring is full, and its "
          "subscription is not evicted")
{
    auto l = limits();
    l.pressure_timeout = 300ms;
    Publisher publisher(test::opaque_description(), l);
    auto o = options(2ull << 20, 64); // the ring holds fewer than 32 frames of the maximum
    o.max_wait = 30s;
    auto sub = test::subscribe(publisher, true, o);
    const std::uint64_t frames = 100;
    std::jthread device(
        [&]
        {
            for(std::uint64_t i = 0; i < frames; ++i)
                if(!test::publish_opaque(publisher, 64 << 10, i))
                    break;
            publisher.finish();
        });
    std::uint64_t next = 0;
    while(auto batch = sub.read(test::deadline()))
    {
        CHECK(batch->frames() < 32);
        next += batch->frames();
    }
    CHECK(next == frames);
    CHECK(sub.outcome() == Outcome::End);
    CHECK(publisher.health().failed_every == 0);
}

TEST_CASE("an opaque batch is one run of bytes, cut where the packed ring wraps, and End follows "
          "the last")
{
    Publisher publisher(test::opaque_description(), limits());
    auto o = options(2ull << 20, 4);
    o.max_wait = 1s;
    auto [sub, ring] = open(publisher, true, o);
    const std::uint64_t frames = 3 * ring / 30'000; // about three laps
    auto bytes = [](std::uint64_t i) { return std::size_t(21'000 + i % 7 * 3'000); };
    std::jthread device(
        [&]
        {
            for(std::uint64_t i = 0; i < frames; ++i)
                if(!test::publish_opaque(publisher, bytes(i), i))
                    break;
            publisher.finish();
        });
    std::uint64_t next = 0, wraps = 0;
    const std::byte *before = nullptr;
    std::optional<Batch> copy;
    while(auto batch = sub.read(test::deadline()))
    {
        REQUIRE(batch->frames() <= 4);
        for(std::size_t i = 0; i < batch->frames(); ++i, ++next)
        {
            REQUIRE(batch->index(i) == next);
            REQUIRE(test::is_opaque_frame(*batch, i, bytes(next), next));
            if(i) // each frame starts where the one before it ends
                REQUIRE(batch->payload(i).data() == batch->payload(i - 1).data() + bytes(next - 1));
        }
        wraps += batch->payload(0).data() < before;
        before = batch->payload(0).data();
        if(batch->frames() == 4 && !copy)
            copy = batch->copy();
    }
    CHECK(next == frames);
    CHECK((wraps == 2 || wraps == 3));
    CHECK(sub.outcome() == Outcome::End);
    REQUIRE(copy); // a copy is one run too, and held no ring bytes while the rest arrived
    for(std::size_t i = 0; i < 4; ++i)
        CHECK(test::is_opaque_frame(*copy, i, bytes(copy->index(i)), copy->index(i)));
    CHECK(copy->payload(3).data() == copy->payload(0).data() + bytes(copy->index(0)) +
                                       bytes(copy->index(1)) + bytes(copy->index(2)));
}

TEST_CASE("a publisher that sends more bytes than were credited, a frame placed on another or "
          "across the ring's end, or one of no length or over the maximum, fails the subscription")
{
    const std::size_t max = 64 << 10;
    SECTION("more bytes than the ring has")
    {
        FakePublisher fake;
        auto fit = fake.ring / max;
        for(std::uint64_t q = 0; q <= fit; ++q)
            fake.frame(q, max, q * max);
        std::vector<Batch> held; // nothing is released, so nothing is credited
        for(std::uint64_t q = 0; q < fit; ++q)
        {
            auto batch = fake.sub.read(test::deadline());
            REQUIRE(batch);
            CHECK(batch->payload(0).size() == max);
            held.push_back(std::move(*batch));
        }
        CHECK(fake.failure().find("invalid frame") != std::string::npos);
    }
    SECTION("across the ring's end, within the bytes Credit returned")
    {
        FakePublisher fake;
        fake.frame(0, 100, 0);
        REQUIRE(fake.sub.read(test::deadline())); // released at once, and credited
        REQUIRE(test::eventually([&] { return fake.sub.health().slots_held == 0; }));
        fake.frame(2, 100, fake.ring - 50);
        CHECK(fake.failure().find("invalid frame") != std::string::npos);
    }
    struct Sent
    {
        std::uint64_t sequence;
        std::size_t bytes;
        std::uint64_t at;
        bool packed = true;
    };
    for(const auto &[name, sent] : std::initializer_list<std::pair<const char *, std::vector<Sent>>>{
            {"no byte left for the frame before it", {{1, 100, 0}}},
            {"on the frame before it", {{0, 1000, 0}, {1, 100, 500}}},
            {"not directly after the frame before it", {{0, 1000, 0}, {1, 100, 1001}}},
            {"no byte left before a later frame that arrived first", {{2, 100, 1000}, {0, 1000, 0}}},
            {"on a later frame that arrived first", {{1, 100, 1000}, {0, 1001, 0}}},
            {"over the maximum", {{0, max + 1, 0}}},
            {"no bytes", {{0, 0, 0}}},
            {"an array stream's header, with no position", {{0, 100, 0, false}}}})
        SECTION(name)
        {
            FakePublisher fake;
            for(const auto &f : sent)
                fake.frame(f.sequence, f.bytes, f.at, f.packed);
            CHECK(fake.failure().find("invalid frame") != std::string::npos);
        }
}

TEST_CASE("an Open without a packed ring is refused, and a Credit for more bytes than were sent "
          "ends the session with End(protocol)")
{
    Publisher publisher(test::opaque_description(), limits(2));
    test::RawPeer none, peer;
    CHECK(none.open(publisher, true, 4) == wire::Malformed);
    CHECK(none.open(publisher, true, 4, 2 * (64 << 10) - 1) == wire::Malformed);
    peer.session += 1;
    REQUIRE(peer.open(publisher, true, 4, 1 << 20) == wire::Ok);
    REQUIRE(test::publish_opaque(publisher, 100, 0));
    REQUIRE(peer.progress_until([&] { return peer.count(wire::Frame) == 1; }));
    auto credit = wire::encode(wire::CreditMessage{peer.session, 1, 100});
    peer.send(wire::Credit, credit); // what was sent: the ring is empty again
    REQUIRE(test::publish_opaque(publisher, 200, 1));
    REQUIRE(peer.progress_until([&] { return peer.count(wire::Frame) == 2; }));
    wire::FrameMessage second; // the publisher states where it placed the frame
    REQUIRE(wire::decode(peer.got.back().second, 8, second));
    CHECK((second.packed && second.sequence == 1 && second.position == 100));
    CHECK(peer.count(wire::End) == 0);
    credit = wire::encode(wire::CreditMessage{peer.session, 2, 301});
    peer.send(wire::Credit, credit);
    REQUIRE(peer.progress_until([&] { return peer.count(wire::End) == 1; }));
    wire::EndMessage end;
    REQUIRE(wire::decode(peer.got.back().second, end));
    CHECK(end.reason == wire::Protocol);
}

TEST_CASE("a 256 MiB packed ring holds several hundred frames of about 300 KiB under a 9 MiB "
          "maximum, and none is overwritten while its batch is held")
{
    const std::uint64_t max = 9ull << 20;
    auto l = limits();
    l.budget = 64ull << 20;
    Publisher publisher(test::opaque_description(max), l);
    auto [sub, ring] = open(publisher, true, options(256ull << 20));
    auto bytes = [](std::uint64_t i) { return std::size_t((280 << 10) + i % 41 * 1000); };
    std::vector<Batch> held;
    for(std::uint64_t i = 0; test::publish_opaque(publisher, bytes(i), i, soon()); ++i)
    {
        auto batch = sub.read(test::deadline());
        REQUIRE(batch);
        held.push_back(std::move(*batch));
    }
    INFO("a ring of " << ring << " bytes held " << held.size() << " frames");
    CHECK(held.size() > 750); // slots of the maximum would hold 28
    CHECK(test::eventually([&] { return publisher.health().pressure; }));
    for(std::uint64_t i = 0; i < held.size(); ++i)
        REQUIRE(test::is_opaque_frame(held[i], 0, bytes(i), i));
}

TEST_CASE("an opaque stream's plans: slots of the maximum at the source, a packed ring to receive")
{
    using namespace TangoUcx::transport;
    auto d = test::opaque_description(9ull << 20);
    auto p = plan_publisher(d, {.budget = 64ull << 20, .max_sessions = 1}, 256, 4096);
    CHECK((p.sizes.opaque && p.sizes.frame_bytes == 9ull << 20));
    CHECK(p.source_bytes == p.S * (9ull << 20));
    SubscriptionOptions o;
    o.batch = 100;
    auto s = plan_subscription({d, limits(), 1}, true, o, 256);
    CHECK((s.charge <= o.budget && s.charge > o.budget - (1 << 20)));
    CHECK(s.payload_bytes > 250ull << 20); // the tables take little of 256 MiB
    CHECK((s.D % 100 == 0 && s.D >= 256 * (s.payload_bytes / (9ull << 20)) - 100));
    o.budget = 4ull << 20; // small frames: the tables take an eighth at most
    s = plan_subscription({test::opaque_description(64), limits(), 1}, true, o, 256);
    CHECK((s.charge <= o.budget && s.payload_bytes > 7 * (o.budget / 8) - (128 << 10)));
    o.budget = 27ull << 20;
    CHECK_THROWS_WITH(plan_subscription({d, limits(), 1}, true, o, 256),
                      Catch::Matchers::ContainsSubstring("needs three frames of 9437184 bytes"));
    d.shape = {4};
    CHECK_THROWS_WITH(sizes_of(d), Catch::Matchers::ContainsSubstring("has no shape"));
    CHECK_THROWS_WITH(sizes_of(test::opaque_description((1ull << 31) + 1)),
                      Catch::Matchers::ContainsSubstring("over 2 GiB"));
}
