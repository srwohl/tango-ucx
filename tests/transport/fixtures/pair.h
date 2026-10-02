// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-ucx project
// SPDX-License-Identifier: LGPL-3.0-or-later
#pragma once

#include <tango-ucx/ucx.h>
#include <transport.h>
#include <wire.h>

#include <nlohmann/json.hpp>

#include <algorithm>
#include <chrono>
#include <cstring>
#include <stdexcept>
#include <thread>

namespace TangoUcx::test
{
inline auto deadline(std::chrono::seconds n = std::chrono::seconds(5))
{
    return std::chrono::steady_clock::now() + n;
}
// Health and UcxSessions are sampled every 256 loop turns: poll until `done` or the deadline.
template <class Done> bool eventually(Done done, std::chrono::seconds s = std::chrono::seconds(10))
{
    for(auto until = deadline(s); !done();
        std::this_thread::sleep_for(std::chrono::milliseconds(5)))
        if(std::chrono::steady_clock::now() > until)
            return done();
    return true;
}
// The sessions the UcxSessions document lists.
inline nlohmann::json sessions(const Publisher &publisher)
{
    return nlohmann::json::parse(transport::ucx_sessions(publisher)).at("sessions");
}
// A numeric field of the first session, such as "room"; ~0 when there is no session.
inline std::uint64_t first_row(const Publisher &publisher, const char *key)
{
    auto s = sessions(publisher);
    return s.empty() ? ~0ull : s[0].at(key).get<std::uint64_t>();
}
inline Description small_description()
{
    return {Element::U8, {128}, 8, "u1"};
}
inline PublisherLimits small_limits(std::uint32_t sessions = 2)
{
    PublisherLimits l;
    l.budget = 8ull << 20;
    l.max_sessions = sessions;
    l.allow_every = true;
    return l;
}
inline SubscriptionOptions small_options()
{
    SubscriptionOptions o;
    o.budget = 4ull << 20;
    o.batch = 1;
    o.max_wait = std::chrono::microseconds(100);
    return o;
}
// What a subscriber learns from the UcxStream attribute.
inline transport::Offer offer_of(const Publisher &publisher)
{
    return transport::parse_ucx_stream(transport::ucx_stream(publisher));
}
// The slots granted and not yet routed, once the publisher has granted any.
inline std::uint32_t granted(const Publisher &publisher)
{
    auto h = publisher.health();
    for(auto until = deadline(); h.slots_held == 0 && std::chrono::steady_clock::now() < until;
        h = publisher.health())
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    return h.slots_held;
}
inline transport::Closer closer_for(Publisher &publisher)
{
    return [&publisher](std::string_view doc, Deadline)
    {
        wire::Reply reply;
        return wire::parse(transport::handle(publisher, doc), reply) && reply.status == wire::Ok;
    };
}
inline Subscription subscribe(Publisher &publisher, bool every,
                              SubscriptionOptions options = small_options(),
                              transport::Closer closer = {})
{
    auto pending = transport::prepare(offer_of(publisher), every, std::move(options));
    auto open = pending.open_message();
    auto reply = transport::handle(publisher, open);
    if(!closer)
        closer = closer_for(publisher);
    return std::move(pending).start(reply, deadline(), std::move(closer));
}
// The test device starts when it first publishes: subscribe every before that.
inline void publish_one(Publisher &publisher, std::byte value = std::byte{0x5a},
                        std::uint64_t timestamp = 123)
{
    publisher.ucx_start();
    auto slot = publisher.acquire(deadline());
    if(!slot)
        throw std::runtime_error("publisher did not grant a slot");
    auto payload = slot->payload();
    std::memset(payload.data(), std::to_integer<int>(value), payload.size());
    auto fields = slot->fields();
    std::fill(fields.begin(), fields.end(), std::byte{0x21});
    std::move(*slot).publish(timestamp);
}
inline Description opaque_description(std::uint64_t max_payload_bytes = 64 << 10)
{
    Description d{Element::Bytes, {}, 8, "<u8"};
    d.max_payload_bytes = max_payload_bytes;
    return d;
}
// Byte i of the opaque frame a seed makes; no two seeds or lengths give the same frame.
inline std::byte opaque_byte(std::uint64_t seed, std::size_t i)
{
    return std::byte(((seed + 1) * 0x9e3779b97f4a7c15ull + i * 0x100000001b3ull) >> 29);
}
// An opaque frame of `bytes` bytes made from `seed`, which is also its field and its timestamp.
// False when the publisher granted no slot by the deadline.
inline bool publish_opaque(Publisher &publisher, std::size_t bytes, std::uint64_t seed,
                           Deadline by = deadline())
{
    publisher.ucx_start();
    auto slot = publisher.acquire(by);
    if(!slot)
        return false;
    auto payload = slot->payload();
    for(std::size_t i = 0; i < bytes && i < payload.size(); ++i)
        payload[i] = opaque_byte(seed, i);
    std::memcpy(slot->fields().data(), &seed, sizeof(seed));
    std::move(*slot).publish_bytes(bytes, seed);
    return true;
}
// Frame `i` of the batch is the opaque frame of `bytes` bytes that `seed` makes, with its record.
inline bool is_opaque_frame(const Batch &batch, std::size_t i, std::size_t bytes,
                            std::uint64_t seed)
{
    auto frame = batch.payload(i);
    auto *record = static_cast<const std::byte *>(batch.records()) + i * 24;
    bool same = frame.size() == bytes && wire::get64(record + 8) == seed &&
                wire::get64(record + 16) == seed;
    for(std::size_t b = 0; same && b < bytes; ++b)
        same = frame[b] == opaque_byte(seed, b);
    return same;
}
} // namespace TangoUcx::test
