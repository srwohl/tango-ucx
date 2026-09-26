// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
// SPDX-License-Identifier: LGPL-3.0-or-later
#pragma once

#include <tango-bulk/bulk.h>
#include <transport.h>
#include <wire.h>

#include <nlohmann/json.hpp>

#include <algorithm>
#include <chrono>
#include <cstring>
#include <stdexcept>
#include <thread>

namespace TangoBulk::test
{
inline auto deadline(std::chrono::seconds n = std::chrono::seconds(5))
{
    return std::chrono::steady_clock::now() + n;
}
// Health and BulkSessions are sampled every 256 loop turns: poll until `done` or the deadline.
template <class Done> bool eventually(Done done, std::chrono::seconds s = std::chrono::seconds(10))
{
    for(auto until = deadline(s); !done();
        std::this_thread::sleep_for(std::chrono::milliseconds(5)))
        if(std::chrono::steady_clock::now() > until)
            return done();
    return true;
}
// The sessions the BulkSessions document lists.
inline nlohmann::json sessions(const Publisher &publisher)
{
    return nlohmann::json::parse(transport::bulk_sessions(publisher)).at("sessions");
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
// What a subscriber learns from the BulkStream attribute.
inline transport::Offer offer_of(const Publisher &publisher)
{
    return transport::parse_bulk_stream(transport::bulk_stream(publisher));
}
// Slots granted before an every row's Open remain ahead of its first grant. Ordinary tests drain
// that prefix so their first publish exercises the row; grant-lookahead tests leave it in place.
inline std::uint32_t granted(const Publisher &publisher)
{
    auto h = publisher.health();
    for(auto until = deadline(); h.slots_held == 0 && std::chrono::steady_clock::now() < until;
        h = publisher.health())
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    return h.slots_held;
}
inline void cancel_earlier_grants(Publisher &publisher)
{
    for(auto n = granted(publisher); n; --n)
        if(!publisher.acquire(deadline()))
            throw std::runtime_error("publisher did not grant a slot");
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
                              transport::Closer closer = {}, bool drain_initial_grants = true)
{
    auto pending = transport::prepare(offer_of(publisher), every, std::move(options));
    auto open = pending.open_message();
    auto reply = transport::handle(publisher, open);
    if(!closer)
        closer = closer_for(publisher);
    auto sub = std::move(pending).start(reply, deadline(), std::move(closer));
    if(every && drain_initial_grants)
        cancel_earlier_grants(publisher);
    return sub;
}
inline void publish_one(Publisher &publisher, std::byte value = std::byte{0x5a},
                        std::uint64_t timestamp = 123)
{
    auto slot = publisher.acquire(deadline());
    if(!slot)
        throw std::runtime_error("publisher did not grant a slot");
    auto payload = slot->payload();
    std::memset(payload.data(), std::to_integer<int>(value), payload.size());
    auto fields = slot->fields();
    std::fill(fields.begin(), fields.end(), std::byte{0x21});
    std::move(*slot).publish(timestamp);
}
} // namespace TangoBulk::test
