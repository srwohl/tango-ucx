// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
// SPDX-License-Identifier: LGPL-3.0-or-later
#pragma once

#include <tango-bulk/bulk.h>
#include <transport.h>
#include <wire.h>

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
// A numeric field of the first BulkSessions row, such as "room"; ~0 when there is no row.
inline std::uint64_t first_row(const Publisher &publisher, std::string_view key)
{
    auto rows = transport::session_rows(publisher);
    auto at = rows.empty() ? std::string::npos : rows[0].find("|" + std::string(key) + "=");
    return at == std::string::npos ? ~0ull : std::stoull(rows[0].substr(at + key.size() + 2));
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
// Slots granted and not yet published or cancelled. The publisher thread refreshes Health only
// every 256 turns, so wait for a copy that counts them.
inline std::uint32_t granted(const Publisher &publisher)
{
    auto h = publisher.health();
    for(auto until = deadline(); h.slots_held == 0 && std::chrono::steady_clock::now() < until;
        h = publisher.health())
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    return h.slots_held;
}
// Grants issued before an every row's Open reserve no room in it and never reach it. Before
// anything is published, every held slot is such a grant: cancel them all.
inline void cancel_earlier_grants(Publisher &publisher)
{
    for(auto n = granted(publisher); n; --n)
        if(!publisher.acquire(deadline()))
            throw std::runtime_error("publisher did not grant a slot");
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
