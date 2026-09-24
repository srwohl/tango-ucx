// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
// SPDX-License-Identifier: LGPL-3.0-or-later
#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>
#include <memory.h>
#include <transport.h>

using namespace TangoBulk;
using namespace TangoBulk::transport;

TEST_CASE("host plans account for fixed slots and reject invalid bounds")
{
    Description d{Element::U16, {64}, 8, "u1"};
    PublisherLimits limits;
    limits.budget = 8ull << 20;
    limits.max_sessions = 2;
    limits.allow_every = true;
    auto p = plan_publisher(d, limits, 256, 4096);
    CHECK(p.S >= p.M + 2);
    CHECK(p.charge <= limits.budget);
    CHECK(p.sizes.frame_bytes == 128);
    CHECK(p.sizes.record_stride == 24);

    Offer o{d, limits, 17};
    SubscriptionOptions opts;
    opts.budget = 4ull << 20;
    opts.batch = 2;
    auto s = plan_subscription(o, true, opts, 256);
    CHECK(s.D >= 4);
    CHECK(s.D % 2 == 0);
    CHECK(s.charge <= opts.budget);
    CHECK(s.payload_bytes == std::uint64_t(s.D) * 128);

    opts.gpu = 0;
    CHECK_THROWS_AS(plan_subscription(o, true, opts, 256), std::invalid_argument);
    opts.gpu = -1;
    o.limits.allow_every = false;
    CHECK_THROWS_AS(plan_subscription(o, true, opts, 256), std::invalid_argument);
}

TEST_CASE("host configurations are capped, or refused before anything is charged")
{
    using Catch::Matchers::ContainsSubstring;
    Description tiny{Element::U8, {1}}, big{Element::U8, {1ull << 20}};
    PublisherLimits wide;
    wide.max_sessions = 64;
    SubscriptionOptions deep;
    deep.budget = 1ull << 30;
    auto p = plan_publisher(tiny, wide, 1024, 4096);
    CHECK(p.S == 1024);
    CHECK(p.charge <= wide.budget);
    auto s = plan_subscription(Offer{tiny, wide, 1}, false, deep, 1024);
    CHECK(s.D == 65536);
    CHECK(s.charge <= deep.budget);

    // UCX reports the request size and the largest header, so only the plans can be given these.
    CHECK_THROWS_WITH(plan_publisher(tiny, wide, 1025, 4096),
                      ContainsSubstring("larger than 1 KiB"));
    CHECK_THROWS_WITH(plan_subscription(Offer{tiny, wide, 1}, false, deep, 1025),
                      ContainsSubstring("larger than 1 KiB"));
    CHECK_THROWS_WITH(plan_publisher({Element::U8, {1}, 1024}, wide, 1024, 1063),
                      ContainsSubstring("header does not fit"));

    struct Row
    {
        const char *name;
        Description d;
        std::uint64_t budget;
        std::uint32_t sessions, batch;
        const char *publisher, *subscription; // the refusal each side names; null leaves it out
    };
    const std::uint64_t gib = 1ull << 30;
    const Row rows[] = {
        {"shape product overflows", {Element::U8, {~0ull, 2}}, gib, 1, 1, "overflows", "overflows"},
        {"2 GiB + 1", {Element::U8, {(1ull << 31) + 1}}, gib, 1, 1, "over 2 GiB", "over 2 GiB"},
        {"1,025 field bytes", {Element::U8, {1}, 1025}, gib, 1, 1, "over 1,024", "over 1,024"},
        {"budget below the fixed part", tiny, 1024, 1, 1, "holds 0 source slots", "holds 0 frames"},
        {"D below 2 batches", big, 6ull << 20, 1, 4, nullptr, "two batches of 4"},
        {"S < M + 2", big, 32ull << 20, 64, 1, "64 sessions need 66", nullptr},
        {"M = 65", tiny, gib, 65, 1, "must be 1 to 64", nullptr},
    };
    for(const auto &row : rows)
    {
        INFO(row.name);
        PublisherLimits limits;
        limits.budget = row.budget;
        limits.max_sessions = row.sessions;
        limits.allow_every = true;
        SubscriptionOptions options;
        options.budget = row.budget;
        options.batch = row.batch;
        Offer offer{row.d, limits, 1};
        auto pinned = pinned_bytes();
        if(row.publisher)
        {
            CHECK_THROWS_WITH(plan_publisher(row.d, limits, 1024, 4096),
                              ContainsSubstring(row.publisher));
            CHECK_THROWS(Publisher(row.d, limits));
        }
        if(row.subscription)
        {
            CHECK_THROWS_WITH(plan_subscription(offer, true, options, 1024),
                              ContainsSubstring(row.subscription));
            CHECK_THROWS(transport::prepare(offer, true, options));
        }
        CHECK(pinned_bytes() == pinned);
    }

    // The refusal names what two batches need: exactly that budget plans them, a byte less does
    // not.
    SubscriptionOptions two;
    two.budget = 6ull << 20;
    two.batch = 4;
    Offer offer{big, {}, 1};
    std::string why;
    try
    {
        plan_subscription(offer, false, two, 1024);
    }
    catch(const std::invalid_argument &e)
    {
        why = e.what();
    }
    REQUIRE(why.find("two batches of 4 need ") != std::string::npos);
    two.budget = std::stoull(why.substr(why.find(" need ") + 6));
    auto at = plan_subscription(offer, false, two, 1024);
    CHECK(at.D == 8);
    CHECK(at.charge == two.budget);
    --two.budget;
    CHECK_THROWS_AS(plan_subscription(offer, false, two, 1024), std::invalid_argument);
}
