// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
// SPDX-License-Identifier: LGPL-3.0-or-later
#include "fixtures/pair.h"
#include <catch2/catch_test_macros.hpp>
#include <tables.h>

using namespace TangoBulk;

TEST_CASE("a held batch keeps its host storage alive after close")
{
    Publisher publisher(test::small_description(), test::small_limits());
    auto sub = test::subscribe(publisher, true);
    test::cancel_earlier_grants(publisher);
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
    using namespace TangoBulk::transport;
    Description d = test::small_description();
    PublisherLimits limits = test::small_limits();
    Offer offer{d, limits, 1};
    auto sub = std::make_unique<Sub>(offer, true, test::small_options());
    sub->plan.sizes = sizes_of(d);
    sub->plan.D = 2;
    sub->plan.batch = 1;
    sub->rings = std::make_shared<Rings>();
    sub->rings->plan = sub->plan;
    sub->rings->description = d;
    sub->rings->payload = new std::byte[256];
    sub->rings->storage = std::shared_ptr<void>(sub->rings->payload, [](void *p)
                                                { delete[] static_cast<std::byte *>(p); });
    sub->rings->records.resize(2 * sub->plan.sizes.record_stride);
    sub->rings->batches.resize(2);
    sub->rings->released.reserve(2);
    sub->rings->busy.assign(1, 0);
    sub->rings->landed.assign(1, 0);
    sub->rings->returned.assign(1, 0);
    sub->queue.reserve(2);
    sub->has_end = true;
    sub->end = {1, 1, 1, TangoBulk::wire::FinishedReason};
    sub->end_at_ns = 1'000'000'000LL;
    sub->decide(2'000'000'000LL);
    CHECK(sub->outcome.load() == 0);
    wire::put64(sub->rings->record(0), 0);
    sub->landed(0);
    REQUIRE(sub->assemble(2'000'000'001LL));
    sub->decide(2'000'000'002LL);
    CHECK(sub->outcome.load() == int(Outcome::End));
}
