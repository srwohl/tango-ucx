// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-ucx project
// SPDX-License-Identifier: LGPL-3.0-or-later
//
// What each delivery promises when every, latest and pull share one instance, over random
// scenarios. Only the contract is checked, through the public API, so the tests survive a
// change of how the publisher keeps its room (ADR 0002).
#include "fixtures/pair.h"
#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>
#include <catch2/generators/catch_generators_range.hpp>

#include <algorithm>
#include <deque>
#include <numeric>
#include <random>
#include <set>
#include <thread>

using namespace TangoUcx;

namespace
{
constexpr std::uint32_t k_batch = 4, k_range = 8;

struct Reader
{
    Subscription subscription;
    std::vector<std::uint64_t> indices;
    bool batch_spans_ranges = false;
    std::optional<Outcome> outcome;
    std::jthread thread;
    void start(bool puller)
    {
        thread = std::jthread(
            [this, puller]
            {
                for(auto until = test::deadline(std::chrono::seconds(20));
                    std::chrono::steady_clock::now() < until;)
                {
                    if(auto batch = subscription.read(test::deadline(std::chrono::seconds(1))))
                    {
                        for(std::size_t i = 0; i < batch->frames(); ++i)
                        {
                            indices.push_back(batch->index(i));
                            batch_spans_ranges |=
                                puller && batch->index(i) / k_range != batch->index(0) / k_range;
                        }
                    }
                    else if((outcome = subscription.outcome()))
                        return;
                }
            });
    }
};
Subscription open(Publisher &publisher, bool every, std::uint32_t range)
{
    auto options = test::small_options();
    if(range)
        options.batch = k_batch;
    auto pending = transport::prepare(test::offer_of(publisher), every, options, range);
    auto reply = transport::handle(publisher, pending.open_message());
    return std::move(pending).start(reply, test::deadline(), test::closer_for(publisher));
}
} // namespace

TEST_CASE("every, latest and pull keep their promises when they share an instance")
{
    auto seed = GENERATE(range(0, 12));
    std::mt19937 random(seed);
    auto pick = [&](int low, int high) { return std::uniform_int_distribution(low, high)(random); };
    int everies = pick(0, 2), latests = pick(0, 2), pullers = pick(0, 3);
    if(everies + latests + pullers == 0)
        everies = 1;
    int frames = pick(1, 64), cancels = pick(0, 6);
    INFO("seed " << seed << ": " << everies << " every, " << latests << " latest, " << pullers
                 << " pullers, " << frames << " frames, " << cancels << " cancelled slots");

    Publisher publisher(test::small_description(),
                        test::small_limits(std::uint32_t(everies + latests + pullers)));
    std::deque<Reader> every, latest, pull; // deque: a started reader never moves
    for(int i = 0; i < everies; ++i)
        every.emplace_back(open(publisher, true, 0)).start(false);
    for(int i = 0; i < latests; ++i)
        latest.emplace_back(open(publisher, false, 0)).start(false);
    for(int i = 0; i < pullers; ++i)
        pull.emplace_back(open(publisher, false, k_range)).start(true);

    // Cancelled slots fall at random among the published frames, and use no frame index.
    std::vector<char> plan(std::size_t(frames + cancels), 0);
    std::fill(plan.begin(), plan.begin() + cancels, 1);
    std::ranges::shuffle(plan, random);
    publisher.ucx_start();
    for(char cancel : plan)
    {
        auto slot = publisher.acquire(test::deadline());
        REQUIRE(slot);
        if(!cancel)
            std::move(*slot).publish();
    }
    publisher.finish();
    for(auto *readers : {&every, &latest, &pull})
        for(auto &reader : *readers)
            reader.thread.join();

    std::vector<std::uint64_t> all(static_cast<std::size_t>(frames), 0);
    std::iota(all.begin(), all.end(), 0);
    for(auto &reader : every)
    {
        CHECK(reader.indices == all); // every frame, in order, from 0
        CHECK(reader.outcome == Outcome::End);
    }
    for(auto &reader : latest)
    {
        CHECK(std::ranges::adjacent_find(reader.indices, std::greater_equal<>{}) ==
              reader.indices.end()); // newer frames only, never one twice
        CHECK(
            std::ranges::all_of(reader.indices, [&](auto i) { return i < std::uint64_t(frames); }));
        CHECK(reader.outcome == Outcome::End);
    }
    if(!pull.empty())
    {
        std::multiset<std::uint64_t> taken;
        for(auto &reader : pull)
        {
            taken.insert(reader.indices.begin(), reader.indices.end());
            CHECK_FALSE(reader.batch_spans_ranges);
            CHECK(reader.outcome == Outcome::End);
        }
        // Each frame went to exactly one puller, and together they had all of them.
        CHECK(std::vector<std::uint64_t>(taken.begin(), taken.end()) == all);
    }
}
