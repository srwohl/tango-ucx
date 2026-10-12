// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-ucx project
// SPDX-License-Identifier: LGPL-3.0-or-later
#include "tango/device_server.h"
#include <catch2/catch_test_macros.hpp>
#include <tango-ucx/tango.h>
#include <transport.h>
#include <wire.h>

#include <nlohmann/json.hpp>
#include <tango/tango.h>

#include <atomic>
#include <thread>

using namespace TangoUcx;
using namespace std::chrono_literals;

namespace
{
Deadline in(std::chrono::seconds s = 10s)
{
    return std::chrono::steady_clock::now() + s;
}
Tango::DeviceProxy device()
{
    return Tango::DeviceProxy(test::DeviceServer::instance().device());
}
Tango::DevLong publish(Tango::DeviceProxy &p, Tango::DevLong n)
{
    Tango::DeviceData d;
    d << n;
    Tango::DevLong done = 0;
    p.command_inout("Publish", d) >> done;
    return done;
}
void command(Tango::DeviceProxy &p, const char *name, Tango::DevLong n)
{
    Tango::DeviceData d;
    d << n;
    p.command_inout(name, d);
}
template <class T> T attribute(Tango::DeviceProxy &p, const char *name)
{
    T v{};
    auto a = p.read_attribute(name);
    a.reset_exceptions(Tango::DeviceAttribute::isempty_flag);
    a >> v;
    return v;
}
std::uint64_t instance_id(Tango::DeviceProxy &p)
{
    return transport::parse_ucx_stream(attribute<std::string>(p, "UcxStream")).instance_id;
}
// UcxSessions is sampled every 256 loop turns: read it until `done` accepts it, or the deadline.
template <class Done> bool sessions_until(Tango::DeviceProxy &p, Done done, Deadline until)
{
    for(;; std::this_thread::sleep_for(10ms))
        if(done(nlohmann::json::parse(attribute<std::string>(p, "UcxSessions"))))
            return true;
        else if(std::chrono::steady_clock::now() > until)
            return false;
}
// What a sequencer checks before it starts a capture: each label names a ready every session of
// this publisher instance.
bool ready(const nlohmann::json &doc, std::uint64_t instance_id, std::vector<std::string> labels)
{
    for(const auto &s : doc.at("sessions"))
        if(s.at("delivery") == "every" && s.at("state") == "ready")
            std::erase(labels, s.at("label").get<std::string>());
    return doc.at("instance_id") == instance_id && labels.empty();
}
// An archiver: joins after `delay`, then reads until its outcome, `pause` after each batch.
struct Archiver
{
    std::vector<std::uint64_t> indices;
    std::optional<Outcome> outcome;
    std::string failure;
    std::jthread thread;
    Archiver(std::string label, std::chrono::milliseconds delay, std::chrono::milliseconds pause,
             std::uint64_t budget = 1 << 20)
        : thread(
              [=, this]
              {
                  std::this_thread::sleep_for(delay);
                  try
                  {
                      auto proxy = device();
                      auto sub = every(proxy, {.budget = budget, .batch = 4, .label = label});
                      for(auto until = in(30s);
                          !(outcome = sub.outcome()) && std::chrono::steady_clock::now() < until;)
                          if(auto batch = sub.read(in(1s)))
                          {
                              for(std::size_t i = 0; i < batch->frames(); ++i)
                                  indices.push_back(batch->index(i));
                              std::this_thread::sleep_for(pause);
                          }
                      sub.close(in());
                  }
                  catch(...)
                  {
                      failure = "the archiver threw";
                  }
              })
    {
    }
};
std::string status(std::string_view reply)
{
    wire::Reply r;
    return wire::parse(reply, r) ? std::string(wire::status_name(r.status)) : "unparsed";
}
} // namespace

TEST_CASE("a subscriber finds renamed UCX commands by their markers")
{
    auto proxy = device();
    std::unique_ptr<Tango::CommandInfoList> commands(proxy.command_list_query());
    std::map<std::string, Tango::CommandInfo> by_name;
    for(const auto &c : *commands)
        by_name[c.cmd_name] = c;
    CHECK_FALSE(by_name.contains("UcxOpen"));
    REQUIRE(by_name.contains("StreamOpen"));
    REQUIRE(by_name.contains("StreamClose"));
    for(auto [name, marker] : {std::pair{"StreamOpen", wire::k_open_marker},
                               std::pair{"StreamClose", wire::k_close_marker}})
    {
        CHECK(by_name[name].in_type == Tango::DEV_STRING);
        CHECK(by_name[name].out_type == Tango::DEV_STRING);
        CHECK(by_name[name].in_type_desc == marker);
        CHECK(transport::marker_major(by_name[name].in_type_desc) == wire::k_major);
    }
    auto sub = every(proxy);
    CHECK(sub.description().shape == std::vector<std::uint64_t>{4096});
    sub.close(in());
}

TEST_CASE("every and latest subscriptions receive a device server's frames")
{
    auto proxy = device();
    auto all = every(proxy, {.batch = 4, .max_wait = 20ms});
    auto newest = latest(proxy);
    REQUIRE(publish(proxy, 12) == 12);
    std::uint64_t next = 0;
    while(next < 12)
    {
        auto batch = all.read(in());
        REQUIRE(batch);
        for(std::size_t i = 0; i < batch->frames(); ++i, ++next)
        {
            CHECK(batch->index(i) == next);
            auto *p = static_cast<const std::uint8_t *>(batch->payload()) + i * 4096;
            CHECK((p[0] == next && p[4095] == next));
        }
    }
    auto shown = newest.read(in());
    REQUIRE(shown);
    CHECK(shown->index(0) < 12);
    proxy.command_inout("Finish");
    CHECK_FALSE(all.read(in()));
    CHECK(all.outcome() == Outcome::End);
    all.close(in());
    shown.reset();
    newest.close(in());
}

TEST_CASE("the pullers of a device server's set share its frames, a range at a time")
{
    auto proxy = device();
    Subscription pullers[] = {pull(proxy, 8, {.batch = 4, .label = "a"}),
                              pull(proxy, 8, {.batch = 4, .label = "b"})};
    CHECK_THROWS(pull(proxy, 6, {.batch = 4})); // not whole batches: the status is `range`
    CHECK(sessions_until(
        proxy, [](auto d) { return d["sessions"][1]["delivery"] == "pull" && d["missing"] == 0; },
        in()));
    REQUIRE(publish(proxy, 20) == 20);
    proxy.command_inout("Finish");
    std::map<std::uint64_t, int> received;
    for(std::size_t p = 0; p < 2; ++p)
    {
        while(auto batch = pullers[p].read(in()))
            for(std::size_t i = 0; i < batch->frames(); ++i)
            {
                CHECK(batch->index(i) / 8 % 2 == p);
                ++received[batch->index(i)];
            }
        CHECK(pullers[p].outcome() == Outcome::End);
        pullers[p].close(in());
    }
    CHECK(received.size() == 20);
    for(auto [index, times] : received)
        CHECK(times == 1);
}

TEST_CASE("UcxClose answers once the session row is free")
{
    auto proxy = device(); // two session rows
    auto first = every(proxy);
    auto second = latest(proxy);
    for(int i = 0; i < 5; ++i)
    {
        first.close(in());
        first = every(proxy); // refused with no-session if the row were still retiring
    }
    CHECK(publish(proxy, 1) == 1);
    CHECK(first.read(in()));
    first.close(in());
    second.close(in());
}

TEST_CASE("the UCX attributes describe the stream and its sessions")
{
    auto proxy = device();
    auto offer = transport::parse_ucx_stream(attribute<std::string>(proxy, "UcxStream"));
    CHECK(offer.description.element == Element::U8);
    CHECK(offer.description.shape == std::vector<std::uint64_t>{4096});
    CHECK(offer.description.field_bytes == 8);
    CHECK(offer.description.field_dtype == "<u8");
    CHECK(offer.limits.allow_every);
    CHECK(offer.limits.max_sessions == 2);
    for(auto name : {"UcxStream", "UcxSessions", "UcxTopology", "UcxPublished", "UcxSkipped",
                     "UcxPressure", "UcxFailedEvery"})
        CHECK_FALSE(proxy.attribute_query(name).description.empty());

    auto sub = every(proxy, {.label = "archiver"});
    REQUIRE(publish(proxy, 3) == 3);
    nlohmann::json doc;
    CHECK(sessions_until(
        proxy, [&](auto d)
        { return (doc = d)["sessions"].size() == 1 && doc["sessions"][0]["sent"] == 3; }, in()));
    INFO(doc.dump());
    CHECK(doc["interface"] == "tango-ucx-sessions/3.2");
    CHECK(doc["instance_id"] == offer.instance_id);
    auto row = doc["sessions"][0];
    CHECK((row["ordinal"] == 1 && row["label"] == "archiver" && row["delivery"] == "every" &&
           row["state"] == "ready" && row["skipped"] == 0));
    CHECK(attribute<Tango::DevULong64>(proxy, "UcxPublished") == 3);
    CHECK(attribute<Tango::DevULong64>(proxy, "UcxSkipped") == 0);
    CHECK(attribute<Tango::DevULong>(proxy, "UcxPressure") == 0);
    CHECK(attribute<Tango::DevULong>(proxy, "UcxFailedEvery") == 0);
    sub.close(in());
}

TEST_CASE("detach_publisher waits for the UCX commands inside the publisher")
{
    auto proxy = device();
    auto before = instance_id(proxy);
    std::atomic<bool> stop{false};
    std::atomic<int> replies{0}, unavailable{0}, other{0};
    auto hammer = [&]
    {
        auto own = device();
        auto doc = wire::format(wire::CloseMessage{12345});
        while(!stop)
            try
            {
                Tango::DeviceData d;
                d << doc;
                std::string reply;
                own.command_inout("StreamClose", d) >> reply;
                auto result = status(reply);
                if(result == "unknown-session")
                    ++replies;
                else if(result == "finished")
                    ++unavailable;
                else
                    ++other;
            }
            catch(const Tango::DevFailed &e)
            {
                if(std::string(e.errors[0].reason) == "UcxUnavailable")
                    ++unavailable;
                else
                    ++other;
            }
    };
    std::jthread a(hammer), b(hammer);
    for(int i = 0; i < 20; ++i)
        command(proxy, "Prepare", 2);
    stop = true;
    a.join();
    b.join();
    CHECK(replies > 0);
    CHECK(other == 0);
    INFO("replies " << replies << ", unavailable " << unavailable);
    CHECK(instance_id(proxy) != before);
    auto sub = every(proxy);
    REQUIRE(publish(proxy, 1) == 1);
    CHECK(sub.read(in()));
    sub.close(in());
}

TEST_CASE("a sequencer starts a capture once its archivers are ready, and each gets every frame")
{
    auto proxy = device();
    proxy.set_timeout_millis(10'000); // Prepare waits for the preview to close its session
    std::atomic<int> previews{0};
    std::optional<Outcome> ended; // the first preview's outcome
    std::jthread preview(         // a latest subscription that subscribes again whenever it ends
        [&](std::stop_token stop)
        {
            auto own = device();
            while(!stop.stop_requested())
                try
                {
                    auto sub = latest(own, {.budget = 1 << 20});
                    ++previews;
                    while(!stop.stop_requested() && !sub.outcome())
                        sub.read(in(1s));
                    if(!ended)
                        ended = sub.outcome();
                }
                catch(...) // the finished publisher refuses Opens until Prepare replaces it
                {
                    std::this_thread::sleep_for(50ms);
                }
        });
    REQUIRE(sessions_until(proxy, [](auto d) { return d["sessions"].size() == 1; }, in()));
    command(proxy, "Prepare", 4);
    auto now = instance_id(proxy);
    Archiver fast("fast", 0ms, 0ms), steady("steady", 50ms, 1ms),
        slow("slow", 150ms, 20ms, 192 << 10); // its ring is shorter than the capture
    REQUIRE(sessions_until(
        proxy, [&](auto d) { return ready(d, now, {"fast", "steady", "slow"}); }, in()));
    constexpr std::uint64_t n = 40;
    command(proxy, "Start", n);
    for(auto *a : {&fast, &steady, &slow})
    {
        a->thread.join();
        INFO(a->failure);
        CHECK(a->indices.size() == n);
        for(std::uint64_t i = 0; i < a->indices.size(); ++i)
            CHECK(a->indices[i] == i);
        CHECK(a->outcome == Outcome::End);
    }
    for(auto until = in(); previews < 2 && std::chrono::steady_clock::now() < until;)
        std::this_thread::sleep_for(10ms);
    CHECK(previews >= 2);
    CHECK(ended == Outcome::End);
}

TEST_CASE("a sequencer whose archiver never joins does not start the capture")
{
    auto proxy = device();
    command(proxy, "Prepare", 4);
    auto now = instance_id(proxy);
    Archiver fast("fast", 0ms, 0ms), steady("steady", 50ms, 0ms); // "slow" never joins
    CHECK(sessions_until(proxy, [&](auto d) { return ready(d, now, {"fast", "steady"}); }, in()));
    CHECK_FALSE(sessions_until(
        proxy, [&](auto d) { return ready(d, now, {"fast", "steady", "slow"}); }, in(2s)));
    CHECK(attribute<Tango::DevULong64>(proxy, "UcxPublished") == 0);
    proxy.command_inout("Finish");
    for(auto *a : {&fast, &steady})
    {
        a->thread.join();
        CHECK(a->indices.empty());
        CHECK(a->outcome == Outcome::End);
    }
}
