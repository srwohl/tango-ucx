// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
// SPDX-License-Identifier: LGPL-3.0-or-later
#include "fixtures/pair.h"
#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>
#include <cuda_runtime.h>
#include <tables.h>

#include <atomic>
#include <cstdlib>
#include <vector>

using namespace TangoBulk;
using namespace TangoBulk::transport;
using namespace std::chrono_literals;
using Catch::Matchers::ContainsSubstring;

namespace
{
// An eager frame cannot land in GPU memory, and the publisher does not yet force rendezvous for
// a GPU session: until it does, these tests ask UCX for rendezvous at every size.
[[maybe_unused]] const bool rendezvous = ::setenv("UCX_RNDV_THRESH", "0", 1) == 0;
// The CUDA runtime behind a switch: Done asks CUDA, Pending and Failed answer for it.
struct Switched final : GpuRuntime
{
    GpuRuntime &real = *gpu_runtime();
    std::atomic<GpuEventState> answer{GpuEventState::Done};
    std::atomic<int> records{0}, queries{0};
    Switched()
    {
        register_gpu_runtime(*this);
    }
    ~Switched()
    {
        register_gpu_runtime(real);
    }
    Allocation allocate(int gpu, std::size_t bytes) override
    {
        return real.allocate(gpu, bytes);
    }
    void release(Allocation a) override
    {
        real.release(a);
    }
    GpuEvent make_event(int gpu) override
    {
        return real.make_event(gpu);
    }
    void destroy_event(GpuEvent e) override
    {
        real.destroy_event(e);
    }
    void record(GpuEvent e, GpuStream s) override
    {
        ++records;
        real.record(e, s);
    }
    GpuEventState query(GpuEvent e) override
    {
        ++queries;
        return answer == GpuEventState::Done ? real.query(e) : answer.load();
    }
    void copy_to_host(void *to, const void *from, std::size_t bytes) override
    {
        real.copy_to_host(to, from, bytes);
    }
};
SubscriptionOptions gpu_options(std::uint32_t batch = 1)
{
    auto o = test::small_options();
    o.gpu = 0;
    o.batch = batch;
    o.max_wait = 1s;
    return o;
}
// The bytes of `frames` 128-byte frames read back from GPU memory.
std::vector<std::byte> host(const void *gpu, std::size_t frames = 1)
{
    std::vector<std::byte> out(frames * 128);
    gpu_runtime()->copy_to_host(out.data(), gpu, out.size());
    return out;
}
auto filled(std::byte value)
{
    return std::vector<std::byte>(128, value);
}
} // namespace

TEST_CASE("a GPU subscription receives frames into GPU memory, and a copy is host memory")
{
    Publisher publisher(test::small_description(), test::small_limits());
    auto sub = test::subscribe(publisher, true, gpu_options(2));
    test::publish_one(publisher, std::byte{0x11});
    test::publish_one(publisher, std::byte{0x22});
    auto batch = sub.read(test::deadline());
    REQUIRE(batch);
    REQUIRE(batch->frames() == 2);
    CHECK(batch->gpu());
    cudaPointerAttributes where{};
    REQUIRE(cudaPointerGetAttributes(&where, batch->payload()) == cudaSuccess);
    CHECK(where.type == cudaMemoryTypeDevice);
    auto bytes = host(batch->payload(), 2);
    CHECK(std::vector(bytes.begin(), bytes.begin() + 128) == filled(std::byte{0x11}));
    CHECK(std::vector(bytes.begin() + 128, bytes.end()) == filled(std::byte{0x22}));

    auto copy = batch->copy();
    CHECK_FALSE(copy.gpu());
    CHECK(std::memcmp(copy.payload(), bytes.data(), bytes.size()) == 0);
    CHECK(copy.index(1) == batch->index(0) + 1);
    CHECK_THROWS_WITH(std::move(copy).gpu_view({}), ContainsSubstring("needs a GPU batch"));
    auto transport = sub.health().transport;
    INFO(transport);
    CHECK(transport.find("tcp") != 0); // shared memory on one host
}

TEST_CASE("a viewed batch is released once its view has ended and its event is done")
{
    Switched runtime;
    Publisher publisher(test::small_description(), test::small_limits());
    auto sub = test::subscribe(publisher, true, gpu_options());
    cudaStream_t stream = nullptr;
    REQUIRE(cudaStreamCreate(&stream) == cudaSuccess);
    test::publish_one(publisher, std::byte{0x33});
    auto batch = sub.read(test::deadline());
    REQUIRE(batch);
    runtime.answer = GpuEventState::Pending;
    {
        GpuView view = std::move(*batch).gpu_view(GpuStream{stream});
        CHECK_THROWS_WITH(std::move(*batch).gpu_view({}), ContainsSubstring("one GPU view"));
        batch.reset(); // the view took the batch: this releases nothing
        CHECK(view.frames() == 1);
        CHECK(host(view.payload()) == filled(std::byte{0x33}));
        CHECK(wire::get64(static_cast<const std::byte *>(view.records()) + 8) == 123);
        std::this_thread::sleep_for(50ms);
        CHECK(runtime.records == 0);
        CHECK(runtime.queries == 0); // an event is not asked before it is recorded
        CHECK(sub.health().slots_held == 1);
    }
    CHECK(runtime.records == 1);
    CHECK(test::eventually([&] { return runtime.queries > 0; }));
    std::this_thread::sleep_for(20ms);
    CHECK(sub.health().slots_held == 1); // the event is pending: the slot is not credited
    runtime.answer = GpuEventState::Done;
    CHECK(test::eventually([&] { return sub.health().slots_held == 0; }));

    // The row and its event serve a later batch; a view outlives the subscription's close.
    test::publish_one(publisher, std::byte{0x44});
    batch = sub.read(test::deadline());
    REQUIRE(batch);
    {
        GpuView view = std::move(*batch).gpu_view({});
        sub.close(test::deadline());
        CHECK(host(view.payload()) == filled(std::byte{0x44}));
    }
    CHECK(runtime.records == 2);
    REQUIRE(cudaStreamDestroy(stream) == cudaSuccess);
}

TEST_CASE("a failed GPU event ends the subscription Failed and quarantines its rings")
{
    Switched runtime;
    Publisher publisher(test::small_description(), test::small_limits());
    auto sub = test::subscribe(publisher, true, gpu_options());
    test::publish_one(publisher);
    auto batch = sub.read(test::deadline());
    REQUIRE(batch);
    runtime.answer = GpuEventState::Failed;
    (void)std::move(*batch).gpu_view({});
    CHECK(test::eventually([&] { return sub.outcome() == Outcome::Failed; }));
    CHECK_THAT(sub.health().failure, ContainsSubstring("a GPU event failed"));
    CHECK(sub.health().slots_held == 1); // never credited
    auto before = quarantine().total.load();
    sub.close(test::deadline());
    CHECK(quarantine().total.load() > before);
}

TEST_CASE("a GPU subscription over TCP is refused with the transports named, unless allowed")
{
    REQUIRE(::setenv("UCX_TLS", "tcp,cuda_copy", 1) == 0);
    Publisher publisher(test::small_description(), test::small_limits());
    auto options = gpu_options();
    CHECK_THROWS_WITH(test::subscribe(publisher, true, options),
                      ContainsSubstring("tcp/") && ContainsSubstring("allow_gpu_over_tcp"));
    options.allow_gpu_over_tcp = true;
    auto sub = test::subscribe(publisher, true, options);
    ::unsetenv("UCX_TLS");
    test::publish_one(publisher, std::byte{0x55});
    auto batch = sub.read(test::deadline());
    REQUIRE(batch);
    CHECK(host(batch->payload()) == filled(std::byte{0x55}));
    CHECK(sub.health().transport.find("tcp") == 0);
}

TEST_CASE("a GPU ring is capped at 4,096 frames, and a GPU that does not exist is refused")
{
    Offer offer{{Element::U8, {1}}, test::small_limits(), 1};
    SubscriptionOptions options;
    options.gpu = 0;
    CHECK(plan_subscription(offer, true, options, 256).D == 4096);
    REQUIRE(cudaGetDeviceCount(&options.gpu) == cudaSuccess);
    CHECK_THROWS_WITH(prepare(offer, true, options), ContainsSubstring("cudaSetDevice"));
}
