// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-ucx project
// SPDX-License-Identifier: LGPL-3.0-or-later
#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>
#include <cuda.h>
#include <cuda_runtime.h>
#include <memory.h>
#include <tables.h>
#include <transport.h>

#include <chrono>
#include <cstdint>
#include <numeric>
#include <stdexcept>
#include <thread>
#include <vector>

using namespace TangoUcx;
using namespace TangoUcx::transport;
using Catch::Matchers::ContainsSubstring;

namespace
{
constexpr std::size_t k_bytes = 1 << 20;
GpuRuntime &runtime()
{
    REQUIRE(gpu_runtime() != nullptr); // linking tango-ucx-cuda registered it
    return *gpu_runtime();
}
GpuEventState settled(GpuRuntime &rt, GpuEvent e)
{
    auto until = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    GpuEventState s;
    while((s = rt.query(e)) == GpuEventState::Pending && std::chrono::steady_clock::now() < until)
        std::this_thread::yield();
    return s;
}
} // namespace

TEST_CASE("an allocation is GPU memory of the GPU asked for and copies back to the host")
{
    auto &rt = runtime();
    Allocation a = rt.allocate(0, k_bytes);
    REQUIRE(a.base != nullptr);
    CHECK(a.bytes == k_bytes);
    CHECK(a.gpu == 0);
    cudaPointerAttributes where{};
    REQUIRE(cudaPointerGetAttributes(&where, a.base) == cudaSuccess);
    CHECK(where.type == cudaMemoryTypeDevice);
    CHECK(where.device == 0);

    std::vector<std::uint8_t> sent(k_bytes), got(k_bytes);
    std::iota(sent.begin(), sent.end(), std::uint8_t{1});
    REQUIRE(cudaMemcpy(a.base, sent.data(), k_bytes, cudaMemcpyHostToDevice) == cudaSuccess);
    // A batch is copied on an application thread, not on the thread that allocated the ring.
    std::thread([&] { rt.copy_to_host(got.data(), a.base, k_bytes); }).join();
    CHECK(got == sent);
    rt.copy_to_host(got.data(), static_cast<std::byte *>(a.base) + 4096, 4096);
    CHECK(std::equal(got.begin(), got.begin() + 4096, sent.begin() + 4096));

    std::size_t before = 0, after = 0, total = 0;
    REQUIRE(cudaMemGetInfo(&before, &total) == cudaSuccess);
    rt.release(a);
    REQUIRE(cudaMemGetInfo(&after, &total) == cudaSuccess);
    CHECK(after >= before);
    rt.release({}); // an allocation that was never made
}

TEST_CASE("an event recorded on a stream reaches Done after the stream's work")
{
    auto &rt = runtime();
    Allocation a = rt.allocate(0, k_bytes);
    cudaStream_t stream = nullptr;
    REQUIRE(cudaStreamCreate(&stream) == cudaSuccess);
    GpuEvent e = rt.make_event(0);
    REQUIRE(e.handle != nullptr);

    REQUIRE(cudaMemsetAsync(a.base, 0x5a, k_bytes, stream) == cudaSuccess);
    // The last hold ends on any thread; the loop thread queries.
    std::thread([&] { rt.record(e, GpuStream{stream}); }).join();
    CHECK(settled(rt, e) == GpuEventState::Done);
    std::vector<std::uint8_t> got(k_bytes);
    rt.copy_to_host(got.data(), a.base, k_bytes);
    CHECK(got == std::vector<std::uint8_t>(k_bytes, 0x5a));

    rt.record(e, GpuStream{}); // the default stream; an event is recorded again for a later batch
    CHECK(settled(rt, e) == GpuEventState::Done);

    rt.destroy_event(e);
    rt.destroy_event({});
    REQUIRE(cudaStreamDestroy(stream) == cudaSuccess);
    rt.release(a);
}

TEST_CASE("a failed record does not throw and shows as Failed at query")
{
    auto &rt = runtime();
    GpuEvent e = rt.make_event(0);
    // A live stream of another context on the same GPU: CUDA refuses it for this event.
    CUdevice gpu = 0;
    CUcontext other = nullptr;
    CUstream stream = nullptr;
    REQUIRE(cuDeviceGet(&gpu, 0) == CUDA_SUCCESS);
    REQUIRE(cuCtxCreate(&other, 0, gpu) == CUDA_SUCCESS);
    REQUIRE(cuStreamCreate(&stream, 0) == CUDA_SUCCESS);
    REQUIRE(cuCtxPopCurrent(nullptr) == CUDA_SUCCESS);
    REQUIRE_NOTHROW(rt.record(e, GpuStream{stream}));
    CHECK(rt.query(e) == GpuEventState::Failed);
    CHECK(rt.query(e) == GpuEventState::Failed);
    rt.destroy_event(e);
    REQUIRE(cuCtxDestroy(other) == CUDA_SUCCESS);
}

TEST_CASE("a GPU that does not exist and memory that is not there are refused by name")
{
    auto &rt = runtime();
    int gpus = 0, current = -1;
    REQUIRE(cudaGetDeviceCount(&gpus) == cudaSuccess);
    CHECK_THROWS_WITH(rt.allocate(gpus, k_bytes), ContainsSubstring("cudaSetDevice") &&
                                                      ContainsSubstring(std::to_string(gpus)));
    CHECK_THROWS_AS(rt.allocate(-1, k_bytes), std::runtime_error);
    CHECK_THROWS_AS(rt.make_event(gpus), std::runtime_error);
    CHECK_THROWS_WITH(rt.allocate(0, std::size_t{1} << 40), ContainsSubstring("cudaMalloc"));
    std::vector<std::byte> host(64), out(64);
    CHECK_THROWS_AS(rt.copy_to_host(out.data(), host.data(), 64), std::runtime_error);
    // A refusal leaves the thread's GPU selected and the runtime usable.
    REQUIRE(cudaGetDevice(&current) == cudaSuccess);
    CHECK(current == 0);
    rt.release(rt.allocate(0, k_bytes));
}

TEST_CASE("a source event rejected by CUDA fails publishing and quarantines its allocation")
{
    auto before = quarantine().total.load();
    CUdevice gpu = 0;
    CUcontext other = nullptr;
    CUstream stream = nullptr;
    (void)runtime();
    // Initialize the caller's primary context before creating an unrelated CUDA context.
    auto event = runtime().make_event(0);
    runtime().destroy_event(event);
    REQUIRE(cuDeviceGet(&gpu, 0) == CUDA_SUCCESS);
    REQUIRE(cuCtxCreate(&other, 0, gpu) == CUDA_SUCCESS);
    REQUIRE(cuStreamCreate(&stream, 0) == CUDA_SUCCESS);
    REQUIRE(cuCtxPopCurrent(nullptr) == CUDA_SUCCESS);
    {
        PublisherLimits limits;
        limits.budget = 8ull << 20;
        limits.max_sessions = 1;
        limits.allow_every = true;
        limits.gpu = 0;
        Publisher publisher({Element::U8, {128}}, limits);
        auto pending = prepare(parse_ucx_stream(ucx_stream(publisher)), true);
        auto deadline = [] { return Deadline::clock::now() + std::chrono::seconds(5); };
        auto reply = handle(publisher, pending.open_message());
        auto sub = std::move(pending).start(reply, deadline(),
                                            [&publisher](std::string_view doc, Deadline)
                                            {
                                                wire::Reply reply;
                                                return wire::parse(handle(publisher, doc), reply) &&
                                                       reply.status == wire::Ok;
                                            });
        publisher.ucx_start();
        auto slot = publisher.acquire(deadline());
        REQUIRE(slot);
        (void)slot->gpu_payload({stream});
        std::move(*slot).publish(); // cudaEventRecord fails for the unrelated context's stream
        CHECK_FALSE(sub.read(deadline()));
        CHECK(sub.outcome() == Outcome::Failed);
        for(auto until = deadline();
            publisher.health().failure.empty() && Deadline::clock::now() < until;)
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        CHECK_THAT(publisher.health().failure, ContainsSubstring("GPU source event failed"));
        CHECK_FALSE(publisher.acquire(deadline()));
    }
    CHECK(quarantine().total > before);
    REQUIRE(cuCtxDestroy(other) == CUDA_SUCCESS);
}
