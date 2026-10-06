// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-ucx project
// SPDX-License-Identifier: LGPL-3.0-or-later
#include "fixtures/fault_adapter.h"
#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>
#include <cuda_runtime.h>
#include <tables.h>

#include <atomic>
#include <cstdlib>
#include <vector>

using namespace TangoUcx;
using namespace TangoUcx::transport;
using namespace std::chrono_literals;
using Catch::Matchers::ContainsSubstring;

namespace
{
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
    Allocation allocate_pinned(int gpu, std::size_t bytes) override
    {
        return real.allocate_pinned(gpu, bytes);
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
// A real CUDA stream dependency. Later writes, uploads and completion events wait for it.
void delay(void *)
{
    std::this_thread::sleep_for(200ms);
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
    CHECK_THROWS_AS(batch->payload(0), std::logic_error); // a span is host bytes; it has none
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
    CHECK(plan_subscription(offer, true, options, 256).ring_depth == 4096);
    REQUIRE(cudaGetDeviceCount(&options.gpu) == cudaSuccess);
    CHECK_THROWS_WITH(prepare(offer, true, options), ContainsSubstring("cudaSetDevice"));
}

TEST_CASE("a frame that arrives eager on a GPU subscription is refused")
{
    test::RawPeer peer; // plays a publisher that does not force rendezvous
    auto pending =
        prepare({test::small_description(), test::small_limits(), 7}, true, gpu_options());
    auto sub = std::move(pending).start(peer.accept(pending), test::deadline(), {});
    wire::FrameMessage f{peer.session, 0, 128, 0, 1};
    f.record_bytes = 8;
    std::array<std::byte, 48> header;
    wire::encode_into(header, f);
    peer.send(wire::Frame, header, filled(std::byte{0}));
    CHECK(peer.progress_until([&] { return sub.outcome() == Outcome::Failed; }));
    CHECK_THAT(sub.health().failure, ContainsSubstring("invalid frame"));
}

TEST_CASE("opaque GPU receive keeps packed frame offsets and lengths")
{
    auto delivery = GENERATE(0, 1, 2);
    Publisher publisher(test::opaque_description(), test::small_limits());
    auto pending =
        prepare(test::offer_of(publisher), delivery == 0, gpu_options(2), delivery == 2 ? 2 : 0);
    auto reply = handle(publisher, pending.open_message());
    auto sub = std::move(pending).start(reply, test::deadline(), test::closer_for(publisher));
    REQUIRE(test::publish_opaque(publisher, 1, 7));
    REQUIRE(test::eventually(
        [&] { return sub.health().slots_held == 1 && publisher.health().transfer_mean_ns > 0; }));
    REQUIRE(test::publish_opaque(publisher, 129, 8));
    auto batch = sub.read(test::deadline());
    REQUIRE(batch);
    auto copy = batch->copy();
    CHECK(test::is_opaque_frame(copy, 0, 1, 7));
    CHECK(test::is_opaque_frame(copy, 1, 129, 8));
    auto view = std::move(*batch).gpu_view({});
    CHECK(view.payload_bytes(1) == 129);
    auto *ptr = static_cast<const std::byte *>(view.payload(1));
    CHECK(ptr - static_cast<const std::byte *>(view.payload(0)) == 1);
    cudaPointerAttributes where{};
    REQUIRE(cudaPointerGetAttributes(&where, ptr) == cudaSuccess);
    CHECK(where.type == cudaMemoryTypeDevice);
    std::vector<std::byte> out(129);
    REQUIRE(cudaMemcpy(out.data(), ptr, out.size(), cudaMemcpyDeviceToHost) == cudaSuccess);
    CHECK(std::memcmp(out.data(), copy.payload(1).data(), out.size()) == 0);
}

TEST_CASE("pinned receive uploads on the application's non-default GPU stream")
{
    Publisher publisher(test::opaque_description(), test::small_limits());
    auto o = test::small_options();
    o.pinned = 0;
    auto sub = test::subscribe(publisher, true, o);
    REQUIRE(test::publish_opaque(publisher, 129, 8));
    auto batch = sub.read(test::deadline());
    REQUIRE(batch);
    auto bytes = batch->payload(0);
    unsigned flags = 0;
    REQUIRE(cudaHostGetFlags(&flags, const_cast<std::byte *>(bytes.data())) == cudaSuccess);
    cudaStream_t stream = nullptr;
    REQUIRE(cudaStreamCreateWithFlags(&stream, cudaStreamNonBlocking) == cudaSuccess);
    Allocation output = gpu_runtime()->allocate(0, bytes.size());
    REQUIRE(cudaMemcpyAsync(output.base, bytes.data(), bytes.size(), cudaMemcpyHostToDevice,
                            stream) == cudaSuccess);
    batch->release_after({stream});
    batch.reset();
    REQUIRE(cudaStreamSynchronize(stream) == cudaSuccess);
    CHECK(test::eventually([&] { return sub.health().slots_held == 0; }));
    std::vector<std::byte> out(129);
    gpu_runtime()->copy_to_host(out.data(), output.base, out.size());
    for(std::size_t i = 0; i < out.size(); ++i)
        CHECK(out[i] == test::opaque_byte(8, i));
    gpu_runtime()->release(output);
    REQUIRE(cudaStreamDestroy(stream) == cudaSuccess);
}

TEST_CASE("GPU source writes arrive unchanged in both host and GPU receive rings")
{
    auto l = test::small_limits();
    l.gpu = 0;
    Publisher publisher(test::opaque_description(), l);
    auto host_sub = test::subscribe(publisher, true);
    auto gpu_sub = test::subscribe(publisher, true, gpu_options());
    cudaStream_t stream = nullptr;
    REQUIRE(cudaStreamCreateWithFlags(&stream, cudaStreamNonBlocking) == cudaSuccess);
    publisher.ucx_start();
    for(std::size_t bytes : {std::size_t(1), std::size_t(129), std::size_t(65536)})
    {
        auto slot = publisher.acquire(test::deadline());
        REQUIRE(slot);
        REQUIRE(cudaMemsetAsync(slot->gpu_payload({stream}), 0x5a, bytes, stream) == cudaSuccess);
        std::move(*slot).publish_bytes(bytes);
        for(auto *sub : {&host_sub, &gpu_sub})
        {
            auto batch = sub->read(test::deadline());
            REQUIRE(batch);
            auto copy = batch->copy();
            CHECK(copy.payload_bytes(0) == bytes);
            CHECK(std::all_of(copy.payload(0).begin(), copy.payload(0).end(),
                              [](auto b) { return b == std::byte{0x5a}; }));
        }
    }
    REQUIRE(cudaStreamDestroy(stream) == cudaSuccess);
}

TEST_CASE("GPU publication and cancellation wait for outstanding source writes")
{
    auto cancel = GENERATE(false, true);
    auto l = test::small_limits();
    l.gpu = 0;
    Publisher publisher(test::small_description(), l);
    auto sub = test::subscribe(publisher, true);
    publisher.ucx_start();
    cudaStream_t stream = nullptr;
    REQUIRE(cudaStreamCreateWithFlags(&stream, cudaStreamNonBlocking) == cudaSuccess);
    auto first = publisher.acquire(test::deadline());
    REQUIRE(first);
    auto *ptr = first->gpu_payload({stream});
    REQUIRE(cudaLaunchHostFunc(stream, delay, nullptr) == cudaSuccess);
    REQUIRE(cudaMemsetAsync(ptr, 0x11, first->payload_bytes(), stream) == cudaSuccess);
    if(cancel)
        first.reset();
    else
        std::move(*first).publish();
    auto next = publisher.acquire(test::deadline());
    REQUIRE(next);
    REQUIRE(cudaMemsetAsync(next->gpu_payload({stream}), 0x22, next->payload_bytes(), stream) ==
            cudaSuccess);
    std::move(*next).publish();
    CHECK_FALSE(sub.read(Deadline::clock::now() + 30ms));
    auto batch = sub.read(test::deadline());
    REQUIRE(batch);
    CHECK(batch->index(0) == 0);
    CHECK(batch->payload(0)[0] == (cancel ? std::byte{0x22} : std::byte{0x11}));
    if(!cancel)
    {
        auto second = sub.read(test::deadline());
        REQUIRE(second);
        CHECK(second->index(0) == 1);
        CHECK(second->payload(0)[0] == std::byte{0x22});
    }
    REQUIRE(cudaStreamDestroy(stream) == cudaSuccess);
}

TEST_CASE("pinned receive storage survives close while an upload still reads it")
{
    Publisher publisher(test::opaque_description(), test::small_limits());
    auto o = test::small_options();
    o.pinned = 0;
    auto sub = test::subscribe(publisher, true, o);
    REQUIRE(test::publish_opaque(publisher, 129, 8));
    auto batch = sub.read(test::deadline());
    REQUIRE(batch);
    cudaStream_t stream = nullptr;
    REQUIRE(cudaStreamCreateWithFlags(&stream, cudaStreamNonBlocking) == cudaSuccess);
    auto output = gpu_runtime()->allocate(0, 129);
    REQUIRE(cudaLaunchHostFunc(stream, delay, nullptr) == cudaSuccess);
    REQUIRE(cudaMemcpyAsync(output.base, batch->payload(0).data(), 129, cudaMemcpyHostToDevice,
                            stream) == cudaSuccess);
    batch->release_after({stream});
    sub.close(test::deadline());
    sub = {};
    batch.reset(); // the last holder must keep pinned storage through the actual CUDA event
    REQUIRE(cudaStreamSynchronize(stream) == cudaSuccess);
    std::vector<std::byte> out(129);
    gpu_runtime()->copy_to_host(out.data(), output.base, out.size());
    for(std::size_t i = 0; i < out.size(); ++i)
        CHECK(out[i] == test::opaque_byte(8, i));
    gpu_runtime()->release(output);
    REQUIRE(cudaStreamDestroy(stream) == cudaSuccess);
}

TEST_CASE("opaque GPU batches stay correct across packed ring wrap and pressure")
{
    Publisher publisher(test::opaque_description(), test::small_limits());
    auto o = gpu_options(2);
    o.budget = 1ull << 20;
    auto sub = test::subscribe(publisher, true, o);
    constexpr std::uint64_t count = 100;
    std::atomic<bool> sent{true};
    std::jthread producer(
        [&]
        {
            for(std::uint64_t i = 0; i < count; ++i)
                if(!test::publish_opaque(publisher, i % 2 ? 65536 : 1, i))
                {
                    sent = false;
                    break;
                }
            publisher.finish();
        });
    std::uint64_t received = 0;
    bool shortened = false;
    while(auto batch = sub.read(test::deadline()))
    {
        shortened |= batch->frames() == 1;
        auto copy = batch->copy();
        for(std::size_t i = 0; i < copy.frames(); ++i, ++received)
        {
            CHECK(copy.index(i) == received);
            CHECK(test::is_opaque_frame(copy, i, received % 2 ? 65536 : 1, received));
        }
    }
    CHECK(sent);
    CHECK(received == count);
    CHECK(shortened);
    CHECK(sub.outcome() == Outcome::End);
}

TEST_CASE("an opaque GPU batch returns byte Credit after its last GPU read completes")
{
    Publisher publisher(test::opaque_description(), test::small_limits());
    auto sub = test::subscribe(publisher, true, gpu_options());
    REQUIRE(test::publish_opaque(publisher, 129, 8));
    auto batch = sub.read(test::deadline());
    REQUIRE(batch);
    auto output = gpu_runtime()->allocate(0, 129);
    cudaStream_t stream = nullptr;
    REQUIRE(cudaStreamCreateWithFlags(&stream, cudaStreamNonBlocking) == cudaSuccess);
    {
        auto view = std::move(*batch).gpu_view({stream});
        REQUIRE(cudaLaunchHostFunc(stream, delay, nullptr) == cudaSuccess);
        REQUIRE(cudaMemcpyAsync(output.base, view.payload(0), view.payload_bytes(0),
                                cudaMemcpyDeviceToDevice, stream) == cudaSuccess);
    }
    batch.reset();
    std::this_thread::sleep_for(20ms);
    CHECK(sub.health().slots_held == 1);
    REQUIRE(cudaStreamSynchronize(stream) == cudaSuccess);
    CHECK(test::eventually([&] { return sub.health().slots_held == 0; }));
    std::vector<std::byte> out(129);
    gpu_runtime()->copy_to_host(out.data(), output.base, out.size());
    for(std::size_t i = 0; i < out.size(); ++i)
        CHECK(out[i] == test::opaque_byte(8, i));
    gpu_runtime()->release(output);
    REQUIRE(cudaStreamDestroy(stream) == cudaSuccess);
}

TEST_CASE("a GPU source slot retains outstanding writes after its publisher closes")
{
    auto l = test::small_limits();
    l.gpu = 0;
    auto publisher = std::make_unique<Publisher>(test::small_description(), l);
    publisher->ucx_start();
    auto slot = publisher->acquire(test::deadline());
    REQUIRE(slot);
    cudaStream_t stream = nullptr;
    REQUIRE(cudaStreamCreateWithFlags(&stream, cudaStreamNonBlocking) == cudaSuccess);
    auto *ptr = slot->gpu_payload({stream});
    publisher.reset();
    auto output = gpu_runtime()->allocate(0, 128);
    REQUIRE(cudaLaunchHostFunc(stream, delay, nullptr) == cudaSuccess);
    REQUIRE(cudaMemsetAsync(ptr, 0x5a, 128, stream) == cudaSuccess);
    REQUIRE(cudaMemcpyAsync(output.base, ptr, 128, cudaMemcpyDeviceToDevice, stream) ==
            cudaSuccess);
    CHECK_THROWS_WITH(std::move(*slot).publish(), ContainsSubstring("publisher has closed"));
    slot.reset();
    REQUIRE(cudaStreamSynchronize(stream) == cudaSuccess);
    auto bytes = host(output.base);
    CHECK(bytes == filled(std::byte{0x5a}));
    gpu_runtime()->release(output);
    REQUIRE(cudaStreamDestroy(stream) == cudaSuccess);
}
