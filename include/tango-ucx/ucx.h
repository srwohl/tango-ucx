// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-ucx project
// SPDX-License-Identifier: LGPL-3.0-or-later
#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace TangoUcx
{

using Deadline = std::chrono::steady_clock::time_point;
enum class Element : std::uint8_t
{
    U8,
    U16,
    U32,
    U64,
    I8,
    I16,
    I32,
    I64,
    F32,
    F64,
    Bytes
};
enum class Outcome : std::uint8_t
{
    End = 1,
    Failed,
    Cancelled
};

struct Description
{
    Element element = Element::U8;
    std::vector<std::uint64_t> shape;
    std::uint32_t field_bytes = 0;
    std::string field_dtype;
    std::uint64_t max_payload_bytes = 0; // nonzero, with an empty shape: an opaque stream
    std::string application_text; // the device's own, in UTF-8. Subscribers get it unchanged.
};
struct PublisherLimits
{
    std::uint64_t budget = 1ull << 30;
    std::uint32_t max_sessions = 8;
    bool allow_every = false;
    std::chrono::milliseconds pressure_timeout{0};
    int gpu = -1;          // source payloads on this GPU; records remain in host memory
    int progress_cpu = -1; // -1 inherits affinity; otherwise reserve this physical core in-process
    std::string net_devices; // UCX NET_DEVICES selector; empty inherits environment/defaults
};
struct SubscriptionOptions
{
    std::uint64_t budget = 256ull << 20;
    std::uint32_t batch = 1;
    std::chrono::microseconds max_wait{10'000};
    double max_rate = 0;
    int gpu = -1;
    bool allow_gpu_over_tcp = false;
    std::string label;
    int pinned = -1;       // pinned host receive for uploads on this GPU; exclusive with gpu
    int progress_cpu = -1; // -1 inherits affinity; otherwise reserve this physical core in-process
    std::string net_devices; // UCX NET_DEVICES selector; empty inherits environment/defaults
};
struct NetworkDevice
{
    std::string name;
    int numa_node = -1; // unknown when sysfs does not provide locality
};
struct Topology
{
    int cpu = -1, numa_node = -1;
    int ring_numa_node = -1;                // sampled host pages: -1 unknown (also CUDA), -2 mixed
    std::string allowed_cpus, sibling_cpus; // Linux CPU lists; siblings include cpu itself
    std::vector<NetworkDevice> network_devices; // selected endpoint devices, deduplicated
};
struct Health
{
    std::uint64_t frames = 0, skipped = 0;
    std::uint32_t every = 0, latest = 0, failed_every = 0; // every: with the pullers
    bool pressure = false;
    std::uint32_t pressure_from = 0, slots_free = 0, slots_held = 0;
    std::uint64_t last_index = 0, transfer_max_ns = 0, transfer_mean_ns = 0;
    std::uint64_t quarantined_bytes = 0;
    bool memory_hooks = true;
    bool started = false; // a publisher's: ucx_start was called
    std::string transport, failure;
    Topology topology; // sampled on the progress thread, at setup and once a second
};

class Publisher;
class Batch;
class GpuView;
struct GpuStream;
namespace transport
{
struct Pub;
struct Rings;
struct Sub;
class PendingSubscription;
std::string handle(Publisher &, std::string_view);
std::string ucx_stream(const Publisher &);
std::string ucx_sessions(const Publisher &);
// A GPU batch's one hold: when its last copy ends, the batch's event is recorded on the stream.
std::shared_ptr<void> begin_gpu_view(const Batch &, GpuStream);
const void *gpu_payload(const Batch &, std::size_t);
} // namespace transport

class Slot
{
  public:
    Slot() = default;
    Slot(Slot &&) noexcept;
    Slot &operator=(Slot &&) noexcept;
    Slot(const Slot &) = delete;
    Slot &operator=(const Slot &) = delete;
    ~Slot();
    std::span<std::byte> payload();
    std::span<std::byte> fields();
    // Queue all writes on this stream before publishing or cancelling the slot.
    void *gpu_payload(GpuStream);
    std::size_t payload_bytes() const; // capacity of the source slot
    void publish(std::optional<std::uint64_t> timestamp_ns = {}) &&;
    // An opaque frame of 1 to max_payload_bytes bytes; a refused length leaves the slot held.
    void publish_bytes(std::size_t bytes, std::optional<std::uint64_t> timestamp_ns = {}) &&;

  private:
    friend class Publisher;
    Slot(std::shared_ptr<transport::Pub>, std::uint32_t);
    std::shared_ptr<transport::Pub> state_;
    std::uint32_t index_ = 0;
    bool active_ = false;
};

class Publisher
{
  public:
    explicit Publisher(Description, PublisherLimits = {});
    Publisher(Publisher &&) noexcept;
    Publisher &operator=(Publisher &&) noexcept;
    Publisher(const Publisher &) = delete;
    Publisher &operator=(const Publisher &) = delete;
    ~Publisher();
    void ucx_start(); // slots are granted from here on; every and pull join before it
    std::optional<Slot> acquire(Deadline);
    void finish();
    Health health() const;
    const Description &description() const;

  private:
    friend class Slot;
    friend struct transport::Sub;
    friend std::string transport::handle(Publisher &, std::string_view);
    friend std::string transport::ucx_stream(const Publisher &);
    friend std::string transport::ucx_sessions(const Publisher &);
    std::shared_ptr<transport::Pub> p_;
};

// The application's CUDA stream; null is the default stream.
struct GpuStream
{
    void *handle = nullptr;
};

class Batch
{
  public:
    Batch() = default;
    Batch(Batch &&) noexcept;
    Batch &operator=(Batch &&) noexcept;
    Batch(const Batch &) = delete;
    Batch &operator=(const Batch &) = delete;
    ~Batch();
    std::size_t frames() const;
    std::uint64_t index(std::size_t frame) const;
    std::span<const std::byte> payload(std::size_t frame) const; // one frame's, in host memory
    std::size_t payload_bytes(std::size_t frame) const;
    const void *payload() const; // the packed array; an opaque batch has none
    const void *records() const;
    bool gpu() const;
    Batch copy() const;
    GpuView gpu_view(GpuStream) &&; // one per batch; the view takes the batch
    void release_after(GpuStream);  // pinned host: call after queueing the last upload read

  private:
    friend class Subscription;
    friend std::shared_ptr<void> transport::begin_gpu_view(const Batch &, GpuStream);
    friend const void *transport::gpu_payload(const Batch &, std::size_t);
    Batch(std::shared_ptr<transport::Rings>, std::uint32_t);
    struct Copy;
    std::shared_ptr<transport::Rings> rings_;
    std::shared_ptr<Copy> copy_;
    std::uint32_t first_ = ~std::uint32_t{0};
    std::size_t frames_ = 0, frame_bytes_ = 0, record_stride_ = 0;
    const std::byte *payload_ = nullptr;
    const std::byte *records_ = nullptr;
    const std::uint64_t *placements_ = nullptr; // an opaque batch: each frame's position, length
};
// Destroy it after the last kernel that reads the batch is queued on its stream.
class GpuView
{
  public:
    GpuView() = default;
    GpuView(GpuView &&) noexcept = default;
    GpuView &operator=(GpuView &&) noexcept;
    GpuView(const GpuView &) = delete;
    GpuView &operator=(const GpuView &) = delete;
    const void *payload() const;
    const void *payload(std::size_t frame) const;
    std::size_t payload_bytes(std::size_t frame) const;
    const void *records() const;
    std::size_t frames() const;

  private:
    friend class Batch;
    Batch batch_;
    std::shared_ptr<void> hold_; // ends before batch_, and records the batch's event
};

class Subscription
{
  public:
    Subscription() = default;
    Subscription(Subscription &&) noexcept;
    Subscription &operator=(Subscription &&) noexcept;
    Subscription(const Subscription &) = delete;
    Subscription &operator=(const Subscription &) = delete;
    ~Subscription();
    std::optional<Batch> read(Deadline);
    std::optional<Outcome> outcome() const;
    int fd() const;
    const Description &description() const;
    void close(Deadline);
    Health health() const;

  private:
    friend class transport::PendingSubscription;
    explicit Subscription(std::unique_ptr<transport::Sub>);
    std::unique_ptr<transport::Sub> s_;
};

} // namespace TangoUcx
