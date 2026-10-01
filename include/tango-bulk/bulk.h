// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
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

namespace TangoBulk
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
};
struct PublisherLimits
{
    std::uint64_t budget = 1ull << 30;
    std::uint32_t max_sessions = 8;
    bool allow_every = false;
    std::chrono::milliseconds pressure_timeout{0};
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
};
struct Health
{
    std::uint64_t frames = 0, skipped = 0;
    std::uint32_t every = 0, latest = 0, failed_every = 0;
    bool pressure = false;
    std::uint32_t pressure_from = 0, slots_free = 0, slots_held = 0;
    std::uint64_t last_index = 0, transfer_max_ns = 0, transfer_mean_ns = 0;
    std::uint64_t quarantined_bytes = 0;
    bool memory_hooks = true;
    std::string transport, failure;
};

class Publisher;
namespace transport
{
struct Pub;
struct Rings;
struct Sub;
class PendingSubscription;
std::string handle(Publisher &, std::string_view);
std::string bulk_stream(const Publisher &);
std::string bulk_sessions(const Publisher &);
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
    void publish(std::optional<std::uint64_t> timestamp_ns = {}) &&;

  private:
    friend class Publisher;
    Slot(std::weak_ptr<transport::Pub>, std::uint32_t);
    std::weak_ptr<transport::Pub> state_;
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
    std::optional<Slot> acquire(Deadline);
    void finish();
    Health health() const;
    const Description &description() const;

  private:
    friend class Slot;
    friend struct transport::Sub;
    friend std::string transport::handle(Publisher &, std::string_view);
    friend std::string transport::bulk_stream(const Publisher &);
    friend std::string transport::bulk_sessions(const Publisher &);
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
    const void *payload() const;
    const void *records() const;
    bool gpu() const;
    Batch copy() const;

  private:
    friend class Subscription;
    Batch(std::shared_ptr<transport::Rings>, std::uint32_t);
    struct Copy;
    std::shared_ptr<transport::Rings> rings_;
    std::shared_ptr<Copy> copy_;
    std::uint32_t first_ = ~std::uint32_t{0};
    std::size_t frames_ = 0, frame_bytes_ = 0, record_stride_ = 0;
    const std::byte *payload_ = nullptr;
    const std::byte *records_ = nullptr;
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

} // namespace TangoBulk
