// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-ucx project
// SPDX-License-Identifier: LGPL-3.0-or-later
// The publisher's internals: the engine that lives as long as the Publisher (engine.cpp) and
// the publisher instances it drives (publisher.cpp).
#pragma once

#include "publisher_operations.h"
#include "tables.h"
#include "topology.h"

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <future>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

namespace TangoUcx::transport
{
struct SourceSlot
{
    std::atomic<bool> posted{false};
    bool cancel = false, held = false;
    std::uint64_t timestamp = 0, payload_bytes = 0;
    std::uint32_t sends = 0;
    bool gpu_accessed = false;
    GpuStream gpu_stream;
    GpuEvent event;
};
struct PubRow
{
    Pub *pub = nullptr;
    std::uint8_t row = 0;
    std::uint32_t ordinal = 0;
    ucp_ep_h ep = nullptr;
    ucs_status_t transport_error = UCS_OK;
    std::uint64_t session = 0, sent = 0, credited = 0, skipped = 0, passes = 0;
    // An opaque stream's packed ring, and where its last sent and last credited frames end.
    std::uint64_t ring_bytes = 0, sent_bytes = 0, credited_bytes = 0;
    std::uint32_t ring_depth = 0, in_flight = 0;
    std::int64_t min_interval_ns = 0, last_send_ns = 0, blocked_since_ns = 0;
    std::int64_t transfer_max_ns = 0, transfer_sum_ns = 0;
    Text<64> label, transport;
    std::vector<NetworkDevice> network_devices;
    bool used = false, ready = false, every = false, closing = false, end_due = false,
         end_sent = false, gpu = false, pull = false;
    bool flush_started = false, flush_done = false, close_started = false, close_done = false;
    wire::Reason reason = wire::FinishedReason;
    std::int64_t retired_ns = 0;
    ControlEntry *close_reply = nullptr;
};
struct RowObservation
{
    std::uint32_t ordinal = 0, depth = 0, room = 0;
    bool every = false, retiring = false;
    std::uint64_t sent = 0, skipped = 0;
    std::int64_t transfer_max_ns = 0, transfer_mean_ns = 0;
    Text<64> label, transport;
    bool pull = false;
    std::uint64_t passes = 0;
};
struct Observation
{
    std::uint64_t frames = 0, skipped = 0, missing = 0;
    std::uint32_t failed_every = 0, pressure_from = 0, slots_free = 0, slots_held = 0, count = 0;
    bool memory_hooks = true;
    bool source_failed = false;
    std::array<RowObservation, 64> rows{};
};
struct Engine;
struct Pub;
// The every sessions of an instance (ADR 0002): each takes every frame and gates every grant.
// They join before the first grant, and each grant is one more frame for all of them, so what
// they have reserved is one position of the instance: its grants not yet routed.
struct EverySessions
{
    std::uint64_t mask = 0;
    bool has(std::uint32_t row) const
    {
        return mask >> row & 1;
    }
    void add(std::uint32_t row)
    {
        mask |= std::uint64_t{1} << row;
    }
    void remove(std::uint32_t row)
    {
        mask &= ~(std::uint64_t{1} << row);
    }
    int blocking(const Pub &) const; // the first row with no room for one more grant, or -1
};
struct SourceSlot;
// The latest sessions of an instance (ADR 0002): each takes a frame when it has room and is due,
// and skips it otherwise. They never gate a grant.
struct LatestSessions
{
    std::uint64_t mask = 0;
    void add(std::uint32_t row)
    {
        mask |= std::uint64_t{1} << row;
    }
    void remove(std::uint32_t row)
    {
        mask &= ~(std::uint64_t{1} << row);
    }
    // The rows that take this frame now; the others skip it.
    std::uint64_t takers(const Pub &, const SourceSlot &, std::int64_t now) const;
};
// The pullers of an instance (ADR 0002): a set that takes each frame once, a range at a time.
// Ranges are kept on the frame index, so a cancelled slot, which uses no index, moves every
// later grant down by one.
struct PullSet
{
    // Its rows, the frames of a range, the ranges given out so far, the row each of the latest
    // went to, the row that took the last, and the frames no puller received.
    std::uint64_t mask = 0, ranges_taken = 0, missing = 0;
    std::uint32_t range_frames = 0;
    std::vector<std::uint8_t> range_pullers;
    std::uint8_t last_taker = 63;
    std::array<std::uint32_t, 64> reserved{}; // each puller's frames granted and not yet routed
    bool has(std::uint32_t row) const
    {
        return mask >> row & 1;
    }
    void add(std::uint32_t row, std::uint32_t range)
    {
        mask |= std::uint64_t{1} << row;
        range_frames = range;
    }
    void remove(const PubRow &, bool started);
    PubRow *puller(Pub &, std::uint64_t frame_index);
    // Room for the next grant, or the row the device waits for.
    int reserve(Pub &);
    // The puller of the next slot to route, before the slot is counted as routed.
    PubRow *route(Pub &, bool cancel);
    // The pullers whose End must wait.
    std::uint64_t end_waits(const Pub &) const;
    // The newest grants, which the device never took.
    void take_back(Pub &, std::uint32_t grants);
    void release(std::uint32_t row);
    void check(std::uint64_t outstanding) const; // asserts; nothing in a release build
};
// One publisher instance: one description, its sessions, slots, records and grants. The engine
// drives it on its progress thread; the device holds it through its slots.
struct Pub final : Owner, std::enable_shared_from_this<Pub>
{
    std::shared_ptr<Engine> engine; // first, so it is released last
    Description description;
    PublisherLimits limits;
    std::uint64_t instance_id = 0;
    PublisherPlan plan;
    // The source ring: a region of the engine's arena, or this instance's own GPU memory.
    std::byte *source_base = nullptr;
    ucp_mem_h source_memh = nullptr;
    Region gpu_source;
    std::shared_ptr<void> source_storage; // keeps GPU writes safe when a slot outlives Publisher
    // The arena this instance still read when the next began: released with the instance.
    Region kept_arena;
    std::vector<std::byte> records;
    PublisherOperations operations;
    std::unique_ptr<SourceSlot[]> slots;
    std::vector<std::uint16_t> order;
    std::array<PubRow, 64> rows{};
    std::uint64_t ready_mask = 0;
    EverySessions every;
    LatestSessions latest;
    PullSet pull;
    std::uint64_t slots_granted = 0, slots_routed = 0, next_frame_index = 0, skipped = 0;
    std::uint32_t scan = 0, failed_every = 0, blocked = 0, ordinals = 0;
    IndexRing grants{IndexRing::Fd::None}; // acquire sleeps on the queue; nobody polls a grant fd
    HealthCopy<Observation> health_copy;
    Topology topology;
    HealthCopy<Topology> topology_copy;
    std::int64_t last_topology_ns = 0;
    std::atomic<bool> started{false}, finishing{false};
    bool ended = false, quarantined = false;
    bool source_failed = false;
    std::shared_ptr<Pub> self_keep;
    Pub(std::shared_ptr<Engine>, Description, PublisherPlan);
    void setup();        // on the progress thread, once the source ring is set
    bool closed() const; // the publisher has stopped
    std::byte *source_at(std::uint64_t offset) const
    {
        return source_base + offset;
    }
    bool next_posted() const;
    // Nothing of this instance is being sent, nor can be: the device returned every slot it
    // acquired, every send completed and every ready session was sent End.
    bool quiet() const;
    bool done() const; // ended, and every session closed
    void retire_all();
    void release_memory() noexcept; // on the progress thread: what is registered with UCX
    void open(ControlEntry &);
    void close(ControlEntry &);
    void publish_slot(std::uint32_t, std::optional<std::uint64_t>, std::optional<std::size_t>);
    void cancel_slot(std::uint32_t);
    void post(SourceSlot &);
    std::span<std::byte> payload(std::uint32_t);
    std::span<std::byte> fields(std::uint32_t);
    void *gpu_payload(std::uint32_t, GpuStream);
    std::uint64_t id_for(std::uint32_t) const;
    int find_row(std::uint64_t) const;
    std::uint64_t outstanding() const // grants not yet routed
    {
        return slots_granted - slots_routed;
    }
    bool has_room(const PubRow &, std::uint64_t frames = 1, std::uint64_t grants = 1) const;
    wire::Status check_open(const wire::OpenMessage &) const;
    void unissue_grants();
    void check_reservations() const; // asserts; nothing in a release build
    bool route_pass(std::int64_t);
    bool grant_pass();
    bool retire_pass(std::int64_t);
    void timer_pass(std::int64_t);
    void publish_health();
    void retire(std::uint32_t, wire::Reason, bool failed = false);
    void send_end(std::uint32_t);
    void start_teardown(std::uint32_t, OpKind, bool force = false);
    void send_frame(std::uint32_t, std::uint32_t, std::int64_t);
    void send_control(std::uint32_t, OpKind, std::uint8_t, std::span<const std::byte>);
    void on_send(OpMeta *, ucs_status_t);
    void on_control(OpMeta *, ucs_status_t);
    void quarantine_owner();
};

// What a publisher keeps for its whole life: the UCX context and worker, the progress thread,
// the control table and the arena. It drives one active instance and the earlier instances that
// still close their sessions (ADR 0001).
struct Engine final : Owner, std::enable_shared_from_this<Engine>
{
    PublisherLimits limits;
    std::unique_ptr<ucp::Context> context;
    std::unique_ptr<ucp::Worker> worker;
    std::uint32_t request_bytes = 0;
    std::uint64_t max_header = 0;
    bool memory_hooks = true;
    // Registered once and faulted in, it holds the active instance's source ring. It is
    // replaced only when a ring needs more, or when an instance that may still read it leaves.
    Region arena;
    ControlTable control;
    Wake wake; // a posted slot, a control request or a poke ends the loop's wait
    std::atomic<bool> poked{false};
    std::jthread loop;
    std::atomic<bool> stopping{false};
    bool setup_ok = false, quarantined = false;
    bool tainted = false; // an instance was quarantined: the worker may still hold its endpoints
    std::promise<void> setup_done;
    std::shared_ptr<Engine> self_keep;

    // The progress thread's own.
    std::shared_ptr<Pub> active;
    std::vector<std::shared_ptr<Pub>> draining;
    std::int64_t begin_since_ns = 0;
    // The active instance, for every other thread.
    mutable std::mutex current_lock;
    std::shared_ptr<Pub> current;

    struct BeginRequest
    {
        Description description;
        PublisherPlan plan;
        std::promise<void> done;
    };
    std::mutex begin_serial; // one begin at a time
    std::mutex begin_lock;
    std::shared_ptr<BeginRequest> begin_request;
    bool begin_closed = false; // under begin_lock: shutdown has refused what is left
    std::atomic<bool> begin_posted{false};

    explicit Engine(PublisherLimits);
    ~Engine();
    void start();
    void stop();
    void run();
    void setup();
    void shutdown();
    void poke(); // another thread made work for the loop
    void begin(Description);
    bool begin_pass(std::int64_t);
    void make_instance(BeginRequest &);
    bool control_pass();
    bool busy() const;
    Pub *owner_of(std::uint64_t session) const;
    std::shared_ptr<Pub> instance() const;
    void set_current(std::shared_ptr<Pub>);
};

inline constexpr std::int64_t k_close_ns = 2'000'000'000LL, k_quarantine_ns = 1'000'000'000LL;
// How often eviction is checked and Health is sampled.
inline constexpr std::int64_t k_timer_ns = 1'000'000LL;
// The longest wait of a loop with no instance that has work: a poke, a control request or a
// worker event ends it sooner.
inline constexpr std::int64_t k_idle_ns = 100'000'000LL;
inline std::int64_t now_ns()
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}
} // namespace TangoUcx::transport
