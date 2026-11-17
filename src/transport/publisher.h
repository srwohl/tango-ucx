// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-ucx project
// SPDX-License-Identifier: LGPL-3.0-or-later
// The publisher's internals: the engine that lives as long as the Publisher (engine.cpp) and
// the publisher instances it drives (publisher.cpp).
#pragma once

#include "publisher_operations.h"
#include "row_set.h"
#include "tables.h"
#include "topology.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
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
    // Where the payload is sent from: the slot's own memory, or a lent ring's buffer, which
    // `done` hands back to the device once the frame's last send completes.
    std::uint64_t source_offset = 0;
    std::function<void()> done;
    bool routing = false; // its frame is being sent to its recipients
    bool gpu_accessed = false;
    GpuStream gpu_stream;
    GpuEvent event;
};
// A session's receive ring, as the publisher keeps account of it. The publisher keeps two
// positions per ring, what it sent and what the subscriber credited back, and both only increase
// (ADR 0002). What is between them is still in the subscriber's ring, and the rest is room:
//
//     credited          sent
//        |-- unreleased --|-- reserved --|---- free ----|
//        |<------------------- depth ------------------>|
//
// Room promised to grants not yet routed is not kept here: the caller passes it as `reserved`.
//
// A frame takes a row of the subscriber's receive tables, so `depth` rows limit every ring. In
// a packed ring (an opaque stream's), frames differ in size and lie end to end, so the frame's
// payload must also fit in the ring's `bytes`: a packed ring has both limits. Any other ring
// gives each frame a slot of its own, and `depth` is its only limit.
struct SessionRing
{
    std::uint32_t depth = 0; // frames: the rows of the subscriber's receive tables
    std::uint64_t bytes = 0; // a packed ring's size, or 0 when each frame has a slot of its own
    // Frames since the session began. `sent` is also the next frame's sequence number.
    std::uint64_t sent = 0, credited = 0;
    // A packed ring's byte positions: where the last sent and the last credited frames end.
    std::uint64_t sent_bytes = 0, credited_bytes = 0;

    bool packed() const
    {
        return bytes != 0;
    }
    // Frames sent and not yet credited: in flight, or held by the subscriber.
    std::uint64_t unreleased() const
    {
        return sent - credited;
    }
    // Room for `frames` more frames after the `reserved` ones and, in a packed ring, bytes for
    // `grants` more grants of at most `max_frame_bytes` each. Both are 1 for one more grant.
    bool has_room(std::uint64_t reserved, std::uint64_t frames, std::uint64_t grants,
                  std::uint64_t max_frame_bytes) const
    {
        bool rows_free = unreleased() + reserved + frames <= depth;
        bool bytes_free = !packed() || ring_room(sent_bytes - credited_bytes, reserved + grants - 1,
                                                 max_frame_bytes, bytes);
        return rows_free && bytes_free;
    }
    // Room for one frame of `payload` bytes now, with none reserved: what a latest session asks
    // before it takes a frame.
    bool fits_now(std::uint64_t payload) const
    {
        return unreleased() < depth &&
               (!packed() || next_position(payload) + payload - credited_bytes <= bytes);
    }
    // Where the next frame of `payload` bytes starts in a packed ring, or 0.
    std::uint64_t next_position(std::uint64_t payload) const
    {
        return packed() ? place(sent_bytes, payload, bytes) : 0;
    }
    void record_send(std::uint64_t position, std::uint64_t payload)
    {
        ++sent;
        if(packed())
            sent_bytes = position + payload;
    }
    // A subscriber's Credit. Credits are cumulative, so an older one never moves a position
    // back. False when it releases more than was sent: a protocol error.
    bool credit(std::uint64_t frames, std::uint64_t released_bytes)
    {
        if(frames > sent || released_bytes > sent_bytes)
            return false;
        credited = std::max(credited, frames);
        credited_bytes = std::max(credited_bytes, released_bytes);
        return true;
    }
    // The room Health reports, in frames: the rows not holding an unreleased frame and, in a
    // packed ring, no more than the frames of the largest size that fit in the free bytes with
    // bytes kept for a wrap, which skips less than one frame (ring_room solved for its frames).
    // Grants not yet routed are not subtracted, unlike in has_room: this is the subscriber's
    // free space.
    std::uint64_t room(std::uint64_t max_frame_bytes) const
    {
        auto frames = depth - std::min<std::uint64_t>(depth, unreleased());
        if(!packed())
            return frames;
        auto fit = (bytes - (sent_bytes - credited_bytes) + 1) / max_frame_bytes;
        return std::min(frames, fit ? fit - 1 : 0);
    }
};
// Where a session is in its life: the session states of docs/diagrams.md, section 6. A row is
// Free until an Open is accepted, Ready while it receives frames, Ended once the instance has
// ended and the session was sent End (finished), and Retiring from the moment it leaves until
// its endpoint is closed and the row is Free again. Pressure is not stored: pressure_row()
// works it out.
enum class SessionState : std::uint8_t
{
    Free,
    Ready,
    Ended,
    Retiring
};
// The End message a session is sent once, at most: not due, due, or sent. An Ended session
// has been sent End; a retiring one is sent End only if its cause of retirement asks for it.
enum class EndNotice : std::uint8_t
{
    NotDue,
    Due,
    Sent
};
// How far a retiring session's endpoint has been torn down: flushed after its last send, then
// closed (retire_pass). A late row is closed by force from any step before Closing, even with
// its flush still in flight.
enum class Teardown : std::uint8_t
{
    None,
    Flushing,
    Flushed,
    Closing,
    Closed
};
// Why a session retires. The cause decides the reason its End gives, whether it is sent End at
// all, and whether it counts as failed (the table in docs/diagrams.md, section 6; retirement()
// in publisher.cpp).
enum class RetireCause : std::uint8_t
{
    CloseCommand,    // the subscriber sent Close
    Abandoned,       // the subscriber stopped waiting for the reply to its Open
    TransportFailed, // UCX reported the endpoint failed, or a send failed
    Protocol,        // the subscriber broke the protocol: too much Credit, or a rejected send
    SourceFailed,    // a GPU source event failed, which finishes the instance
    Evicted,         // the device waited for its room longer than pressure_timeout
    InstanceClosing  // the publisher closes the instance: it is destroyed, or the next began
};
struct PubRow
{
    Pub *pub = nullptr;
    std::uint8_t row = 0;
    std::uint32_t ordinal = 0;
    ucp_ep_h ep = nullptr;
    ucs_status_t transport_error = UCS_OK;
    std::uint64_t session = 0, skipped = 0, passes = 0;
    SessionRing ring;
    std::uint32_t in_flight = 0;
    std::int64_t min_interval_ns = 0, last_send_ns = 0, blocked_since_ns = 0;
    std::int64_t transfer_max_ns = 0, transfer_sum_ns = 0;
    Text<64> label, transport;
    std::vector<NetworkDevice> network_devices;
    SessionState state = SessionState::Free;
    EndNotice end = EndNotice::NotDue;
    bool every = false, gpu = false, pull = false;
    Teardown teardown = Teardown::None;
    wire::Reason reason = wire::FinishedReason;
    std::int64_t retired_ns = 0;
    ControlEntry *close_reply = nullptr;

    bool in_use() const // a session holds the row: it is not Free
    {
        return state != SessionState::Free;
    }
    bool live() const // Ready or Ended: the session has not begun to retire
    {
        return state == SessionState::Ready || state == SessionState::Ended;
    }
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
    RowSet members;
    bool has(std::uint32_t row) const
    {
        return members.has(row);
    }
    void add(std::uint32_t row)
    {
        members.add(row);
    }
    void remove(std::uint32_t row)
    {
        members.remove(row);
    }
    // The first row with no room for one more grant, if any.
    std::optional<std::uint32_t> blocking(const Pub &) const;
};
struct SourceSlot;
// The latest sessions of an instance (ADR 0002): each takes a frame when it has room and is due,
// and skips it otherwise. They never gate a grant.
struct LatestSessions
{
    RowSet members;
    void add(std::uint32_t row)
    {
        members.add(row);
    }
    void remove(std::uint32_t row)
    {
        members.remove(row);
    }
    // The rows that take this frame now; the others skip it.
    RowSet takers(const Pub &, const SourceSlot &, std::int64_t now) const;
};
// The pullers of an instance (ADR 0002): a set that takes each frame once, a range at a time.
// Ranges are kept on the frame index, so a cancelled slot, which uses no index, moves every
// later grant down by one.
struct PullSet
{
    // Its rows, the frames of a range, the ranges given out so far, the row each of the latest
    // went to, the row that took the last, and the frames no puller received.
    RowSet members;
    std::uint64_t ranges_taken = 0, missing = 0;
    std::uint32_t range_frames = 0;
    std::vector<std::uint8_t> range_pullers;
    std::uint8_t last_taker = 63;
    std::array<std::uint32_t, 64> reserved{}; // each puller's frames granted and not yet routed
    bool has(std::uint32_t row) const
    {
        return members.has(row);
    }
    void add(std::uint32_t row, std::uint32_t range)
    {
        members.add(row);
        range_frames = range;
    }
    void remove(const PubRow &, bool started);
    PubRow *puller(Pub &, std::uint64_t frame_index);
    // Room for the next grant, or the row the device waits for.
    std::optional<std::uint32_t> reserve(Pub &);
    // The puller of the next slot to route, before the slot is counted as routed.
    PubRow *route(Pub &, bool cancel);
    // The pullers whose End must wait.
    RowSet end_waits(const Pub &) const;
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
    RowSet ready_rows; // the rows whose session is ready and not retiring
    EverySessions every;
    LatestSessions latest;
    PullSet pull;
    std::uint64_t slots_granted = 0, slots_routed = 0, next_frame_index = 0, skipped = 0;
    std::uint32_t scan = 0, failed_every = 0, ordinals = 0;
    // The row the last grant pass stopped at: a slot was free, and this row had no room for it.
    std::optional<std::uint32_t> blocked;
    IndexRing grants{IndexRing::Fd::None}; // acquire sleeps on the queue; nobody polls a grant fd
    HealthCopy<Observation> health_copy;
    Topology topology;
    HealthCopy<Topology> topology_copy;
    std::int64_t last_topology_ns = 0;
    std::atomic<bool> started{false}, finishing{false};
    // Every sessions and pullers that are ready and not retiring. Written on the progress thread
    // as they join and retire, so a waiting start reads it exactly, unlike Health.
    std::atomic<std::uint32_t> holding{0};
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
    Record record(std::uint32_t slot_index)
    {
        return {records.data() + std::uint64_t(slot_index) * plan.sizes.record_stride};
    }
    bool next_posted() const;
    // Nothing of this instance is being sent, nor can be: the device returned every slot it
    // acquired, every send completed and every ready session was sent End.
    bool quiet() const;
    bool done() const; // ended, and every session closed
    void retire_all();
    void count_holding();
    void release_memory() noexcept; // on the progress thread: what is registered with UCX
    void open(ControlEntry &);
    void close(ControlEntry &);
    void publish_slot(std::uint32_t, std::optional<std::uint64_t>, std::optional<std::size_t>,
                      std::optional<std::uint32_t> buffer = {}, std::function<void()> done = {});
    void hand_back(SourceSlot &); // a lent buffer, once its frame's last send completed
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
    std::optional<std::uint32_t> pressure_row() const;
    void publish_health();
    void retire(std::uint32_t, RetireCause);
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
    Wake wake; // a posted slot or a poke ends the loop's wait
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
    std::shared_ptr<BeginRequest> making; // the progress thread's: taken, waiting for its arena
    std::future<void> prefaulting;        // faulting in a new arena's pages
    bool begin_closed = false;            // under begin_lock: shutdown has refused what is left
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
