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
#include <cassert>
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
// What the publisher does when something that must hold does not, by whose fault it is:
//
//   - the device misuses the API (publishes twice, the wrong kind of slot): it throws, and the
//     device's call fails;
//   - a subscriber breaks the protocol (too much Credit, a rejected send): its session retires
//     with RetireCause::Protocol, and the others go on;
//   - the publisher's own bug: an assert, and nothing in a release build, except for the counts
//     the progress loop waits on to reach zero, which count_down keeps from wrapping, since a
//     wrapped count would stall the loop for good.
template <class Count> Count count_down(Count &count)
{
    assert(count > 0);
    if(count > 0)
        --count;
    return count;
}
// A slot's place in its cycle, as the progress loop knows it: the source slot states of
// docs/diagrams.md, section 8. The loop cannot tell Granted from the diagram's Held, because the
// device takes a grant from the grant ring without telling it. Posted and Cancelled are the
// device's word: the slot's `posted` and its publication's `cancel`.
enum class SlotState : std::uint8_t
{
    Free,    // no grant, and no send of its frame in flight
    Granted, // granted, and perhaps acquired and filled by the device
    Sending  // routed, with sends of its frame in flight
};
// One slot of the source ring. Two threads use it, never at the same time: ownership moves with
// the slot.
//
//   1. The loop grants it (`granted`) and pushes it to the grant ring.
//   2. The device acquires it, fills the payload and the publication, and posts it.
//   3. The loop sees `posted`, reads the publication and routes the frame (`sends`).
//   4. Once the last send completes the slot is Free, and a lent buffer goes back to the device
//      through the publication's `done`.
struct SourceSlot
{
    // What the device says about its frame: cleared by the loop as it grants the slot, written
    // on the device's thread between acquire and post, and read by the loop once it sees
    // `posted`.
    struct Publication
    {
        std::uint64_t timestamp = 0, payload_bytes = 0;
        bool cancel = false; // given back unpublished: routed in grant order, but sent nowhere
        // Where the payload is sent from: the slot's own memory, or a lent ring's buffer, which
        // `done` hands back to the device once the frame's last send completes.
        std::uint64_t source_offset = 0;
        std::function<void()> done;
        bool gpu_accessed = false; // the device wrote the payload through gpu_payload
        GpuStream gpu_stream;      // on this stream
    };
    // The handoff from the device to the loop. The device stores it last, after the
    // publication, and the loop loads it first, before reading the publication. The store is
    // sequentially consistent, as Wake asks (post()).
    std::atomic<bool> posted{false};
    Publication publication;
    // The loop's own.
    bool granted = false;
    std::uint32_t sends = 0; // sends of its frame in flight
    bool routing = false;    // its frame is being sent to its recipients: `done` waits
    GpuEvent event;          // made at setup: the device records it, and the loop polls it

    SlotState state() const
    {
        if(granted)
            return SlotState::Granted;
        return sends ? SlotState::Sending : SlotState::Free;
    }
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
        assert(credited <= sent);
        return sent - credited;
    }
    // Rows for `frames` more frames after the `reserved` ones.
    bool rows_free(std::uint64_t reserved, std::uint64_t frames) const
    {
        return unreleased() + reserved + frames <= depth;
    }
    // In a packed ring, bytes for `grants` more grants of at most `max_frame_bytes` each after the
    // `reserved` ones. Any other ring has a slot for each row, so rows_free says it all.
    bool bytes_free(std::uint64_t reserved, std::uint64_t grants,
                    std::uint64_t max_frame_bytes) const
    {
        return !packed() || ring_room(sent_bytes - credited_bytes, reserved + grants - 1,
                                      max_frame_bytes, bytes);
    }
    // Room for one more grant after the `reserved` ones.
    bool has_room(std::uint64_t reserved, std::uint64_t max_frame_bytes) const
    {
        return rows_free(reserved, 1) && bytes_free(reserved, 1, max_frame_bytes);
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
        assert(unreleased() <= depth);
        auto frames = depth - unreleased();
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
// One session of an instance: the progress loop's own.
struct PubRow
{
    Pub *pub = nullptr;
    std::uint8_t row = 0;
    std::uint32_t ordinal = 0;
    ucp_ep_h ep = nullptr;
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
class EverySessions
{
  public:
    RowSet members() const
    {
        return members_;
    }
    bool has(std::uint32_t row) const
    {
        return members_.has(row);
    }
    void add(std::uint32_t row)
    {
        members_.add(row);
    }
    void remove(std::uint32_t row)
    {
        members_.remove(row);
    }
    // The first row with no room for one more grant, if any.
    std::optional<std::uint32_t> blocking(const Pub &) const;

  private:
    RowSet members_;
};
struct SourceSlot;
// The latest sessions of an instance (ADR 0002): each takes a frame when it has room and is due,
// and skips it otherwise. They never gate a grant.
class LatestSessions
{
  public:
    RowSet members() const
    {
        return members_;
    }
    void add(std::uint32_t row)
    {
        members_.add(row);
    }
    void remove(std::uint32_t row)
    {
        members_.remove(row);
    }
    // The rows that take this frame now; the others skip it.
    RowSet takers(const Pub &, const SourceSlot &, std::int64_t now) const;

  private:
    RowSet members_;
};
// The pullers of an instance (ADR 0002): a set that takes each frame once, a range at a time.
// Ranges are kept on the frame index, so a cancelled slot, which uses no index, moves every
// later grant down by one.
class PullSet
{
  public:
    void setup(std::uint32_t slot_count);
    RowSet members() const
    {
        return members_;
    }
    // The frames no puller received, which every puller's End reports.
    std::uint64_t missing() const
    {
        return missing_;
    }
    bool has(std::uint32_t row) const
    {
        return members_.has(row);
    }
    wire::Status check_open(const wire::OpenMessage &) const;
    void add(std::uint32_t row, std::uint32_t range)
    {
        members_.add(row);
        range_frames_ = range;
    }
    void remove(const PubRow &, bool started);
    // The puller that holds the range of a frame index, if it is still in the set. A lookup: it
    // changes nothing.
    PubRow *holder(Pub &, std::uint64_t frame_index) const;
    // Gives the range of a frame index, untaken or left by its puller, to the next puller that
    // can take it, and returns that puller.
    PubRow *take_range(Pub &, std::uint64_t frame_index);
    // Room for the next grant, or the row the device waits for.
    std::optional<std::uint32_t> reserve(Pub &);
    // The puller of the next published slot to route, before the slot is counted as routed.
    PubRow *route(Pub &);
    // The pullers whose End must wait.
    RowSet end_waits(const Pub &) const;
    // The grants not yet routed whose frame index lies in a range the row holds.
    std::uint64_t reserved(const Pub &, std::uint32_t row) const;

  private:
    // Its rows, the frames of a range, the ranges given out so far, the row each of the latest
    // went to, the row that took the last, and the frames no puller received.
    RowSet members_;
    std::uint64_t ranges_taken_ = 0, missing_ = 0;
    std::uint32_t range_frames_ = 0;
    std::vector<std::uint8_t> range_pullers_;
    std::uint8_t last_taker_ = 63;

    bool untaken(std::uint64_t frame_index) const
    {
        return frame_index / range_frames_ == ranges_taken_;
    }
    // Where range_pullers_ keeps the puller of a frame index's range.
    std::size_t entry(std::uint64_t frame_index) const
    {
        return frame_index / range_frames_ % range_pullers_.size();
    }
    std::uint64_t granted_in_range(const Pub &, std::uint64_t frame_index) const;
    bool can_take_range(const Pub &, std::uint32_t row, bool untaken, std::uint64_t granted) const;
};
// The sessions a published frame is sent to, and the latest sessions that skip it.
struct Recipients
{
    RowSet send, skip;
};
// The phases of a publisher instance: docs/diagrams.md, section 5. Before the start, sessions
// join and no slot is granted. Started, slots are granted while the sessions have room.
// Finishing, no more slots are granted and the ones not yet acquired are taken back. Ended,
// every slot the device held is routed, and the sessions are sent End.
enum class Phase : std::uint8_t
{
    Before,
    Started,
    Finishing,
    Ended
};
// An instance's phase, which only moves forward. Device threads start and finish the instance,
// and so does begin() on its caller's thread; only the progress loop ends it. Each step is one
// compare-and-swap, so a start and a finish that race cannot both win: an instance that is
// finishing is never started. It also keeps whether the instance was ever started, which the
// phase alone cannot say once it is finishing: Health reports it.
class InstancePhase
{
  public:
    Phase load() const
    {
        return phase_of(state_.load(std::memory_order_acquire));
    }
    bool was_started() const
    {
        return (state_.load(std::memory_order_acquire) & k_started) != 0;
    }
    // Before to Started. True when the instance is started, also by an earlier call; false
    // when it is finishing or has ended.
    bool start()
    {
        auto expected = std::uint8_t(Phase::Before);
        if(state_.compare_exchange_strong(expected, std::uint8_t(Phase::Started) | k_started,
                                          std::memory_order_acq_rel))
            return true;
        return phase_of(expected) == Phase::Started;
    }
    // Before or Started to Finishing. True when this call finished it.
    bool finish()
    {
        auto current = state_.load(std::memory_order_acquire);
        while(phase_of(current) < Phase::Finishing)
            if(state_.compare_exchange_weak(current,
                                            std::uint8_t(Phase::Finishing) | (current & k_started),
                                            std::memory_order_acq_rel))
                return true;
        return false;
    }
    // Finishing to Ended: the progress loop, once every granted slot is routed. Nobody else
    // moves an instance on from Finishing, so a plain store keeps the started bit.
    void end()
    {
        auto current = state_.load(std::memory_order_acquire);
        state_.store(std::uint8_t(Phase::Ended) | (current & k_started), std::memory_order_release);
    }

  private:
    // The phase in the low bits and, in the top bit, whether the instance was ever started. One
    // atomic, so the two always change, and are read, together.
    static constexpr std::uint8_t k_started = 0x80;
    static Phase phase_of(std::uint8_t state)
    {
        return Phase(state & ~k_started);
    }
    std::atomic<std::uint8_t> state_{std::uint8_t(Phase::Before)};
};
// One publisher instance: one description, its sessions, slots, records and grants. The engine
// drives it on its progress thread; the device holds it through its slots.
//
// The progress loop makes an instance and hands it to the other threads (Engine::set_current).
// From then on its fields fall in four groups, by the thread that writes them:
//
//   - fixed at setup: never written again, and read on any thread;
//   - shared: each safe on any thread, on the terms its comment gives;
//   - a slot and its record: whoever holds the slot (SourceSlot);
//   - the rest: the progress loop's own.
struct Pub final : Owner, std::enable_shared_from_this<Pub>
{
    // Fixed at setup.
    std::shared_ptr<Engine> engine; // first, so it is released last
    Description description;
    PublisherLimits limits;
    std::uint64_t instance_id = 0;
    PublisherPlan plan;
    // The source ring: a region of the engine's arena, or this instance's own GPU memory. The
    // pointers stay set when the memory is released (release_memory).
    std::byte *source_base = nullptr;
    ucp_mem_h source_memh = nullptr;
    Region gpu_source;
    std::shared_ptr<void> source_storage; // keeps GPU writes safe when a slot outlives Publisher
    // The arrays are fixed; each slot, and the record of the same index, belongs to whoever holds
    // the slot. The device writes a record's fields between acquire and post, and the loop its
    // index and timestamp as it routes the slot.
    std::unique_ptr<SourceSlot[]> slots;
    std::vector<std::byte> records;

    // Shared.
    InstancePhase phase; // any thread starts and finishes it; only the loop ends it
    // Every sessions and pullers that are ready and not retiring. The loop writes it as they join
    // and retire, so a waiting start reads it exactly, unlike Health.
    std::atomic<std::uint32_t> holding{0};
    // The loop pushes grants and takes back the ones not acquired, the device pops them, and
    // finish and begin wake a device that waits. acquire sleeps on the queue; nobody polls a
    // grant fd.
    IndexRing grants{IndexRing::Fd::None};
    // The loop writes them, and health() and the Ucx commands read them.
    HealthCopy<Observation> health_copy;
    HealthCopy<Topology> topology_copy;

    // The progress loop's own.
    // The arena this instance still read when the next began: released with the instance.
    Region kept_arena;
    PublisherOperations operations;
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
    Topology topology;
    std::int64_t last_topology_ns = 0;
    bool quarantined = false;
    bool source_failed = false;
    std::shared_ptr<Pub> self_keep;

    Pub(std::shared_ptr<Engine>, Description, PublisherPlan);

    // On a device thread, for a slot it holds (Slot): they read the fixed fields, and write only
    // the slot and its record.
    std::span<std::byte> payload(std::uint32_t);
    std::span<std::byte> fields(std::uint32_t);
    void *gpu_payload(std::uint32_t, GpuStream);
    void publish_slot(std::uint32_t, std::optional<std::uint64_t>, std::optional<std::size_t>,
                      std::optional<std::uint32_t> buffer = {}, std::function<void()> done = {});
    void cancel_slot(std::uint32_t); // also on whatever thread drops a Slot it did not publish
    void post(SourceSlot &);

    // On any thread: they read only fixed fields, and the engine's `stopping`.
    bool closed() const; // the publisher has stopped
    std::byte *source_at(std::uint64_t offset) const
    {
        return source_base + offset;
    }
    Record record(std::uint32_t slot_index)
    {
        return {records.data() + std::uint64_t(slot_index) * plan.sizes.record_stride};
    }

    // On the progress loop, which includes the UCX callbacks it runs inside worker->progress():
    // on_send and on_control, and the endpoint error and Credit handlers, which retire a session.
    void setup(); // once the source ring is set, before the instance is handed out
    bool next_posted() const;
    // Nothing of this instance is being sent, nor can be: the device returned every slot it
    // acquired, every send completed and every ready session was sent End.
    bool quiet() const;
    bool done() const; // Ended, and every session closed
    void retire_all();
    void count_holding();
    void release_memory() noexcept; // what is registered with UCX
    void open(ControlEntry &);
    void close(ControlEntry &);
    // A lent buffer, once its frame's last send completed: the device's `done` runs here, on the
    // loop.
    void hand_back(SourceSlot &);
    int find_row(std::uint64_t) const;
    std::uint64_t outstanding() const // grants not yet routed
    {
        return slots_granted - slots_routed;
    }
    RowSet device_holders() const;
    void join_delivery(std::uint32_t, const wire::OpenMessage &);
    bool leave_delivery(const PubRow &);
    Recipients recipients(const SourceSlot &, std::int64_t now);
    bool blocks(std::uint32_t, std::optional<std::uint32_t> pressure) const;
    std::optional<std::uint32_t> reserve_grant();
    bool has_room(const PubRow &) const;
    wire::Status check_open(const wire::OpenMessage &) const;
    void unissue_grants();
    void check_members() const; // asserts; nothing in a release build
    bool route_pass(std::int64_t);
    bool route_next(std::int64_t);
    GpuEventState source_state(const SourceSlot &) const;
    void fail_source(SourceSlot &);
    void route_frame(std::uint32_t, const Recipients &, std::int64_t);
    void send_ends();
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
//
// Its fields fall in groups by the thread that writes them, as an instance's do (Pub): fixed at
// setup, shared, and the progress loop's own. The other threads are those that use the
// Publisher: its constructor, destructor and begin, the Ucx commands and a device that acquires.
struct Engine final : Owner, std::enable_shared_from_this<Engine>
{
    // Fixed at setup: by the constructor, or by the loop before it sets setup_done, which start()
    // waits for.
    PublisherLimits limits;
    std::unique_ptr<ucp::Context> context;
    std::unique_ptr<ucp::Worker> worker; // the pointer is fixed; the worker is the loop's
    std::uint32_t request_bytes = 0;
    std::uint64_t max_header = 0;
    bool memory_hooks = true;
    bool setup_ok = false;
    std::promise<void> setup_done;
    // The loop's own, except that a helper thread faults in a new arena's pages (prefaulting).
    // Registered once and faulted in, it holds the active instance's source ring. It is replaced
    // only when a ring needs more, or when an instance that may still read it leaves.
    Region arena;

    // Shared.
    ControlTable control; // callers queue Opens and Closes (handle); the loop answers them
    Wake wake;            // a posted slot or a poke ends the loop's wait
    std::jthread loop;    // started and joined on the caller's thread
    std::atomic<bool> stopping{false}; // stop() sets it; the loop and closed() read it
    // The active instance, for every other thread: the loop sets it.
    mutable std::mutex current_lock;
    std::shared_ptr<Pub> current;
    // A begin, handed from its caller to the loop.
    struct BeginRequest
    {
        Description description;
        PublisherPlan plan;
        std::promise<void> done;
    };
    std::mutex begin_serial; // one begin at a time
    std::mutex begin_lock;   // guards begin_request and begin_closed
    std::shared_ptr<BeginRequest> begin_request;
    bool begin_closed = false; // shutdown has refused what is left
    std::atomic<bool> begin_posted{false};

    // The progress loop's own.
    std::shared_ptr<Pub> active;
    std::vector<std::shared_ptr<Pub>> draining;
    std::int64_t begin_since_ns = 0;
    std::shared_ptr<BeginRequest> making; // taken, waiting for its arena
    std::future<void> prefaulting;        // faulting in a new arena's pages
    bool quarantined = false;
    bool tainted = false; // an instance was quarantined: the worker may still hold its endpoints
    std::shared_ptr<Engine> self_keep;

    // On the threads that use the Publisher.
    explicit Engine(PublisherLimits);
    ~Engine(); // wherever the last reference goes, which may be the loop (stop())
    void start();
    void stop();
    void begin(Description);
    // On any thread.
    void poke(); // another thread made work for the loop
    std::shared_ptr<Pub> instance() const;

    // On the progress loop.
    void run();
    void setup();
    void shutdown();
    bool begin_pass(std::int64_t);
    void make_instance(BeginRequest &);
    bool control_pass();
    bool busy() const;
    Pub *owner_of(std::uint64_t session) const;
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
