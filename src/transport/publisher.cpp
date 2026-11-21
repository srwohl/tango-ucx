// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-ucx project
// SPDX-License-Identifier: LGPL-3.0-or-later
#include "publisher.h"

#include <algorithm>
#include <bit>
#include <cassert>
#include <chrono>
#include <cstring>
#include <format>
#include <stdexcept>
#include <thread>
#include <utility>

namespace TangoUcx::transport
{
namespace
{
void send_cb(void *, ucs_status_t status, void *arg)
{
    auto *operation = static_cast<OpMeta *>(arg);
    auto *publisher = static_cast<Pub *>(operation->owner);
    if(operation->kind == OpKind::Frame)
        publisher->on_send(operation, status);
    else
        publisher->on_control(operation, status);
}
// Puts an operation in flight and returns what UCX needs to run it. The request lives in the
// operation's own storage, and send_cb is called when it completes.
ucp_request_param_t prepare(OpMeta &operation, std::uint32_t flags = 0)
{
    operation.in_flight = true;
    ucp_request_param_t params{};
    params.op_attr_mask = UCP_OP_ATTR_FIELD_REQUEST | UCP_OP_ATTR_FIELD_CALLBACK |
                          UCP_OP_ATTR_FIELD_USER_DATA | UCP_OP_ATTR_FIELD_FLAGS;
    params.flags = flags;
    params.request = &operation;
    params.cb.send = send_cb;
    params.user_data = &operation;
    return params;
}
// UCX does not call send_cb for an operation that completed at once, so it is called here.
void complete_if_done(OpMeta &operation, ucs_status_ptr_t request)
{
    if(!UCS_PTR_IS_PTR(request))
        send_cb(nullptr, UCS_PTR_STATUS(request), &operation);
}
void ep_error(void *arg, ucp_ep_h, ucs_status_t)
{
    auto *row = static_cast<PubRow *>(arg);
    assert(row && row->pub);
    row->pub->retire(row->row, RetireCause::TransportFailed);
}
} // namespace

Pub::Pub(std::shared_ptr<Engine> engine, Description description, PublisherPlan plan)
    : engine(std::move(engine)), description(std::move(description)), plan(plan)
{
    limits = this->engine->limits;
    instance_id = (random64() >> 8) | 1;
}
bool Pub::closed() const
{
    return engine->stopping.load();
}
void Pub::setup()
{
    records.resize(plan.records_bytes);
    slots = std::make_unique<SourceSlot[]>(plan.slot_count);
    if(limits.gpu >= 0)
    {
        std::vector<GpuEvent> events(plan.slot_count);
        source_storage = gpu_storage(limits.gpu, plan.source_bytes, false, events);
        for(std::size_t i = 0; i < events.size(); ++i)
            slots[i].event = events[i];
        gpu_source = Region::adopt(engine->context->get(), source_storage, plan.source_bytes,
                                   UCS_MEMORY_TYPE_CUDA);
        source_base = gpu_source.base();
        source_memh = gpu_source.memh();
    }
    order.resize(plan.slot_count);
    operations.allocate(this, plan, engine->request_bytes);
    grants.reserve(plan.slot_count);
    pull.range_pullers.assign(plan.slot_count + 2, 0); // grants outstanding span fewer ranges
    for(std::uint32_t row_index = 0; row_index < plan.row_count; ++row_index)
    {
        rows[row_index].pub = this;
        rows[row_index].row = std::uint8_t(row_index);
    }
    publish_health(); // placement is available even before the first session opens
}
bool Pub::next_posted() const
{
    return slots_routed < slots_granted &&
           slots[order[slots_routed % plan.slot_count]].posted.load();
}
bool Pub::quiet() const
{
    if(phase.load() != Phase::Ended)
        return false;
    for(std::uint32_t row_index = 0; row_index < plan.row_count; ++row_index)
    {
        const auto &row = rows[row_index];
        if(row.in_use() && (row.in_flight || row.state == SessionState::Ready))
            return false;
    }
    return true;
}
bool Pub::done() const
{
    return phase.load() == Phase::Ended && std::ranges::none_of(rows, &PubRow::in_use);
}
void Pub::retire_all()
{
    for(std::uint32_t row_index = 0; row_index < plan.row_count; ++row_index)
        retire(row_index, RetireCause::InstanceClosing); // a row already retiring stays as it is
}
// source_base and source_memh stay as they are: lent_ring() reads them on another thread, and
// nothing sends from an instance that is done.
void Pub::release_memory() noexcept
{
    gpu_source.unmap();
    kept_arena.unmap();
}
int Pub::find_row(std::uint64_t session) const
{
    for(std::uint32_t row_index = 0; row_index < plan.row_count; ++row_index)
        if(rows[row_index].in_use() && rows[row_index].session == session)
            return int(row_index);
    return -1;
}
// Room for one more grant. An every session has reserved room for every grant not yet routed, a
// puller for those in the ranges it holds.
bool Pub::has_room(const PubRow &row) const
{
    auto reserved = every.has(row.row) ? outstanding() : pull.reserved(*this, row.row);
    return row.ring.has_room(reserved, plan.sizes.frame_bytes);
}
RowSet LatestSessions::takers(const Pub &pub, const SourceSlot &slot, std::int64_t now) const
{
    RowSet taken;
    for(auto row_index : members)
    {
        const auto &row = pub.rows[row_index];
        if(row.in_flight == 0 && now - row.last_send_ns >= row.min_interval_ns &&
           row.ring.fits_now(slot.publication.payload_bytes))
            taken.add(row_index);
    }
    return taken;
}
std::optional<std::uint32_t> EverySessions::blocking(const Pub &pub) const
{
    for(auto row_index : members)
        if(!pub.has_room(pub.rows[row_index]))
            return row_index;
    return {};
}
// The index says which range, and the table which puller: after a pass no rule gives it. None
// for a stream with no pull set, a range not yet taken, or one whose puller has left.
PubRow *PullSet::holder(Pub &pub, std::uint64_t frame_index) const
{
    if(!range_frames || untaken(frame_index))
        return nullptr;
    auto row_index = range_pullers[entry(frame_index)];
    return has(row_index) ? &pub.rows[row_index] : nullptr;
}
// The frames of the range of a frame index that are granted and not yet routed.
std::uint64_t PullSet::granted_in_range(const Pub &pub, std::uint64_t frame_index) const
{
    auto range_start = frame_index - frame_index % range_frames;
    auto first = std::max(pub.next_frame_index, range_start);
    auto last = std::min(pub.next_frame_index + pub.outstanding(), range_start + range_frames);
    return last > first ? last - first : 0;
}
// Whether a puller has room to take a range: rows for all of an untaken range, or for the grants
// of a range left by its puller, and in a packed ring bytes for those grants. At least one of
// each, for the grant being made.
bool PullSet::can_take_range(const Pub &pub, std::uint32_t row, bool untaken,
                             std::uint64_t granted) const
{
    const auto &ring = pub.rows[row].ring;
    auto held = reserved(pub, row);
    auto grants = std::max<std::uint64_t>(granted, 1);
    return ring.rows_free(held, untaken ? range_frames : grants) &&
           ring.bytes_free(held, grants, pub.plan.sizes.frame_bytes);
}
// A range goes to the next puller after the last taker, in the fixed order of the rows, that can
// take it. The range's grants then count against that puller, and those passed over count a
// pass. With no puller able to take it, nothing; with no puller left, an untaken range is
// nobody's, and its frames are missing.
PubRow *PullSet::take_range(Pub &pub, std::uint64_t frame_index)
{
    if(!range_frames)
        return nullptr;
    assert(!holder(pub, frame_index));
    bool fresh = untaken(frame_index);
    auto granted = granted_in_range(pub, frame_index);
    assert(!fresh || granted == 0); // a grant is made only into a taken range
    auto &rows = pub.rows;
    RowSet passed_over;
    for(std::uint32_t step = 1; step <= rows.size(); ++step)
    {
        auto row_index = (last_taker + step) % rows.size();
        if(!has(row_index))
            continue;
        if(!can_take_range(pub, row_index, fresh, granted))
        {
            passed_over.add(row_index);
            continue;
        }
        for(auto passed : passed_over)
            ++rows[passed].passes;
        ranges_taken += fresh;
        last_taker = std::uint8_t(row_index);
        range_pullers[entry(frame_index)] = last_taker;
        return &rows[row_index];
    }
    if(fresh && members.empty())
    {
        range_pullers[entry(frame_index)] = last_taker;
        ++ranges_taken;
    }
    return nullptr;
}
// A puller's reservation is not stored: the grants not yet routed hold the frame indices from
// next_frame_index on, and the range table says whose range each is in. A grant routed, taken
// back or cancelled leaves that window, and a puller that leaves holds no range, so nothing has
// to be released. The order of the calls still matters: a range taken, or a frame routed, at the
// wrong position is a wrong decision, but no count is left behind that disagrees with the grants.
std::uint64_t PullSet::reserved(const Pub &pub, std::uint32_t row) const
{
    if(!range_frames || !has(row))
        return 0;
    auto first = pub.next_frame_index;
    auto end = first + pub.outstanding();
    std::uint64_t held = 0;
    for(auto start = first - first % range_frames; start < end; start += range_frames)
        if(start / range_frames < ranges_taken && range_pullers[entry(start)] == row)
            held += granted_in_range(pub, start);
    return held;
}
// The puller of the next grant's frame index, which is the next one if no slot ahead of it is
// cancelled, has room for the grant, and nothing is returned. Without room the device waits
// for that puller or, when none has room for the range, for the one longest without a frame, and
// that row is returned. With no puller left it does not wait: nothing.
std::optional<std::uint32_t> PullSet::reserve(Pub &pub)
{
    auto frame_index = pub.next_frame_index + pub.outstanding();
    auto *taker = holder(pub, frame_index);
    if(!taker)
        taker = take_range(pub, frame_index);
    if(taker && pub.has_room(*taker))
        return {};
    if(members.empty())
        return {};
    auto *slow = taker;
    if(!slow)
        for(auto row_index : members)
            if(!slow || pub.rows[row_index].last_send_ns < slow->last_send_ns)
                slow = &pub.rows[row_index];
    return slow->row;
}
// The puller the next routed slot goes to. A cancelled slot is not routed here: it uses no frame
// index, so every later grant moves down by one, and the newest granted index leaves the window
// of whoever held it. A frame whose range's puller is
// gone, when no other puller can take the range's grants, goes to any puller with room for one
// more frame. If none has room, it is missing.
// TODO(pull-loss): a puller should never lose frames while the set is healthy, as every does
// not. Two edges lose them today, both when a puller leaves:
//   1. grants it had reserved, when no other puller has room: wait (pressure) instead of
//      counting them missing, and only miss them when no puller is left;
//   2. frames in its ring it never released. Options: keep failing the set, as an every
//      subscriber's death fails it (today); let a puller that closes cleanly receive what it
//      holds before it goes, so only a crash loses frames; or redeliver, at least once, keeping
//      source slots until released (duplicates, non-consecutive ranges, earlier pressure).
// Not decided; update CONTEXT.md's Missing with the choice.
PubRow *PullSet::route(Pub &pub)
{
    auto *to_puller = holder(pub, pub.next_frame_index);
    if(!to_puller)
        to_puller = take_range(pub, pub.next_frame_index);
    if(to_puller)
        return to_puller;
    if(!range_frames)
        return nullptr;
    for(auto row_index : members)
        if(pub.has_room(pub.rows[row_index]))
            return &pub.rows[row_index];
    ++missing;
    return nullptr;
}
// A puller's End reports the set's missing count, so while one puller is sending, the End of
// every puller waits.
RowSet PullSet::end_waits(const Pub &pub) const
{
    for(auto row_index : members)
        if(pub.rows[row_index].in_flight)
            return members;
    return {};
}
// A puller left before it was sent End. Frames it received but did not release are counted as
// missing, because a frame is never sent twice, and the ranges it held are nobody's. With no
// pullers left before the start, the range is cleared: the stream then has no pull set.
void PullSet::remove(const PubRow &row, bool started)
{
    if(members.has(row.row) && row.end != EndNotice::Sent)
        missing += row.ring.unreleased();
    members.remove(row.row);
    if(members.empty() && !started)
        range_frames = 0;
}
std::span<std::byte> Pub::payload(std::uint32_t slot_index)
{
    if(limits.gpu >= 0)
        throw std::logic_error("a GPU source slot uses gpu_payload on its GPU stream");
    if(limits.lend_source)
        throw std::logic_error("a lent ring's slot has no memory: publish_from a lent buffer");
    assert(slot_index < plan.slot_count);
    return {source_at(std::uint64_t(slot_index) * plan.sizes.frame_bytes),
            std::size_t(plan.sizes.frame_bytes)};
}
void *Pub::gpu_payload(std::uint32_t slot_index, GpuStream stream)
{
    if(limits.gpu < 0 || closed())
        throw std::logic_error("gpu_payload needs an active GPU source slot");
    auto &publication = slots[slot_index].publication;
    if(publication.gpu_accessed && publication.gpu_stream.handle != stream.handle)
        throw std::logic_error("all writes to a source slot use its GPU stream");
    publication.gpu_stream = stream;
    publication.gpu_accessed = true;
    return source_at(std::uint64_t(slot_index) * plan.sizes.frame_bytes);
}
std::span<std::byte> Pub::fields(std::uint32_t slot_index)
{
    assert(slot_index < plan.slot_count);
    return {record(slot_index).fields(), plan.sizes.field_bytes};
}
// Marks a filled slot ready to send. Opaque streams pass the frame's length; array streams do not.
void Pub::publish_slot(std::uint32_t slot_index, std::optional<std::uint64_t> timestamp,
                       std::optional<std::size_t> bytes, std::optional<std::uint32_t> buffer,
                       std::function<void()> done)
{
    assert(slot_index < plan.slot_count);
    if(closed())
        throw std::logic_error("the publisher has closed");
    if(buffer.has_value() != limits.lend_source)
        throw std::logic_error(limits.lend_source ? "a lent ring publishes with publish_from"
                                                  : "publish_from needs a lent ring");
    if(buffer && *buffer >= plan.slot_count)
        throw std::out_of_range(
            std::format("lent buffer {}; the ring has {}", *buffer, plan.slot_count));
    if(bytes.has_value() != plan.sizes.opaque)
        throw std::logic_error(plan.sizes.opaque ? "an opaque stream publishes with publish_bytes"
                                                 : "publish_bytes needs an opaque stream");
    if(bytes && (!*bytes || *bytes > plan.sizes.frame_bytes))
        throw std::invalid_argument(std::format("a frame of {} bytes; an opaque frame has 1 to {}",
                                                *bytes, plan.sizes.frame_bytes));
    auto &slot = slots[slot_index];
    auto &publication = slot.publication;
    if(limits.gpu >= 0 && !publication.gpu_accessed)
        throw std::logic_error("a GPU source slot needs gpu_payload before publishing");
    publication.payload_bytes = bytes.value_or(plan.sizes.frame_bytes);
    publication.timestamp =
        timestamp.value_or(std::chrono::duration_cast<std::chrono::nanoseconds>(
                               std::chrono::system_clock::now().time_since_epoch())
                               .count());
    publication.source_offset = std::uint64_t(buffer.value_or(slot_index)) * plan.sizes.frame_bytes;
    publication.done = std::move(done);
    if(publication.gpu_accessed)
        gpu_runtime()->record(slot.event, publication.gpu_stream);
    post(slot);
}
void Pub::hand_back(SourceSlot &slot)
{
    if(!slot.publication.done || slot.routing || slot.sends)
        return;
    auto done = std::move(slot.publication.done);
    slot.publication.done = nullptr;
    done();
}
// Gives a slot back unpublished. It is still posted because the loop takes slots in grant order.
void Pub::cancel_slot(std::uint32_t slot_index)
{
    assert(slot_index < plan.slot_count);
    auto &slot = slots[slot_index];
    auto &publication = slot.publication;
    publication.cancel = true;
    if(publication.gpu_accessed)
        gpu_runtime()->record(slot.event, publication.gpu_stream);
    post(slot);
}
// Hands a slot to the loop, and ends the loop's wait. The store is sequentially consistent, as
// Wake asks.
void Pub::post(SourceSlot &slot)
{
    slot.posted.store(true);
    engine->wake.notify();
}

void Pub::send_control(std::uint32_t row_index, OpKind kind, std::uint8_t message_id,
                       std::span<const std::byte> encoded)
{
    assert(kind == OpKind::Ready || kind == OpKind::End);
    auto &operation = *operations.control(row_index, kind); // each is sent once per session
    assert(!operation.in_flight);
    auto header = operations.header(&operation);
    assert(encoded.size() <= header.size());
    std::memcpy(header.data(), encoded.data(), encoded.size());
    auto &row = rows[row_index];
    ++row.in_flight;
    auto params = prepare(operation);
    complete_if_done(operation, ucp_am_send_nbx(row.ep, message_id, header.data(), encoded.size(),
                                                nullptr, 0, &params));
}
void Pub::send_end(std::uint32_t row_index)
{
    auto &row = rows[row_index];
    wire::EndMessage end_message{row.session, row.ring.sent, next_frame_index, row.reason,
                                 row.pull ? pull.missing : 0};
    auto encoded = wire::encode(end_message);
    send_control(row_index, OpKind::End, wire::End, encoded);
    row.end = EndNotice::Sent;
    if(row.state == SessionState::Ready)
        row.state = SessionState::Ended;
}
// Starts the flush or the close of a retiring row's endpoint. A forced close does not wait for
// the subscriber.
void Pub::start_teardown(std::uint32_t row_index, OpKind kind, bool force)
{
    assert(kind == OpKind::Flush || kind == OpKind::Close);
    assert(!force || kind == OpKind::Close);
    auto &operation = *operations.control(row_index, kind); // teardown starts each step once
    assert(!operation.in_flight);
    auto &row = rows[row_index];
    row.teardown = kind == OpKind::Flush ? Teardown::Flushing : Teardown::Closing;
    auto params = prepare(operation, force ? UCP_EP_CLOSE_FLAG_FORCE : 0);
    complete_if_done(operation, kind == OpKind::Flush ? ucp_ep_flush_nbx(row.ep, &params)
                                                      : ucp_ep_close_nbx(row.ep, &params));
}

void Pub::send_frame(std::uint32_t slot_index, std::uint32_t row_index, std::int64_t now)
{
    auto &row = rows[row_index];
    auto &slot = slots[slot_index];
    const auto &publication = slot.publication;
    auto &operation = *operations.frame(slot_index, row_index);
    auto frame_record = record(slot_index);
    wire::FrameMessage frame_message{};
    frame_message.session = row.session;
    frame_message.sequence = row.ring.sent;
    frame_message.payload_bytes = publication.payload_bytes;
    frame_message.packed = row.ring.packed(); // the publisher places the frame, and says where
    frame_message.position = row.ring.next_position(publication.payload_bytes);
    frame_message.index = frame_record.index();
    frame_message.timestamp_ns = frame_record.timestamp();
    frame_message.record_bytes = plan.sizes.padded_field_bytes;
    std::memcpy(frame_message.record.data(), frame_record.fields(), frame_message.record_bytes);
    auto header = operations.header(&operation);
    auto header_bytes = wire::encode_into(header, frame_message);
    assert(header_bytes); // the plan sizes each header for the largest Frame
    operation.sent_ns = now;
    row.last_send_ns = now;
    row.ring.record_send(frame_message.position, publication.payload_bytes);
    ++row.in_flight;
    ++slot.sends;
    // An eager frame cannot land in GPU memory.
    auto params = prepare(operation, row.gpu || limits.gpu >= 0 ? UCP_AM_SEND_FLAG_RNDV : 0);
    params.op_attr_mask |= UCP_OP_ATTR_FIELD_MEMH | UCP_OP_ATTR_FIELD_MEMORY_TYPE;
    params.memh = source_memh;
    params.memory_type = limits.gpu >= 0 ? UCS_MEMORY_TYPE_CUDA : UCS_MEMORY_TYPE_HOST;
    complete_if_done(operation, ucp_am_send_nbx(row.ep, wire::Frame, header.data(), header_bytes,
                                                source_at(publication.source_offset),
                                                publication.payload_bytes, &params));
}
// UCX calls a completion once, and never for an operation that completed at once, which
// complete_if_done completes instead: each completes once.
void Pub::on_send(OpMeta *operation, ucs_status_t status)
{
    assert(operation->in_flight);
    operation->in_flight = false;
    std::uint32_t row_index = operation->row;
    std::uint32_t slot_index = operation->slot;
    assert(slot_index < plan.slot_count);
    auto &row = rows[row_index];
    count_down(row.in_flight);
    if(count_down(slots[slot_index].sends) == 0)
        hand_back(slots[slot_index]);
    auto transfer_ns = now_ns() - operation->sent_ns;
    row.transfer_max_ns = std::max(row.transfer_max_ns, transfer_ns);
    row.transfer_sum_ns += transfer_ns;
    if(status != UCS_OK && status != UCS_ERR_CANCELED)
        retire(row_index,
               status == UCS_ERR_REJECTED ? RetireCause::Protocol : RetireCause::TransportFailed);
}
void Pub::on_control(OpMeta *operation, ucs_status_t status)
{
    assert(operation->in_flight);
    operation->in_flight = false;
    auto row_index = operation->row;
    auto &row = rows[row_index];
    switch(operation->kind)
    {
        case OpKind::Ready:
        case OpKind::End:
            count_down(row.in_flight);
            if(status != UCS_OK && status != UCS_ERR_CANCELED && row.live())
                retire(row_index, RetireCause::TransportFailed);
            return;
        case OpKind::Flush:
            if(row.teardown == Teardown::Flushing) // a forced close may have overtaken it
                row.teardown = Teardown::Flushed;
            return;
        case OpKind::Close:
            row.teardown = Teardown::Closed;
            row.ep = nullptr;
            return;
        case OpKind::Frame:
            return;
    }
}
namespace
{
// What a cause of retirement means for the session (docs/diagrams.md, section 6): the reason its
// End gives, whether it is sent End at all, and whether it counts as failed, which UcxFailedEvery
// reports for the sessions that hold the device.
struct Retirement
{
    wire::Reason reason;
    bool end_due;
    bool failed;
};
constexpr Retirement retirement(RetireCause cause)
{
    switch(cause)
    {
        case RetireCause::CloseCommand:
        case RetireCause::Abandoned:
            return {.reason = wire::Closing, .end_due = false, .failed = false};
        case RetireCause::TransportFailed: // nothing can reach the subscriber
            return {.reason = wire::Closing, .end_due = false, .failed = true};
        case RetireCause::Protocol:
        case RetireCause::SourceFailed:
            return {.reason = wire::Protocol, .end_due = true, .failed = true};
        case RetireCause::Evicted:
            return {.reason = wire::Evicted, .end_due = true, .failed = true};
        case RetireCause::InstanceClosing: // the subscriber learns why its session ended
            return {.reason = wire::Closing, .end_due = true, .failed = false};
    }
    return {.reason = wire::Closing, .end_due = false, .failed = false};
}
} // namespace
// A live session leaves: it stops receiving frames, holds no room any more, and its row is
// torn down by retire_pass. A row already retiring, or free, is left as it is.
void Pub::retire(std::uint32_t row_index, RetireCause cause)
{
    assert(row_index < plan.row_count);
    auto &row = rows[row_index];
    if(!row.live())
        return;
    bool held_device = (every.members | pull.members).has(row_index);
    ready_rows.remove(row_index);
    every.remove(row_index);
    latest.remove(row_index);
    pull.remove(row, phase.was_started());
    auto effect = retirement(cause);
    if(effect.failed && held_device)
        ++failed_every;
    row.state = SessionState::Retiring;
    row.reason = effect.reason;
    row.retired_ns = now_ns();
    if(row.end != EndNotice::Sent)
        row.end = effect.end_due ? EndNotice::Due : EndNotice::NotDue;
    count_holding();
}
void Pub::count_holding()
{
    holding.store((every.members | pull.members).size(), std::memory_order_release);
}
// Why an Open is refused, or Ok. The checks run in order and the first that fails decides.
wire::Status Pub::check_open(const wire::OpenMessage &open_message) const
{
    // An every subscription or a puller can make the device wait.
    bool holds_device = open_message.delivery != wire::Delivery::Latest;
    bool is_puller = open_message.delivery == wire::Delivery::Pull;
    if(open_message.instance_id != instance_id)
        return wire::InstanceId;
    auto current = phase.load();
    if(current >= Phase::Finishing)
        return wire::Finished;
    if(holds_device && !limits.allow_every)
        return wire::EveryRefused;
    if(holds_device && current == Phase::Started)
        return wire::Started; // every and pull must join before the start
    if(find_row(open_message.session) >= 0)
        return wire::Malformed; // the session id is already in use
    if(plan.sizes.opaque && open_message.ring_bytes < 2 * plan.sizes.frame_bytes)
        return wire::Malformed; // a packed ring must hold two of the largest frame
    if(is_puller && open_message.range % open_message.batch)
        return wire::Range; // a range must be whole batches
    if(is_puller && open_message.range > open_message.depth)
        return wire::Range; // a range must fit the ring
    if(is_puller && !pull.members.empty() && open_message.range != pull.range_frames)
        return wire::Range; // every puller of a set has the same range
    return wire::Ok;
}
void Pub::close(ControlEntry &entry)
{
    int row_index = find_row(entry.in.close.session);
    if(row_index < 0 || rows[row_index].close_reply)
    {
        engine->control.reply(entry, {wire::UnknownSession});
        return;
    }
    rows[row_index].close_reply = &entry;
    retire(row_index, RetireCause::CloseCommand);
}
void Pub::open(ControlEntry &entry)
{
    auto &control = engine->control;
    auto &worker = engine->worker;
    const auto &open_message = *entry.in.open;
    auto status = check_open(open_message);
    std::uint32_t row_index = 0;
    while(row_index < plan.row_count && rows[row_index].in_use())
        ++row_index;
    if(status == wire::Ok && row_index == plan.row_count)
        status = wire::NoSession;
    if(status != wire::Ok)
    {
        control.reply(entry, {status});
        return;
    }
    auto &row = rows[row_index];
    row = PubRow{};
    row.pub = this;
    row.row = std::uint8_t(row_index);
    row.ordinal = ++ordinals;
    row.every = open_message.delivery == wire::Delivery::Every;
    row.pull = open_message.delivery == wire::Delivery::Pull;
    row.ring.depth = open_message.depth;
    row.ring.bytes = plan.sizes.opaque ? open_message.ring_bytes : 0;
    row.gpu = open_message.gpu;
    row.session = open_message.session;
    row.min_interval_ns =
        std::int64_t(std::min<std::uint64_t>(open_message.min_interval_ns, INT64_MAX));
    row.label.set({open_message.label.data(), open_message.label_bytes});
    try
    {
        row.ep = worker->endpoint({open_message.address.data(), open_message.address_bytes}, &row,
                                  ep_error);
    }
    catch(const std::exception &error)
    {
        wire::Reply refused{wire::Endpoint};
        std::string_view why = error.what();
        refused.detail_bytes = std::uint8_t(std::min(why.size(), refused.detail.size()));
        std::memcpy(refused.detail.data(), why.data(), refused.detail_bytes);
        control.reply(entry, refused);
        return;
    }
    // The subscriber set up its ring and handlers before it sent Open, so it is ready now.
    row.state = SessionState::Ready;
    row.transport.n = std::uint8_t(worker->transports(row.ep, row.transport.c));
    row.network_devices = endpoint_network_devices(row.ep);
    last_topology_ns = 0;
    ready_rows.add(row_index);
    switch(open_message.delivery)
    {
        case wire::Delivery::Every:
            every.add(row_index);
            break;
        case wire::Delivery::Pull:
            pull.add(row_index, open_message.range);
            break;
        case wire::Delivery::Latest:
            latest.add(row_index);
            break;
    }
    count_holding(); // before the reply, so a subscriber that has its reply is counted
    auto encoded = wire::encode(wire::SessionMessage{row.session});
    send_control(row_index, OpKind::Ready, wire::Ready, encoded);
    wire::Reply reply{wire::Ok};
    auto address = worker->address();
    reply.address_bytes = std::uint16_t(address.size());
    std::memcpy(reply.address.data(), address.data(), address.size());
    // The subscriber gave up waiting for the reply, so nobody will use the session.
    if(!control.reply(entry, reply))
        retire(row_index, RetireCause::Abandoned);
}
// Takes back the grants the device never picked up, after finish(). The grant ring is first in,
// first out, so the grants still in it are the newest. Their slots become free, and their frame
// indices leave the window, so no session holds room for them.
void Pub::unissue_grants()
{
    // The grants the device has not taken. It may take one meanwhile: that one stays granted.
    std::uint32_t queued = 0;
    for(int slot_index; (slot_index = grants.try_pop()) >= 0; ++queued)
        slots[slot_index].granted = false;
    slots_granted -= queued;
}

// Every and latest sessions are all ready sessions.
void Pub::check_members() const
{
    assert((every.members | latest.members).without(ready_rows).empty());
}
// Routes the posted slots in grant order and, once a finishing instance has routed every slot,
// ends it and sends its sessions End.
bool Pub::route_pass(std::int64_t now)
{
    if(phase.load() >= Phase::Finishing)
        unissue_grants();
    bool work = false;
    while(slots_routed < slots_granted && route_next(now))
        work = true;
    if(phase.load() == Phase::Finishing && slots_routed == slots_granted)
        phase.end();
    if(phase.load() == Phase::Ended)
        send_ends();
    check_members();
    return work;
}
// Routes the slot of the oldest grant not yet routed, once the device has posted it and its
// writes are done. False while it waits for either.
bool Pub::route_next(std::int64_t now)
{
    auto slot_index = order[slots_routed % plan.slot_count];
    auto &slot = slots[slot_index];
    if(!slot.posted.load(std::memory_order_acquire))
        return false;
    auto source = source_state(slot);
    if(source == GpuEventState::Pending)
        return false;
    if(source == GpuEventState::Failed)
        fail_source(slot);
    slot.posted.store(false, std::memory_order_relaxed);
    slot.granted = false;
    // A cancelled slot uses no frame index, and goes to no puller.
    auto *to_puller = slot.publication.cancel ? nullptr : pull.route(*this);
    ++slots_routed;
    if(!slot.publication.cancel)
        route_frame(slot_index, to_puller, now);
    return true;
}
// Whether the device's writes to a posted slot are done: a host source's are, and a GPU source's
// once the event recorded on its stream has completed, or failed.
GpuEventState Pub::source_state(const SourceSlot &slot) const
{
    if(!slot.publication.gpu_accessed)
        return GpuEventState::Done;
    return gpu_runtime()->query(slot.event);
}
// A GPU source event failed, so the slot's payload cannot be trusted: its frame is sent nowhere,
// the instance finishes, and every live session retires.
void Pub::fail_source(SourceSlot &slot)
{
    source_failed = true;
    phase.finish();
    slot.publication.cancel = true;
    for(std::uint32_t row_index = 0; row_index < plan.row_count; ++row_index)
        if(rows[row_index].live())
            retire(row_index, RetireCause::SourceFailed);
}
// Gives a published frame the next frame index and sends it to the every sessions, the latest
// sessions that take it, and the puller of its range. The other latest sessions skip it.
void Pub::route_frame(std::uint32_t slot_index, PubRow *to_puller, std::int64_t now)
{
    auto &slot = slots[slot_index];
    auto slot_record = record(slot_index);
    slot_record.set_index(next_frame_index);
    slot_record.set_timestamp(slot.publication.timestamp);
    ++next_frame_index;
    // Every sessions and the puller had room reserved when the slot was granted.
    auto takers = latest.takers(*this, slot, now);
    auto recipients = every.members | takers;
    if(to_puller && to_puller->live())
        recipients.add(to_puller->row);
    for(auto skipper : latest.members.without(takers))
    {
        ++skipped;
        ++rows[skipper].skipped;
    }
    // A send may complete at once: the buffer goes back only after the last is issued.
    slot.routing = true;
    for(auto recipient : recipients)
        send_frame(slot_index, recipient, now);
    slot.routing = false;
    hand_back(slot);
}
// Sends End to each ready session of an ended instance once it has nothing in flight. A puller's
// End waits for the whole set (PullSet::end_waits).
void Pub::send_ends()
{
    auto end_waits = pull.end_waits(*this);
    for(std::uint32_t row_index = 0; row_index < plan.row_count; ++row_index)
    {
        auto &row = rows[row_index];
        if(row.state == SessionState::Ready && row.in_flight == 0 && !end_waits.has(row_index))
            send_end(row_index);
    }
}
bool Pub::grant_pass()
{
    if(phase.load() != Phase::Started)
        return false;
    bool work = false;
    blocked.reset();
    for(;;)
    {
        int slot_index = -1;
        for(std::uint32_t i = 0; i < plan.slot_count; ++i)
        {
            auto candidate = (scan + i) % plan.slot_count;
            if(slots[candidate].state() == SlotState::Free)
            {
                slot_index = candidate;
                scan = (candidate + 1) % plan.slot_count;
                break;
            }
        }
        if(slot_index < 0)
            break;
        // The every sessions gate first. The pull set is asked only if they have room, because it
        // may give a range to a puller as it answers.
        blocked = every.blocking(*this);
        if(!blocked)
            blocked = pull.reserve(*this);
        if(blocked)
            break;
        // Each grant starts a fresh publication, which the device fills once it acquires the
        // slot. It is cleared before the push, which hands the slot to the device.
        slots[slot_index].publication = {};
        slots[slot_index].granted = true;
        order[slots_granted % plan.slot_count] = slot_index;
        grants.push(slot_index);
        ++slots_granted;
        work = true;
    }
    check_members();
    return work;
}
bool Pub::retire_pass(std::int64_t now)
{
    bool work = false;
    for(std::uint32_t row_index = 0; row_index < plan.row_count; ++row_index)
    {
        auto &row = rows[row_index];
        if(row.state != SessionState::Retiring)
            continue;
        if(row.end == EndNotice::Due && row.in_flight == 0 && row.ep)
        {
            send_end(row_index);
            work = true;
            continue;
        }
        bool late = now - row.retired_ns > k_close_ns;
        bool unclosed = row.ep && row.teardown < Teardown::Closing;
        bool idle = row.in_flight == 0;
        // A flush follows the row's last send and a close follows the flush. A late row is
        // closed by force, whatever it still has in flight.
        if(unclosed && (late || (row.teardown == Teardown::Flushed && idle)))
        {
            start_teardown(row_index, OpKind::Close, late);
            work = true;
        }
        else if(unclosed && row.teardown == Teardown::None && idle)
        {
            start_teardown(row_index, OpKind::Flush);
            work = true;
        }
        if(row.teardown == Teardown::Closed && row.in_flight == 0 &&
           !operations.control(row_index, OpKind::Flush)->in_flight &&
           !operations.control(row_index, OpKind::Close)->in_flight)
        {
            if(row.close_reply)
            {
                engine->control.reply(*row.close_reply, {wire::Ok});
                row.close_reply = nullptr;
            }
            row.state = SessionState::Free;
            row.end = EndNotice::NotDue;
            row.teardown = Teardown::None;
            work = true;
        }
        else if(row.teardown >= Teardown::Closing &&
                now - row.retired_ns > k_close_ns + k_quarantine_ns)
        {
            quarantine_owner();
            return work;
        }
    }
    return work;
}
// The row the device is waiting for, if it is waiting: it has taken every grant, and the last
// grant pass stopped at this row although a slot was free. That is the pressure Health reports.
// An idle device has a grant to take, and after finish() the device waits for no grant.
std::optional<std::uint32_t> Pub::pressure_row() const
{
    if(phase.load() != Phase::Started || !grants.empty())
        return {};
    return blocked;
}
// Eviction counts only while the device waits: every every session without room, and of a set
// only the puller the device waits for. A puller that is passed over holds no pressure.
void Pub::timer_pass(std::int64_t now)
{
    auto pressure = pressure_row();
    auto timeout = std::chrono::nanoseconds(limits.pressure_timeout).count();
    for(std::uint32_t row_index = 0; row_index < plan.row_count; ++row_index)
    {
        auto &row = rows[row_index];
        bool blocking =
            pressure &&
            (every.has(row_index) ? !has_room(row) : pull.has(row_index) && *pressure == row_index);
        if(!blocking)
            row.blocked_since_ns = 0;
        else if(!row.blocked_since_ns)
            row.blocked_since_ns = now;
        else if(timeout && now - row.blocked_since_ns > timeout)
            retire(row_index, RetireCause::Evicted);
    }
    publish_health();
}
void Pub::publish_health()
{
    auto now = now_ns();
    if(!last_topology_ns || now - last_topology_ns >= 1'000'000'000LL)
    {
        last_topology_ns = now;
        sample_topology(topology, limits.gpu >= 0 ? nullptr : source_base, plan.source_bytes);
        topology.network_devices.clear();
        for(const auto &row : rows)
            if(row.in_use())
                for(const auto &device : row.network_devices)
                    if(!std::ranges::any_of(topology.network_devices, [&](const auto &known)
                                            { return known.name == device.name; }))
                        topology.network_devices.push_back(device);
        topology_copy.write(topology);
    }
    Observation observation;
    observation.frames = next_frame_index;
    observation.skipped = skipped;
    observation.missing = pull.missing;
    observation.failed_every = failed_every;
    observation.source_failed = source_failed;
    auto pressure = pressure_row();
    observation.pressure_from = pressure ? rows[*pressure].ordinal : 0;
    for(const auto &slot : std::span(slots.get(), plan.slot_count))
    {
        observation.slots_free += slot.state() == SlotState::Free;
        observation.slots_held += slot.state() == SlotState::Granted;
    }
    observation.memory_hooks = engine->memory_hooks;
    for(const auto &row : rows)
        if(row.in_use())
            observation.rows[observation.count++] = {
                row.ordinal,
                row.ring.depth,
                std::uint32_t(row.ring.room(plan.sizes.frame_bytes)),
                row.every,
                row.state == SessionState::Retiring,
                row.ring.sent,
                row.skipped,
                row.transfer_max_ns,
                row.ring.sent ? row.transfer_sum_ns / std::int64_t(row.ring.sent) : 0,
                row.label,
                row.transport,
                row.pull,
                row.passes};
    health_copy.write(observation);
}
void Pub::quarantine_owner()
{
    if(quarantined)
        return;
    quarantined = true;
    engine->tainted = true;
    self_keep = shared_from_this();
    quarantine().keep(*this, plan.charge);
}
} // namespace TangoUcx::transport

namespace TangoUcx
{
Slot::Slot(std::shared_ptr<transport::Pub> pub, std::uint32_t index)
    : state_(std::move(pub)), index_(index), active_(true)
{
}
Slot::Slot(Slot &&other) noexcept
    : state_(std::move(other.state_)), index_(other.index_),
      active_(std::exchange(other.active_, false))
{
}
Slot &Slot::operator=(Slot &&other) noexcept
{
    if(this != &other)
    {
        if(active_)
            if(auto pub = state_)
                pub->cancel_slot(index_);
        state_ = std::move(other.state_);
        index_ = other.index_;
        active_ = std::exchange(other.active_, false);
    }
    return *this;
}
Slot::~Slot()
{
    if(active_)
        if(auto pub = state_)
            pub->cancel_slot(index_);
}
std::span<std::byte> Slot::payload()
{
    if(auto pub = active_ ? state_ : nullptr; pub && !pub->closed())
        return pub->payload(index_);
    return {};
}
std::span<std::byte> Slot::fields()
{
    if(auto pub = active_ ? state_ : nullptr; pub && !pub->closed())
        return pub->fields(index_);
    return {};
}
void Slot::publish(std::optional<std::uint64_t> timestamp) &&
{
    if(!active_)
        throw std::logic_error("slot already published");
    if(auto pub = state_)
        pub->publish_slot(index_, timestamp, {});
    active_ = false;
    state_.reset();
}
void *Slot::gpu_payload(GpuStream stream)
{
    if(!active_ || !state_)
        throw std::logic_error("gpu_payload needs an active GPU source slot");
    return state_->gpu_payload(index_, stream);
}
std::size_t Slot::payload_bytes() const
{
    if(!active_ || !state_)
        throw std::logic_error("payload_bytes needs an active source slot");
    return state_->plan.sizes.frame_bytes;
}
void Slot::publish_bytes(std::size_t bytes, std::optional<std::uint64_t> timestamp) &&
{
    if(!active_)
        throw std::logic_error("slot already published");
    if(auto pub = state_)
        pub->publish_slot(index_, timestamp, bytes);
    active_ = false;
    state_.reset();
}
void Slot::publish_from(std::uint32_t buffer, std::function<void()> done,
                        std::optional<std::size_t> bytes, std::optional<std::uint64_t> timestamp) &&
{
    if(!active_)
        throw std::logic_error("slot already published");
    if(auto pub = state_)
        pub->publish_slot(index_, timestamp, bytes, buffer, std::move(done));
    active_ = false;
    state_.reset();
}
Publisher::Publisher(PublisherLimits limits)
    : p_(std::make_shared<transport::Engine>(std::move(limits)))
{
    p_->start();
}
// Constructed once the delegated constructor returns: a refused description stops the engine.
Publisher::Publisher(Description description, PublisherLimits limits) : Publisher(std::move(limits))
{
    begin(std::move(description));
}
Publisher::Publisher(Publisher &&) noexcept = default;
Publisher &Publisher::operator=(Publisher &&other) noexcept
{
    if(this != &other)
    {
        if(p_)
            p_->stop();
        p_ = std::move(other.p_);
    }
    return *this;
}
Publisher::~Publisher()
{
    if(p_)
        p_->stop();
}
void Publisher::begin(Description description)
{
    if(!p_)
        throw std::logic_error("moved publisher");
    p_->begin(std::move(description));
}
std::optional<Slot> Publisher::acquire(Deadline deadline)
{
    auto pub = p_ ? p_->instance() : nullptr;
    if(!pub || pub->phase.load() >= transport::Phase::Finishing)
        return {};
    int slot_index = pub->grants.pop_until(
        deadline,
        [&] { return pub->phase.load() >= transport::Phase::Finishing || pub->closed(); });
    if(slot_index < 0)
        return {};
    return Slot(std::move(pub), slot_index);
}
// No slot is granted before this, and no every Open is accepted after it.
void Publisher::ucx_start()
{
    if(auto pub = p_ ? p_->instance() : nullptr)
    {
        pub->phase.start();
        p_->poke();
    }
}
// The wait polls an exact count the progress thread keeps, and gives up at the deadline. A
// session may still fail between the count and the start: UcxFailedEvery reports it.
bool Publisher::ucx_start(std::uint32_t every, Deadline deadline)
{
    auto pub = p_ ? p_->instance() : nullptr;
    if(!pub)
        return false;
    while(pub->phase.load() == transport::Phase::Before)
    {
        if(pub->holding.load(std::memory_order_acquire) >= every)
        {
            if(!pub->phase.start()) // a finish won the race
                return false;
            p_->poke();
            return true;
        }
        if(std::chrono::steady_clock::now() >= deadline)
            return false;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return pub->phase.was_started();
}
void Publisher::finish()
{
    if(auto pub = p_ ? p_->instance() : nullptr)
    {
        pub->phase.finish();
        pub->grants.signal();
        p_->poke();
    }
}
std::span<std::byte> Publisher::lent_ring() const
{
    // A finishing instance stays current while the next is made, and the loop may replace the
    // arena its ring is in meanwhile.
    auto pub = p_ ? p_->instance() : nullptr;
    if(!pub || !pub->limits.lend_source || pub->phase.load() >= transport::Phase::Finishing)
        return {};
    return {pub->source_base, std::size_t(pub->plan.source_bytes)};
}
Health Publisher::health() const
{
    if(!p_)
        return {};
    auto pub = p_->instance();
    if(!pub)
        return {.quarantined_bytes = transport::quarantine().total.load(),
                .memory_hooks = p_->memory_hooks};
    auto observation = pub->health_copy.read();
    Health result{.frames = observation.frames,
                  .skipped = observation.skipped,
                  .failed_every = observation.failed_every,
                  .pressure = observation.pressure_from != 0,
                  .pressure_from = observation.pressure_from,
                  .slots_free = observation.slots_free,
                  .slots_held = observation.slots_held,
                  .quarantined_bytes = transport::quarantine().total.load(),
                  .memory_hooks = observation.memory_hooks,
                  .started = pub->phase.was_started(),
                  .topology = pub->topology_copy.read()};
    if(observation.source_failed)
        result.failure = "a GPU source event failed; its memory cannot be reused";
    for(const auto &row : std::span(observation.rows).first(observation.count))
    {
        if(!row.retiring)
            ++(row.every || row.pull ? result.every
                                     : result.latest); // a puller holds the device as every does
        result.transfer_max_ns =
            std::max<std::uint64_t>(result.transfer_max_ns, row.transfer_max_ns);
        result.transfer_mean_ns =
            std::max<std::uint64_t>(result.transfer_mean_ns, row.transfer_mean_ns);
        if(row.transport.n && result.transport.find(row.transport.view()) == std::string::npos)
            result.transport +=
                (result.transport.empty() ? "" : ";") + std::string(row.transport.view());
    }
    return result;
}
Description Publisher::description() const
{
    if(!p_)
        throw std::logic_error("moved publisher");
    auto pub = p_->instance();
    if(!pub)
        throw std::logic_error("no publisher instance: begin has not been called");
    return pub->description;
}
} // namespace TangoUcx

namespace TangoUcx::transport
{
std::string handle(Publisher &publisher, std::string_view document)
{
    wire::Request request;
    auto status = wire::parse(document, request);
    if(status != wire::Ok || !publisher.p_)
        return wire::format(wire::Reply{status != wire::Ok ? status : wire::Finished});
    return wire::format(publisher.p_->control.call(request, std::chrono::steady_clock::now() +
                                                                std::chrono::seconds(3)));
}
namespace
{
std::shared_ptr<Pub> required_instance(const std::shared_ptr<Engine> &p)
{
    if(!p)
        throw std::logic_error("moved publisher");
    auto pub = p->instance();
    if(!pub)
        throw std::runtime_error("the publisher has no instance: none has begun, or the next "
                                 "is being made");
    return pub;
}
} // namespace
std::string ucx_stream(const Publisher &publisher)
{
    auto pub = required_instance(publisher.p_);
    return wire::format(wire::StreamMessage{pub->instance_id, pub->description,
                                            pub->limits.allow_every, pub->limits.max_sessions});
}
// Formatted on the calling thread, from the loop's fixed Health copy.
std::string ucx_sessions(const Publisher &publisher)
{
    auto pub = required_instance(publisher.p_);
    auto observation = pub->health_copy.read();
    wire::SessionsMessage sessions_message{pub->instance_id, {}, observation.missing};
    for(const auto &row : std::span(observation.rows).first(observation.count))
        sessions_message.sessions.push_back(
            {row.ordinal, std::string(row.label.view()), row.every, row.retiring, row.depth,
             row.room, row.sent, row.skipped, std::uint64_t(row.transfer_max_ns) / 1000,
             std::string(row.transport.view()), row.pull, row.passes});
    return wire::format(sessions_message);
}
std::string ucx_topology(const Publisher &publisher)
{
    return wire::format(publisher.health().topology);
}
} // namespace TangoUcx::transport
