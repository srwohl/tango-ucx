// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-ucx project
// SPDX-License-Identifier: LGPL-3.0-or-later
#include "publisher.h"

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstring>
#include <format>
#include <stdexcept>
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
void ep_error(void *arg, ucp_ep_h, ucs_status_t status)
{
    auto *row = static_cast<PubRow *>(arg);
    if(row && row->pub)
    {
        if(row->transport_error == UCS_OK)
            row->transport_error = status;
        row->pub->retire(row->row, wire::Closing, true);
    }
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
    if(!ended)
        return false;
    for(std::uint32_t row_index = 0; row_index < plan.row_count; ++row_index)
    {
        const auto &row = rows[row_index];
        if(row.used && (row.in_flight || (row.ready && !row.end_sent)))
            return false;
    }
    return true;
}
bool Pub::done() const
{
    return ended && std::ranges::none_of(rows, &PubRow::used);
}
void Pub::retire_all()
{
    for(std::uint32_t row_index = 0; row_index < plan.row_count; ++row_index)
        if(rows[row_index].used && !rows[row_index].closing)
        {
            retire(row_index, wire::Closing, false);
            rows[row_index].end_due = true; // the subscriber learns why its session ended
        }
}
void Pub::release_memory() noexcept
{
    source_base = nullptr;
    source_memh = nullptr;
    gpu_source.unmap();
    kept_arena.unmap();
}
std::uint64_t Pub::id_for(std::uint32_t row_index) const
{
    return rows[row_index].session;
}
int Pub::find_row(std::uint64_t session) const
{
    for(std::uint32_t row_index = 0; row_index < plan.row_count; ++row_index)
        if(rows[row_index].used && rows[row_index].session == session)
            return int(row_index);
    return -1;
}
// Room for more frames, some of them granted now: rows of the receive tables for the frames
// and, in a packed ring, the bytes for the grants. One of each is room for one more grant.
bool Pub::has_room(const PubRow &row, std::uint64_t frames, std::uint64_t grants) const
{
    auto reserved = every.has(row.row) ? outstanding() : pull.reserved[row.row];
    return row.sent + reserved - row.credited + frames <= row.ring_depth &&
           (!row.ring_bytes || ring_room(row.sent_bytes - row.credited_bytes, reserved + grants - 1,
                                         plan.sizes.frame_bytes, row.ring_bytes));
}
std::uint64_t LatestSessions::takers(const Pub &pub, const SourceSlot &slot, std::int64_t now) const
{
    std::uint64_t taken = 0;
    for(auto left = mask; left; left &= left - 1)
    {
        auto row_index = std::countr_zero(left);
        const auto &row = pub.rows[row_index];
        if(row.sent - row.credited < row.ring_depth && row.in_flight == 0 &&
           now - row.last_send_ns >= row.min_interval_ns &&
           (!row.ring_bytes || place(row.sent_bytes, slot.payload_bytes, row.ring_bytes) +
                                       slot.payload_bytes - row.credited_bytes <=
                                   row.ring_bytes))
            taken |= std::uint64_t{1} << row_index;
    }
    return taken;
}
int EverySessions::blocking(const Pub &pub) const
{
    for(auto left = mask; left; left &= left - 1)
        if(auto row_index = std::countr_zero(left); !pub.has_room(pub.rows[row_index]))
            return row_index;
    return -1;
}
// The puller that holds the range of a frame index, or none, as in a stream with no pull set.
// The index says which range, and the table which puller: after a pass no rule gives it. A
// range's first grant gives the range to the next puller in the fixed order that has room for
// all of it. A range whose puller has gone goes to the next with room for its grants
// outstanding, which that puller then reserves.
PubRow *PullSet::puller(Pub &pub, std::uint64_t frame_index)
{
    if(!range_frames)
        return nullptr;
    auto &rows = pub.rows;
    auto range_start = frame_index - frame_index % range_frames;
    auto &holder = range_pullers[frame_index / range_frames % range_pullers.size()];
    bool untaken = frame_index / range_frames == ranges_taken;
    if(!untaken && has(holder))
        return &rows[holder];
    auto first = std::max(pub.next_frame_index, range_start),
         last = std::min(pub.next_frame_index + pub.outstanding(), range_start + range_frames);
    std::uint64_t outstanding = last > first ? last - first : 0, passed_over = 0;
    for(std::uint32_t step = 1; step <= rows.size(); ++step)
    {
        auto row_index = (last_taker + step) % rows.size();
        if(!has(row_index))
            continue;
        if(pub.has_room(rows[row_index],
                        untaken ? range_frames : std::max<std::uint64_t>(outstanding, 1),
                        std::max<std::uint64_t>(outstanding, 1)))
        {
            for(; passed_over; passed_over &= passed_over - 1)
                ++rows[std::countr_zero(passed_over)].passes;
            reserved[row_index] += std::uint32_t(outstanding);
            ranges_taken += untaken;
            return &rows[holder = last_taker = std::uint8_t(row_index)];
        }
        passed_over |= std::uint64_t{1} << row_index;
    }
    if(untaken && !mask) // no puller is left: the range is nobody's, and its frames missing
        holder = last_taker, ++ranges_taken;
    return nullptr;
}
// The grants taken back are the newest, so they would have had the last frame indices. Their
// pullers release what they reserved for them.
void PullSet::take_back(Pub &pub, std::uint32_t grants)
{
    auto newest = pub.next_frame_index + pub.outstanding() - 1;
    for(std::uint32_t back = 0; back < grants; ++back)
        if(auto *holder = puller(pub, newest - back); holder && reserved[holder->row])
            --reserved[holder->row];
}
std::span<std::byte> Pub::payload(std::uint32_t slot_index)
{
    if(limits.gpu >= 0)
        throw std::logic_error("a GPU source slot uses gpu_payload on its GPU stream");
    if(slot_index >= plan.slot_count)
        throw std::out_of_range("source slot");
    return {source_at(std::uint64_t(slot_index) * plan.sizes.frame_bytes),
            std::size_t(plan.sizes.frame_bytes)};
}
void *Pub::gpu_payload(std::uint32_t slot_index, GpuStream stream)
{
    if(limits.gpu < 0 || closed())
        throw std::logic_error("gpu_payload needs an active GPU source slot");
    auto &slot = slots[slot_index];
    if(slot.gpu_accessed && slot.gpu_stream.handle != stream.handle)
        throw std::logic_error("all writes to a source slot use its GPU stream");
    slot.gpu_stream = stream;
    slot.gpu_accessed = true;
    return source_at(std::uint64_t(slot_index) * plan.sizes.frame_bytes);
}
std::span<std::byte> Pub::fields(std::uint32_t slot_index)
{
    if(slot_index >= plan.slot_count)
        throw std::out_of_range("source slot");
    return {records.data() + std::uint64_t(slot_index) * plan.sizes.record_stride + 16,
            plan.sizes.field_bytes};
}
// Marks a filled slot ready to send. Opaque streams pass the frame's length; array streams do not.
void Pub::publish_slot(std::uint32_t slot_index, std::optional<std::uint64_t> timestamp,
                       std::optional<std::size_t> bytes)
{
    if(slot_index >= plan.slot_count)
        return;
    if(closed())
        throw std::logic_error("the publisher has closed");
    if(bytes.has_value() != plan.sizes.opaque)
        throw std::logic_error(plan.sizes.opaque ? "an opaque stream publishes with publish_bytes"
                                                 : "publish_bytes needs an opaque stream");
    if(bytes && (!*bytes || *bytes > plan.sizes.frame_bytes))
        throw std::invalid_argument(std::format("a frame of {} bytes; an opaque frame has 1 to {}",
                                                *bytes, plan.sizes.frame_bytes));
    auto &slot = slots[slot_index];
    if(limits.gpu >= 0 && !slot.gpu_accessed)
        throw std::logic_error("a GPU source slot needs gpu_payload before publishing");
    slot.payload_bytes = bytes.value_or(plan.sizes.frame_bytes);
    slot.timestamp = timestamp.value_or(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                            std::chrono::system_clock::now().time_since_epoch())
                                            .count());
    slot.cancel = false;
    if(slot.gpu_accessed)
        gpu_runtime()->record(slot.event, slot.gpu_stream);
    post(slot);
}
// Gives a slot back unpublished. It is still posted because the loop takes slots in grant order.
void Pub::cancel_slot(std::uint32_t slot_index)
{
    if(slot_index < plan.slot_count)
    {
        auto &slot = slots[slot_index];
        slot.cancel = true;
        if(slot.gpu_accessed)
            gpu_runtime()->record(slot.event, slot.gpu_stream);
        post(slot);
    }
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
    auto &operation = *operations.control(row_index, kind);
    if(operation.in_flight)
        return;
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
    wire::EndMessage end_message{row.session, row.sent, next_frame_index, row.reason,
                                 row.pull ? pull.missing : 0};
    auto encoded = wire::encode(end_message);
    send_control(row_index, OpKind::End, wire::End, encoded);
    row.end_sent = true;
}
// Starts the flush or the close of a retiring row's endpoint. A forced close does not wait for
// the subscriber.
void Pub::start_teardown(std::uint32_t row_index, OpKind kind, bool force)
{
    assert(kind == OpKind::Flush || kind == OpKind::Close);
    assert(!force || kind == OpKind::Close);
    auto &operation = *operations.control(row_index, kind);
    if(operation.in_flight)
        return;
    auto &row = rows[row_index];
    (kind == OpKind::Flush ? row.flush_started : row.close_started) = true;
    auto params = prepare(operation, force ? UCP_EP_CLOSE_FLAG_FORCE : 0);
    complete_if_done(operation, kind == OpKind::Flush ? ucp_ep_flush_nbx(row.ep, &params)
                                                      : ucp_ep_close_nbx(row.ep, &params));
}

void Pub::send_frame(std::uint32_t slot_index, std::uint32_t row_index, std::int64_t now)
{
    auto &row = rows[row_index];
    auto &slot = slots[slot_index];
    auto &operation = *operations.frame(slot_index, row_index);
    auto *record = records.data() + std::uint64_t(slot_index) * plan.sizes.record_stride;
    wire::FrameMessage frame_message{};
    frame_message.session = row.session;
    frame_message.sequence = row.sent;
    frame_message.payload_bytes = slot.payload_bytes;
    frame_message.packed = row.ring_bytes != 0; // the publisher places the frame, and says where
    frame_message.position =
        row.ring_bytes ? place(row.sent_bytes, slot.payload_bytes, row.ring_bytes) : 0;
    frame_message.index = wire::get64(record);
    frame_message.timestamp_ns = wire::get64(record + 8);
    frame_message.record_bytes = plan.sizes.padded_field_bytes;
    std::memcpy(frame_message.record.data(), record + 16, frame_message.record_bytes);
    auto header = operations.header(&operation);
    auto header_bytes = wire::encode_into(header, frame_message);
    if(!header_bytes)
    {
        retire(row_index, wire::Protocol, true);
        return;
    }
    operation.sent_ns = now;
    row.last_send_ns = now;
    ++row.sent;
    if(row.ring_bytes)
        row.sent_bytes = frame_message.position + slot.payload_bytes;
    ++row.in_flight;
    ++slot.sends;
    // An eager frame cannot land in GPU memory.
    auto params = prepare(operation, row.gpu || limits.gpu >= 0 ? UCP_AM_SEND_FLAG_RNDV : 0);
    params.op_attr_mask |= UCP_OP_ATTR_FIELD_MEMH | UCP_OP_ATTR_FIELD_MEMORY_TYPE;
    params.memh = source_memh;
    params.memory_type = limits.gpu >= 0 ? UCS_MEMORY_TYPE_CUDA : UCS_MEMORY_TYPE_HOST;
    complete_if_done(operation,
                     ucp_am_send_nbx(row.ep, wire::Frame, header.data(), header_bytes,
                                     source_at(std::uint64_t(slot_index) * plan.sizes.frame_bytes),
                                     slot.payload_bytes, &params));
}
void Pub::on_send(OpMeta *operation, ucs_status_t status)
{
    if(!operation->in_flight)
        return;
    operation->in_flight = false;
    std::uint32_t row_index = operation->row;
    std::uint32_t slot_index = operation->slot;
    auto &row = rows[row_index];
    if(row.in_flight)
        --row.in_flight;
    if(slot_index < plan.slot_count && slots[slot_index].sends)
        --slots[slot_index].sends;
    auto transfer_ns = now_ns() - operation->sent_ns;
    row.transfer_max_ns = std::max(row.transfer_max_ns, transfer_ns);
    row.transfer_sum_ns += transfer_ns;
    if(status != UCS_OK && status != UCS_ERR_CANCELED)
        retire(row_index, status == UCS_ERR_REJECTED ? wire::Protocol : wire::Closing, true);
}
void Pub::on_control(OpMeta *operation, ucs_status_t status)
{
    if(!operation->in_flight)
        return;
    operation->in_flight = false;
    auto row_index = operation->row;
    auto &row = rows[row_index];
    switch(operation->kind)
    {
        case OpKind::Ready:
        case OpKind::End:
            if(row.in_flight)
                --row.in_flight;
            if(status != UCS_OK && status != UCS_ERR_CANCELED && !row.closing)
                retire(row_index, wire::Closing, true);
            return;
        case OpKind::Flush:
            row.flush_done = true;
            return;
        case OpKind::Close:
            row.close_done = true;
            row.ep = nullptr;
            return;
        case OpKind::Frame:
            return;
    }
}
void Pub::retire(std::uint32_t row_index, wire::Reason why, bool failed)
{
    if(row_index >= plan.row_count)
        return;
    auto &row = rows[row_index];
    if(!row.used || row.closing)
        return;
    auto bit = std::uint64_t{1} << row_index;
    bool held_device = ((every.mask | pull.mask) & bit) != 0;
    if(row.ready)
        ready_mask &= ~bit;
    // A puller left before it was sent End. Frames it received but did not release are counted
    // as missing, because a frame is never sent twice.
    if(pull.mask & bit && !row.end_sent)
        pull.missing += row.sent - row.credited;
    every.remove(row_index);
    latest.remove(row_index);
    pull.remove(row_index);
    // With no pullers left before the start, clear the range. The stream then has no pull set.
    if(!pull.mask && !started.load(std::memory_order_acquire))
        pull.range_frames = 0;
    if(failed && held_device)
        ++failed_every;
    row.ready = false;
    row.closing = true;
    row.reason = why;
    row.retired_ns = now_ns();
    row.end_due = why != wire::FinishedReason && why != wire::Closing;
}
// Why an Open is refused, or Ok. The checks run in order and the first that fails decides.
wire::Status Pub::check_open(const wire::OpenMessage &open_message) const
{
    // An every subscription or a puller can make the device wait.
    bool holds_device = open_message.every || open_message.range;
    bool is_puller = open_message.range != 0;
    if(open_message.instance_id != instance_id)
        return wire::InstanceId;
    if(finishing.load())
        return wire::Finished;
    if(holds_device && !limits.allow_every)
        return wire::EveryRefused;
    if(holds_device && started.load(std::memory_order_acquire))
        return wire::Started; // every and pull must join before the start
    if(find_row(open_message.session) >= 0)
        return wire::Malformed; // the session id is already in use
    if(plan.sizes.opaque && open_message.ring_bytes < 2 * plan.sizes.frame_bytes)
        return wire::Malformed; // a packed ring must hold two of the largest frame
    if(is_puller && open_message.range % open_message.batch)
        return wire::Range; // a range must be whole batches
    if(is_puller && open_message.range > open_message.depth)
        return wire::Range; // a range must fit the ring
    if(is_puller && pull.mask && open_message.range != pull.range_frames)
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
    retire(row_index, wire::Closing, false);
}
void Pub::open(ControlEntry &entry)
{
    auto &control = engine->control;
    auto &worker = engine->worker;
    const auto &open_message = *entry.in.open;
    auto status = check_open(open_message);
    std::uint32_t row_index = 0;
    while(row_index < plan.row_count && rows[row_index].used)
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
    row.used = true;
    row.every = open_message.every;
    row.pull = open_message.range != 0;
    row.ring_depth = open_message.depth;
    row.ring_bytes = plan.sizes.opaque ? open_message.ring_bytes : 0;
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
        row.used = false;
        wire::Reply refused{wire::Endpoint};
        std::string_view why = error.what();
        refused.detail_bytes = std::uint8_t(std::min(why.size(), refused.detail.size()));
        std::memcpy(refused.detail.data(), why.data(), refused.detail_bytes);
        control.reply(entry, refused);
        return;
    }
    // The subscriber set up its ring and handlers before it sent Open, so it is ready now.
    row.ready = true;
    row.transport.n = std::uint8_t(worker->transports(row.ep, row.transport.c));
    row.network_devices = endpoint_network_devices(row.ep);
    last_topology_ns = 0;
    ready_mask |= std::uint64_t{1} << row_index;
    if(open_message.every)
        every.add(row_index);
    else if(open_message.range)
        pull.add(row_index, open_message.range);
    else
        latest.add(row_index);
    auto encoded = wire::encode(wire::SessionMessage{row.session});
    send_control(row_index, OpKind::Ready, wire::Ready, encoded);
    wire::Reply reply{wire::Ok};
    auto address = worker->address();
    reply.address_bytes = std::uint16_t(address.size());
    std::memcpy(reply.address.data(), address.data(), address.size());
    // The subscriber gave up waiting for the reply, so nobody will use the session.
    if(!control.reply(entry, reply))
        retire(row_index, wire::Closing, false);
}
// Takes back the grants the device never picked up, after finish(). The grant ring is first in,
// first out, so the grants still in it are the newest. Their slots become free, and the room
// that pullers reserved for them is released. Every sessions follow slots_granted.
void Pub::unissue_grants()
{
    // The grants the device has not taken. It may take one meanwhile: that one stays granted.
    std::uint32_t queued = 0;
    for(int slot_index; (slot_index = grants.try_pop()) >= 0; ++queued)
        slots[slot_index].held = false;
    pull.take_back(*this, queued);
    slots_granted -= queued;
}

// Every and latest sessions are all ready sessions, and a grant reserves at most one puller, so the
// pullers together hold no more than the grants not yet routed.
void Pub::check_reservations() const
{
#ifndef NDEBUG
    assert(((every.mask | latest.mask) & ~ready_mask) == 0);
    std::uint64_t pulled = 0;
    for(auto left = pull.mask; left; left &= left - 1)
        pulled += pull.reserved[std::countr_zero(left)];
    assert(pulled <= outstanding());
#endif
}
bool Pub::route_pass(std::int64_t now)
{
    bool work = false;
    if(finishing.load(std::memory_order_relaxed))
        unissue_grants();
    while(slots_routed < slots_granted)
    {
        auto slot_index = order[slots_routed % plan.slot_count];
        auto &slot = slots[slot_index];
        if(!slot.posted.load(std::memory_order_acquire))
            break;
        if(slot.gpu_accessed)
        {
            auto state = gpu_runtime()->query(slot.event);
            if(state == GpuEventState::Pending)
                break;
            if(state == GpuEventState::Failed)
            {
                source_failed = true;
                finishing.store(true);
                slot.cancel = true;
                for(std::uint32_t i = 0; i < plan.row_count; ++i)
                    if(rows[i].used && !rows[i].closing)
                        retire(i, wire::Protocol, true);
            }
            slot.gpu_accessed = false;
        }
        slot.posted.store(false, std::memory_order_relaxed);
        slot.held = false;
        // The puller this frame goes to. A cancelled slot uses no frame index, so every later
        // grant moves down by one, and the puller of the newest granted index loses one grant.
        auto *to_puller =
            pull.puller(*this, slot.cancel ? next_frame_index + slots_granted - slots_routed - 1
                                           : next_frame_index);
        ++slots_routed;
        if(to_puller && pull.reserved[to_puller->row])
            --pull.reserved[to_puller->row];
        if(!slot.cancel)
        {
            wire::put64(records.data() + std::uint64_t(slot_index) * plan.sizes.record_stride,
                        next_frame_index);
            wire::put64(records.data() + std::uint64_t(slot_index) * plan.sizes.record_stride + 8,
                        slot.timestamp);
            ++next_frame_index;
            // The range's puller is gone and no other puller could take the whole range. Any
            // puller with room for one more frame takes this one. If none has room, it is missing.
            for(std::uint32_t row_index = 0;
                pull.range_frames && !to_puller && row_index < plan.row_count; ++row_index)
                if(pull.has(row_index) && has_room(rows[row_index]))
                    to_puller = &rows[row_index];
            pull.missing += pull.range_frames && !to_puller;
            // Every sessions and the puller had room reserved when the slot was granted.
            auto takers = latest.takers(*this, slot, now);
            auto recipients = every.mask | takers;
            if(to_puller && to_puller->ready && !to_puller->closing)
                recipients |= std::uint64_t{1} << to_puller->row;
            for(auto left = latest.mask & ~takers; left; left &= left - 1)
            {
                ++skipped;
                ++rows[std::countr_zero(left)].skipped;
            }
            for(auto left = recipients; left; left &= left - 1)
                send_frame(slot_index, std::countr_zero(left), now);
        }
        else
        {
            slot.cancel = false;
        }
        work = true;
    }
    if(finishing.load() && slots_routed == slots_granted && !ended)
    {
        ended = true;
    }
    if(!ended)
    {
        check_reservations();
        return work;
    }
    // A puller's End reports the set's missing count, so it waits until no puller is sending.
    bool sending = false;
    for(const auto &row : rows)
        sending |= row.pull && row.ready && row.in_flight;
    for(std::uint32_t row_index = 0; row_index < plan.row_count; ++row_index)
    {
        auto &row = rows[row_index];
        if(row.ready && !row.end_sent && row.in_flight == 0 && !(row.pull && sending))
            send_end(row_index);
    }
    check_reservations();
    return work;
}
bool Pub::grant_pass()
{
    if(finishing.load() || !started.load(std::memory_order_acquire))
        return false;
    bool work = false;
    blocked = 0;
    for(;;)
    {
        int slot_index = -1;
        for(std::uint32_t i = 0; i < plan.slot_count; ++i)
        {
            auto candidate = (scan + i) % plan.slot_count;
            if(!slots[candidate].held && slots[candidate].sends == 0)
            {
                slot_index = candidate;
                scan = (candidate + 1) % plan.slot_count;
                break;
            }
        }
        if(slot_index < 0)
            break;
        bool room = true;
        if(int row_index = every.blocking(*this); row_index >= 0)
        {
            room = false;
            blocked = row_index + 1;
        }
        // The puller of the grant's frame index, which is the next one if no slot ahead of it is
        // cancelled. The device waits for that puller or, when none has room for the range, for
        // the one longest without a frame. With no puller left it does not wait.
        auto *holder =
            room ? pull.puller(*this, next_frame_index + slots_granted - slots_routed) : nullptr;
        if(room && pull.mask && !(holder && has_room(*holder)))
        {
            auto *slow = holder;
            for(std::uint32_t row_index = 0; !holder && row_index < plan.row_count; ++row_index)
                if(pull.has(row_index) &&
                   !(slow && slow->last_send_ns <= rows[row_index].last_send_ns))
                    slow = &rows[row_index];
            room = false;
            blocked = slow->row + 1;
        }
        if(!room)
            break;
        if(holder)
            ++pull.reserved[holder->row];
        slots[slot_index].held = true;
        order[slots_granted % plan.slot_count] = slot_index;
        grants.push(slot_index);
        ++slots_granted;
        work = true;
    }
    check_reservations();
    return work;
}
bool Pub::retire_pass(std::int64_t now)
{
    bool work = false;
    for(std::uint32_t row_index = 0; row_index < plan.row_count; ++row_index)
    {
        auto &row = rows[row_index];
        if(!row.used || !row.closing)
            continue;
        if(row.end_due && !row.end_sent && row.in_flight == 0 && row.ep)
        {
            send_end(row_index);
            work = true;
            continue;
        }
        bool late = now - row.retired_ns > k_close_ns;
        bool unclosed = row.ep && !row.close_started, idle = row.in_flight == 0;
        // A flush follows the row's last send and a close follows the flush. A late row is
        // closed by force, whatever it still has in flight.
        if(unclosed && (late || (row.flush_done && idle)))
        {
            start_teardown(row_index, OpKind::Close, late);
            work = true;
        }
        else if(unclosed && !row.flush_started && idle)
        {
            start_teardown(row_index, OpKind::Flush);
            work = true;
        }
        if(row.close_done && row.in_flight == 0 &&
           !operations.control(row_index, OpKind::Flush)->in_flight &&
           !operations.control(row_index, OpKind::Close)->in_flight)
        {
            if(row.close_reply)
            {
                engine->control.reply(*row.close_reply, {wire::Ok});
                row.close_reply = nullptr;
            }
            row.used = false;
            row.closing = false;
            row.end_due = false;
            row.end_sent = false;
            work = true;
        }
        else if(row.close_started && now - row.retired_ns > k_close_ns + k_quarantine_ns)
        {
            quarantine_owner();
            return work;
        }
    }
    return work;
}
// Eviction counts only while the device waits for a grant that a free slot could serve but a row
// has no room for: the pressure Health reports. An idle device evicts nobody, and after finish()
// the device waits for no grant. Of a set, only the puller the device waits for is evicted: one
// that is passed over holds no pressure.
void Pub::timer_pass(std::int64_t now)
{
    bool waiting = !finishing.load(std::memory_order_relaxed) && grants.empty() && blocked;
    auto timeout = std::chrono::nanoseconds(limits.pressure_timeout).count();
    for(std::uint32_t row_index = 0; row_index < plan.row_count; ++row_index)
    {
        auto &row = rows[row_index];
        bool blocking =
            waiting && (every.has(row_index) ? !has_room(row)
                                             : pull.has(row_index) && blocked == row_index + 1);
        if(!blocking)
            row.blocked_since_ns = 0;
        else if(!row.blocked_since_ns)
            row.blocked_since_ns = now;
        else if(timeout && now - row.blocked_since_ns > timeout)
            retire(row_index, wire::Evicted, true);
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
            if(row.used)
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
    observation.pressure_from = grants.empty() && blocked ? rows[blocked - 1].ordinal : 0;
    for(const auto &slot : std::span(slots.get(), plan.slot_count))
    {
        observation.slots_free += !slot.held && slot.sends == 0;
        observation.slots_held += slot.held;
    }
    observation.memory_hooks = engine->memory_hooks;
    // The frames a row can still be granted: rows of its tables and, in a packed ring, bytes.
    auto room = [&](const PubRow &row) -> std::uint64_t
    {
        auto frames =
            row.ring_depth - std::min<std::uint64_t>(row.ring_depth, row.sent - row.credited);
        if(!row.ring_bytes)
            return frames;
        auto fit =
            (row.ring_bytes - (row.sent_bytes - row.credited_bytes) + 1) / plan.sizes.frame_bytes;
        return std::min(frames, fit ? fit - 1 : 0);
    };
    for(const auto &row : rows)
        if(row.used)
            observation.rows[observation.count++] = {
                row.ordinal,
                row.ring_depth,
                std::uint32_t(room(row)),
                row.every,
                row.closing,
                row.sent,
                row.skipped,
                row.transfer_max_ns,
                row.sent ? row.transfer_sum_ns / std::int64_t(row.sent) : 0,
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
    if(!pub || pub->finishing.load())
        return {};
    int slot_index =
        pub->grants.pop_until(deadline, [&] { return pub->finishing.load() || pub->closed(); });
    if(slot_index < 0)
        return {};
    return Slot(std::move(pub), slot_index);
}
// No slot is granted before this, and no every Open is accepted after it.
void Publisher::ucx_start()
{
    if(auto pub = p_ ? p_->instance() : nullptr)
    {
        pub->started.store(true, std::memory_order_release);
        p_->poke();
    }
}
void Publisher::finish()
{
    if(auto pub = p_ ? p_->instance() : nullptr)
    {
        pub->finishing.store(true, std::memory_order_release);
        pub->grants.signal();
        p_->poke();
    }
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
                  .started = pub->started.load(std::memory_order_acquire),
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
