// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-ucx project
// SPDX-License-Identifier: LGPL-3.0-or-later
// A publisher instance (Pub): its setup, the slots the device fills and posts, the sessions
// that open, receive frames and retire, and the passes of the progress loop that drive them.
// Which session gets which frame is in delivery.cpp.
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
    pull.setup(plan.slot_count);
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
                                 row.pull ? pull.missing() : 0};
    auto encoded = wire::encode(end_message);
    send_control(row_index, OpKind::End, wire::End, encoded);
    row.end = EndNotice::Sent;
    if(row.state == SessionState::Ready)
        row.state = SessionState::Ended;
}
// The close is forced because UCX keeps a gracefully closed endpoint that its peer connected back
// to, with the peer's shared memory mapped, until the worker is destroyed.
void Pub::start_teardown(std::uint32_t row_index, OpKind kind)
{
    assert(kind == OpKind::Flush || kind == OpKind::Close);
    auto &operation = *operations.control(row_index, kind); // teardown starts each step once
    assert(!operation.in_flight);
    auto &row = rows[row_index];
    row.teardown = kind == OpKind::Flush ? Teardown::Flushing : Teardown::Closing;
    auto params = prepare(operation, kind == OpKind::Close ? UCP_EP_CLOSE_FLAG_FORCE : 0);
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
            if(row.teardown == Teardown::Flushing) // a late close may have overtaken it
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
    bool held_device = leave_delivery(row);
    ready_rows.remove(row_index);
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
    holding.store(device_holders().size(), std::memory_order_release);
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
    return is_puller ? pull.check_open(open_message) : wire::Ok;
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
    join_delivery(row_index, open_message);
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
    if(slot.publication.cancel)
    {
        ++slots_routed; // it uses no frame index, and goes to nobody
        return true;
    }
    auto to = recipients(slot, now);
    ++slots_routed;
    route_frame(slot_index, to, now);
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
// Gives a published frame the next frame index and sends it to its recipients.
void Pub::route_frame(std::uint32_t slot_index, const Recipients &to, std::int64_t now)
{
    auto &slot = slots[slot_index];
    auto slot_record = record(slot_index);
    slot_record.set_index(next_frame_index);
    slot_record.set_timestamp(slot.publication.timestamp);
    ++next_frame_index;
    for(auto skipper : to.skip)
    {
        ++skipped;
        ++rows[skipper].skipped;
    }
    // A send may complete at once: the buffer goes back only after the last is issued.
    slot.routing = true;
    for(auto recipient : to.send)
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
        blocked = reserve_grant();
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
        // closed at once, whatever it still has in flight.
        if(unclosed && (late || (row.teardown == Teardown::Flushed && idle)))
        {
            start_teardown(row_index, OpKind::Close);
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
// Eviction counts only while the device waits, for the rows that hold that pressure (blocks).
void Pub::timer_pass(std::int64_t now)
{
    auto pressure = pressure_row();
    auto timeout = std::chrono::nanoseconds(limits.pressure_timeout).count();
    for(std::uint32_t row_index = 0; row_index < plan.row_count; ++row_index)
    {
        auto &row = rows[row_index];
        if(!blocks(row_index, pressure))
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
    observation.missing = pull.missing();
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
