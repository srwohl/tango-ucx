// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-ucx project
// SPDX-License-Identifier: LGPL-3.0-or-later
#include "publisher_operations.h"
#include "tables.h"

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstring>
#include <format>
#include <stdexcept>
#include <utility>

namespace TangoUcx::transport
{
struct SourceSlot
{
    std::atomic<bool> posted{false};
    bool cancel = false, held = false;
    std::uint64_t timestamp = 0, payload_bytes = 0;
    std::uint32_t sends = 0;
};
struct PubRow
{
    Pub *pub = nullptr;
    std::uint8_t row = 0;
    std::uint32_t ordinal = 0;
    ucp_ep_h ep = nullptr;
    ucs_status_t transport_error = UCS_OK;
    std::uint64_t id = 0, sent = 0, credited = 0, skipped = 0, passes = 0;
    // An opaque stream's packed ring, and where its last sent and last credited frames end.
    std::uint64_t ring_bytes = 0, sent_bytes = 0, credited_bytes = 0;
    std::uint32_t ring_depth = 0, in_flight = 0, reserved = 0;
    std::int64_t min_interval_ns = 0, last_send_ns = 0, blocked_since_ns = 0;
    std::int64_t transfer_max_ns = 0, transfer_sum_ns = 0;
    Text<64> label, transport;
    bool used = false, ready = false, every = false, closing = false, end_due = false,
         end_sent = false, gpu = false, pull = false;
    bool flush_started = false, flush_done = false, close_started = false, close_done = false,
         forced = false;
    wire::Reason reason = wire::FinishedReason;
    std::int64_t close_started_ns = 0;
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
    std::array<RowObservation, 64> rows{};
};
struct Pub final : Owner, std::enable_shared_from_this<Pub>
{
    Description description;
    PublisherLimits limits;
    std::uint64_t instance_id = 0;
    PublisherPlan plan;
    std::unique_ptr<ucp::Context> context;
    std::unique_ptr<ucp::Worker> worker;
    Region source;
    std::vector<std::byte> records;
    PublisherOperations operations;
    std::unique_ptr<SourceSlot[]> slots;
    std::vector<std::uint16_t> order;
    std::array<PubRow, 64> rows{};
    std::uint64_t ready_mask = 0, every_mask = 0, retiring_mask = 0;
    // A set of pullers: its rows, the frames of a range, the ranges given out so far, the row
    // each of the latest went to, the row that took the last, and the frames no puller received.
    std::uint64_t pull_mask = 0, ranges_taken = 0, missing = 0;
    std::uint32_t range_frames = 0;
    std::vector<std::uint8_t> range_pullers;
    std::uint8_t last_taker = 63;
    std::uint64_t slots_granted = 0, slots_routed = 0, next_frame_index = 0, skipped = 0, turn = 0;
    std::uint32_t scan = 0, failed_every = 0, blocked = 0, ordinals = 0;
    IndexRing grants;
    ControlTable control;
    HealthCopy<Observation> health_copy;
    std::jthread loop;
    std::atomic<bool> started{false}, finishing{false}, stopping{false};
    bool ended = false, quarantined = false, setup_ok = false;
    std::promise<void> setup_done;
    std::shared_ptr<Pub> self_keep;
    explicit Pub(Description, PublisherLimits);
    ~Pub();
    void start();
    void run();
    void setup();
    void shutdown();
    void publish_slot(std::uint32_t, std::optional<std::uint64_t>, std::optional<std::size_t>);
    void cancel_slot(std::uint32_t);
    std::span<std::byte> payload(std::uint32_t);
    std::span<std::byte> fields(std::uint32_t);
    std::uint64_t id_for(std::uint32_t) const;
    int find_row(std::uint64_t) const;
    bool has_room(const PubRow &, std::uint64_t frames = 1, std::uint64_t grants = 1) const;
    PubRow *puller(std::uint64_t);
    bool control_pass();
    void unissue_grants();
    bool route_pass(std::int64_t);
    bool grant_pass();
    bool retire_pass(std::int64_t);
    void timer_pass(std::int64_t);
    void publish_health();
    void retire(std::uint32_t, wire::Reason, bool failed = false);
    void send_end(std::uint32_t);
    void send_frame(std::uint32_t, std::uint32_t, std::int64_t);
    void send_control(std::uint32_t, OpKind, std::uint8_t, std::span<const std::byte>);
    void on_send(OpMeta *, ucs_status_t);
    void on_control(OpMeta *, ucs_status_t);
    void quarantine_owner();
};

namespace
{
constexpr std::int64_t k_close_ns = 2'000'000'000LL, k_quarantine_ns = 1'000'000'000LL;
std::int64_t now_ns()
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}
void send_cb(void *, ucs_status_t status, void *arg)
{
    auto *operation = static_cast<OpMeta *>(arg);
    auto *publisher = static_cast<Pub *>(operation->owner);
    if(operation->kind == OpKind::Frame)
        publisher->on_send(operation, status);
    else
        publisher->on_control(operation, status);
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
ucs_status_t on_credit(void *arg, const void *header, size_t header_bytes, void *, size_t,
                       const ucp_am_recv_param_t *)
{
    auto *publisher = static_cast<Pub *>(arg);
    wire::CreditMessage credit_message;
    if(!wire::decode({static_cast<const std::byte *>(header), header_bytes}, credit_message))
        return UCS_OK;

    const int row_index = publisher->find_row(credit_message.session);
    if(row_index < 0)
        return UCS_OK;

    auto &row = publisher->rows[row_index];
    if(!row.ready || row.closing)
        return UCS_OK;

    // A subscriber cannot release more frames or bytes than the publisher has sent.
    if(credit_message.credit > row.sent || credit_message.bytes > row.sent_bytes)
    {
        publisher->retire(row_index, wire::Protocol, true);
        return UCS_OK;
    }

    // Credits are cumulative. Older messages must not move them backward.
    row.credited = std::max(row.credited, credit_message.credit);
    row.credited_bytes = std::max(row.credited_bytes, credit_message.bytes);

    return UCS_OK;
}
} // namespace

Pub::Pub(Description description, PublisherLimits limits)
    : description(std::move(description)), limits(limits)
{
    instance_id = (random64() >> 8) | 1;

    // A maximum payload size makes this an opaque stream.
    if(this->description.max_payload_bytes)
        this->description.element = Element::Bytes;
    if(!wire::valid_text(this->description.application_text))
        throw std::invalid_argument("the application text is not UTF-8 or is over 2,048 bytes");
}
Pub::~Pub()
{
    if(loop.joinable())
    {
        stopping = true;
        control.wake();
        loop.join();
    }
}
void Pub::start()
{
    auto ready = setup_done.get_future();
    loop = std::jthread(&Pub::run, shared_from_this());
    // A refused setup: join here, or the loop may drop the last reference and join itself.
    ready.wait();
    if(!setup_ok)
        loop.join();
    ready.get();
}
void Pub::setup()
{
    context = std::make_unique<ucp::Context>();
    worker = std::make_unique<ucp::Worker>(*context);
    auto request_bytes = context->request_bytes();
    plan = plan_publisher(description, limits, request_bytes, worker->max_am_header());
    source = Region::allocate(context->get(), plan.source_bytes);
    records.resize(plan.records_bytes);
    slots = std::make_unique<SourceSlot[]>(plan.slot_count);
    order.resize(plan.slot_count);
    operations.allocate(this, plan, request_bytes);
    grants.reserve(plan.slot_count);
    range_pullers.assign(plan.slot_count + 2,
                         0); // the grants outstanding span fewer ranges than this
    for(std::uint32_t row_index = 0; row_index < plan.row_count; ++row_index)
    {
        rows[row_index].pub = this;
        rows[row_index].row = std::uint8_t(row_index);
    }
    worker->set_am(wire::Credit, on_credit, this);
    setup_ok = true;
}
void Pub::run()
{
    try
    {
        setup();
        setup_done.set_value();
    }
    catch(...)
    {
        try
        {
            setup_done.set_exception(std::current_exception());
        }
        catch(...)
        {
        };
        source.unmap();
        if(worker)
            worker->destroy();
        if(context)
            context->destroy();
        return;
    }
    unsigned idle = 0;
    while(!stopping.load(std::memory_order_acquire))
    {
        auto now = now_ns();
        bool work = control_pass();
        work |= route_pass(now);
        work |= worker->progress() != 0;
        work |= grant_pass();
        work |= retire_pass(now);
        if(++turn % 256 == 0)
            timer_pass(now);
        idle = work ? 0 : idle + 1;
        if(idle > 64)
        {
            idle = 0;
            std::this_thread::yield();
        }
    }
    shutdown();
}
std::uint64_t Pub::id_for(std::uint32_t row_index) const
{
    return rows[row_index].id;
}
int Pub::find_row(std::uint64_t id) const
{
    for(std::uint32_t row_index = 0; row_index < plan.row_count; ++row_index)
        if(rows[row_index].used && rows[row_index].id == id)
            return int(row_index);
    return -1;
}
// Room for more frames, some of them granted now: rows of the receive tables for the frames
// and, in a packed ring, the bytes for the grants. One of each is room for one more grant.
bool Pub::has_room(const PubRow &row, std::uint64_t frames, std::uint64_t grants) const
{
    return row.sent + row.reserved - row.credited + frames <= row.ring_depth &&
           (!row.ring_bytes ||
            ring_room(row.sent_bytes - row.credited_bytes, row.reserved + grants - 1,
                      plan.sizes.frame_bytes, row.ring_bytes));
}
// The puller that holds the range of a frame index, or none. The index says which range, and
// the table which puller: after a pass no rule gives it. A range's first grant gives the range
// to the next puller in the fixed order that has room for all of it. A range whose puller has
// gone goes to the next with room for its grants outstanding, which that puller then reserves.
PubRow *Pub::puller(std::uint64_t frame_index)
{
    auto range_start = frame_index - frame_index % range_frames;
    auto &holder = range_pullers[frame_index / range_frames % range_pullers.size()];
    bool untaken = frame_index / range_frames == ranges_taken;
    if(!untaken && pull_mask >> holder & 1)
        return &rows[holder];
    auto first = std::max(next_frame_index, range_start),
         last =
             std::min(next_frame_index + slots_granted - slots_routed, range_start + range_frames);
    std::uint64_t outstanding = last > first ? last - first : 0, passed_over = 0;
    for(std::uint32_t step = 1; step <= rows.size(); ++step)
    {
        auto row_index = (last_taker + step) % rows.size();
        if(!(pull_mask >> row_index & 1))
            continue;
        if(has_room(rows[row_index],
                    untaken ? range_frames : std::max<std::uint64_t>(outstanding, 1),
                    std::max<std::uint64_t>(outstanding, 1)))
        {
            for(; passed_over; passed_over &= passed_over - 1)
                ++rows[std::countr_zero(passed_over)].passes;
            rows[row_index].reserved += std::uint32_t(outstanding);
            ranges_taken += untaken;
            return &rows[holder = last_taker = std::uint8_t(row_index)];
        }
        passed_over |= std::uint64_t{1} << row_index;
    }
    if(untaken && !pull_mask) // no puller is left: the range is nobody's, and its frames missing
        holder = last_taker, ++ranges_taken;
    return nullptr;
}
std::span<std::byte> Pub::payload(std::uint32_t slot_index)
{
    if(slot_index >= plan.slot_count)
        throw std::out_of_range("source slot");
    return {source.at(std::uint64_t(slot_index) * plan.sizes.frame_bytes),
            std::size_t(plan.sizes.frame_bytes)};
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
    if(bytes.has_value() != plan.sizes.opaque)
        throw std::logic_error(plan.sizes.opaque ? "an opaque stream publishes with publish_bytes"
                                                 : "publish_bytes needs an opaque stream");
    if(bytes && (!*bytes || *bytes > plan.sizes.frame_bytes))
        throw std::invalid_argument(std::format("a frame of {} bytes; an opaque frame has 1 to {}",
                                                *bytes, plan.sizes.frame_bytes));
    auto &slot = slots[slot_index];
    slot.payload_bytes = bytes.value_or(plan.sizes.frame_bytes);
    slot.timestamp = timestamp.value_or(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                            std::chrono::system_clock::now().time_since_epoch())
                                            .count());
    slot.cancel = false;
    slot.posted.store(true, std::memory_order_release);
}
// Gives a slot back unpublished. It is still posted because the loop takes slots in grant order.
void Pub::cancel_slot(std::uint32_t slot_index)
{
    if(slot_index < plan.slot_count)
    {
        slots[slot_index].cancel = true;
        slots[slot_index].posted.store(true, std::memory_order_release);
    }
}

void Pub::send_control(std::uint32_t row_index, OpKind kind, std::uint8_t message_id,
                       std::span<const std::byte> encoded)
{
    auto &operation = *operations.control(row_index, kind);
    if(operation.in_flight)
        return;
    auto header = operations.header(&operation);
    assert(encoded.size() <= header.size());
    std::memcpy(header.data(), encoded.data(), encoded.size());
    operation.in_flight = true;
    auto &row = rows[row_index];
    if(kind == OpKind::Ready || kind == OpKind::End)
        ++row.in_flight;
    ucp_request_param_t params{};
    params.op_attr_mask =
        UCP_OP_ATTR_FIELD_REQUEST | UCP_OP_ATTR_FIELD_CALLBACK | UCP_OP_ATTR_FIELD_USER_DATA;
    params.request = &operation;
    params.cb.send = send_cb;
    params.user_data = &operation;
    auto *request =
        ucp_am_send_nbx(row.ep, message_id, header.data(), encoded.size(), nullptr, 0, &params);
    if(!UCS_PTR_IS_PTR(request))
        on_control(&operation, UCS_PTR_STATUS(request));
}
void Pub::send_end(std::uint32_t row_index)
{
    auto &row = rows[row_index];
    wire::EndMessage end_message{row.id, row.sent, next_frame_index, row.reason,
                                 row.pull ? missing : 0};
    auto encoded = wire::encode(end_message);
    send_control(row_index, OpKind::End, wire::End, encoded);
    row.end_sent = true;
}

void Pub::send_frame(std::uint32_t slot_index, std::uint32_t row_index, std::int64_t now)
{
    auto &row = rows[row_index];
    auto &operation = *operations.frame(slot_index, row_index);
    auto *record = records.data() + std::uint64_t(slot_index) * plan.sizes.record_stride;
    wire::FrameMessage frame_message{};
    frame_message.session = row.id;
    frame_message.sequence = row.sent;
    frame_message.payload_bytes = slots[slot_index].payload_bytes;
    frame_message.packed = row.ring_bytes != 0; // the publisher places the frame, and says where
    frame_message.position =
        frame_message.packed
            ? place(row.sent_bytes, slots[slot_index].payload_bytes, row.ring_bytes)
            : 0;
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
    operation.in_flight = true;
    row.last_send_ns = now;
    ++row.sent;
    if(row.ring_bytes)
        row.sent_bytes = frame_message.position + slots[slot_index].payload_bytes;
    ++row.in_flight;
    ++slots[slot_index].sends;
    ucp_request_param_t params{};
    params.op_attr_mask = UCP_OP_ATTR_FIELD_REQUEST | UCP_OP_ATTR_FIELD_CALLBACK |
                          UCP_OP_ATTR_FIELD_USER_DATA | UCP_OP_ATTR_FIELD_MEMH |
                          UCP_OP_ATTR_FIELD_MEMORY_TYPE | UCP_OP_ATTR_FIELD_FLAGS;
    params.flags = row.gpu ? UCP_AM_SEND_FLAG_RNDV : 0; // an eager frame cannot land in GPU memory
    params.request = &operation;
    params.cb.send = send_cb;
    params.user_data = &operation;
    params.memh = source.memh();
    params.memory_type = UCS_MEMORY_TYPE_HOST;
    auto *request = ucp_am_send_nbx(row.ep, wire::Frame, header.data(), header_bytes,
                                    source.at(std::uint64_t(slot_index) * plan.sizes.frame_bytes),
                                    slots[slot_index].payload_bytes, &params);
    if(!UCS_PTR_IS_PTR(request))
        on_send(&operation, UCS_PTR_STATUS(request));
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
            if(status != UCS_OK)
            {
                row.forced = true;
                row.flush_done = true;
            }
            else
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
    bool held_device = ((every_mask | pull_mask) & bit) != 0;
    if(row.ready)
        ready_mask &= ~bit;
    // A puller that ends before its End: what was in its ring is missing, for no frame is sent
    // twice. Its ranges go to the pullers that are left. A set that empties before the start is
    // no set.
    if(pull_mask & bit && !row.end_sent)
        missing += row.sent - row.credited;
    every_mask &= ~bit;
    pull_mask &= ~bit;
    if(!pull_mask && !started.load(std::memory_order_acquire))
        range_frames = 0;
    if(failed && held_device)
        ++failed_every;
    row.ready = false;
    row.closing = true;
    row.reason = why;
    row.close_started_ns = now_ns();
    row.end_due = why != wire::FinishedReason && why != wire::Closing;
    retiring_mask |= bit;
}
bool Pub::control_pass()
{
    return control.drain(
        [&](ControlEntry &entry)
        {
            if(!entry.in.open)
            {
                int row_index = find_row(entry.in.close.session);
                if(row_index < 0 || rows[row_index].close_reply)
                {
                    control.reply(entry, {wire::UnknownSession});
                    return;
                }
                rows[row_index].close_reply = &entry;
                retire(row_index, wire::Closing, false);
                return;
            }
            const auto &open_message = *entry.in.open;
            wire::Status status = wire::Ok;
            std::uint32_t row_index = plan.row_count;
            bool holds = open_message.every ||
                         open_message.range; // an every row or a puller: it can hold the device
            if(open_message.instance_id != instance_id)
                status = wire::InstanceId;
            else if(finishing.load())
                status = wire::Finished;
            else if(holds && !limits.allow_every)
                status = wire::EveryRefused;
            else if(holds && started.load(std::memory_order_acquire))
                status = wire::Started; // an every row and a puller join before the first grant
            else if(find_row(open_message.session) >= 0 ||
                    (plan.sizes.opaque && open_message.ring_bytes < 2 * plan.sizes.frame_bytes))
                status = wire::Malformed; // a packed ring holds a frame and its wrap at least
            else if(open_message.range && (open_message.range % open_message.batch ||
                                           open_message.range > open_message.depth ||
                                           (pull_mask && open_message.range != range_frames)))
                status = wire::Range; // whole batches, within its ring, and the set's
            else
            {
                for(std::uint32_t i = 0; i < plan.row_count; ++i)
                    if(!rows[i].used)
                    {
                        row_index = i;
                        break;
                    }
                if(row_index == plan.row_count)
                    status = wire::NoSession;
            }
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
            row.id = open_message.session;
            row.min_interval_ns =
                std::int64_t(std::min<std::uint64_t>(open_message.min_interval_ns, INT64_MAX));
            row.label.set({open_message.label.data(), open_message.label_bytes});
            try
            {
                row.ep = worker->endpoint({open_message.address.data(), open_message.address_bytes},
                                          &row, ep_error);
            }
            catch(...)
            {
                row.used = false;
                control.reply(entry, {wire::Endpoint});
                return;
            }
            // The subscriber registered its rings and handlers before Open: it is ready now.
            row.ready = true;
            row.transport.n = std::uint8_t(worker->transports(row.ep, row.transport.c));
            ready_mask |= std::uint64_t{1} << row_index;
            if(open_message.every)
                every_mask |= std::uint64_t{1} << row_index;
            if(open_message.range)
            {
                pull_mask |= std::uint64_t{1} << row_index;
                range_frames = open_message.range;
            }
            auto encoded = wire::encode(wire::SessionMessage{row.id});
            send_control(row_index, OpKind::Ready, wire::Ready, encoded);
            wire::Reply reply{wire::Ok};
            auto address = worker->address();
            reply.address_bytes = std::uint16_t(address.size());
            std::memcpy(reply.address.data(), address.data(), address.size());
            if(!control.reply(entry, reply))
                retire(row_index, wire::Closing,
                       false); // its caller gave up: nobody will use the session
        });
}
// The grant ring is first in, first out, so the grants still queued are the newest issued, and
// the device never saw them. Un-issued, they use no frame index and their slots are free at once.
// Each ready every row reserved room for each of them: every rows join before the first grant.
// So did the puller of each one's frame index, and they were to have the last frame indices.
void Pub::unissue_grants()
{
    std::lock_guard lock(grants.m);
    auto queued = grants.size;
    for(int slot_index; (slot_index = grants.take_locked()) >= 0;)
        slots[slot_index].held = false;
    for(std::uint32_t grant = 0; range_frames && grant < queued; ++grant)
        if(auto *holder = puller(next_frame_index + slots_granted - slots_routed - 1 - grant);
           holder && holder->reserved)
            --holder->reserved;
    slots_granted -= queued;
    for(std::uint32_t row_index = 0; row_index < plan.row_count; ++row_index)
        if((ready_mask & every_mask) >> row_index & 1)
            rows[row_index].reserved -= queued;
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
        slot.posted.store(false, std::memory_order_relaxed);
        slot.held = false;
        // The frame's puller. A cancelled slot uses no frame index: every grant after it is for
        // the index before, and the puller of the last index granted has one grant fewer.
        auto *to_puller =
            range_frames ? puller(slot.cancel ? next_frame_index + slots_granted - slots_routed - 1
                                              : next_frame_index)
                         : nullptr;
        ++slots_routed;
        for(std::uint32_t row_index = 0; row_index < plan.row_count; ++row_index)
            if((every_mask & (std::uint64_t{1} << row_index)) && rows[row_index].reserved)
                --rows[row_index].reserved;
        if(to_puller && to_puller->reserved)
            --to_puller->reserved;
        if(!slot.cancel)
        {
            wire::put64(records.data() + std::uint64_t(slot_index) * plan.sizes.record_stride,
                        next_frame_index);
            wire::put64(records.data() + std::uint64_t(slot_index) * plan.sizes.record_stride + 8,
                        slot.timestamp);
            ++next_frame_index;
            // Its range's puller has gone and no other could take the range: a puller with room
            // for one more takes the frame, and with none it is missing.
            for(std::uint32_t row_index = 0;
                range_frames && !to_puller && row_index < plan.row_count; ++row_index)
                if(pull_mask >> row_index & 1 && has_room(rows[row_index]))
                    to_puller = &rows[row_index];
            missing += range_frames && !to_puller;
            std::uint64_t recipients = 0;
            for(std::uint32_t row_index = 0; row_index < plan.row_count; ++row_index)
            {
                auto bit = std::uint64_t{1} << row_index;
                auto &row = rows[row_index];
                if(!row.ready || row.closing)
                    continue;
                if(every_mask & bit || &row == to_puller)
                    recipients |= bit; // it joined before the first grant
                else if(!row.pull && row.sent - row.credited < row.ring_depth &&
                        row.in_flight == 0 && now - row.last_send_ns >= row.min_interval_ns &&
                        (!row.ring_bytes ||
                         place(row.sent_bytes, slot.payload_bytes, row.ring_bytes) +
                                 slot.payload_bytes - row.credited_bytes <=
                             row.ring_bytes))
                    recipients |= bit;
            }
            for(std::uint32_t row_index = 0; row_index < plan.row_count; ++row_index)
            {
                auto bit = std::uint64_t{1} << row_index;
                if(ready_mask & bit && !((every_mask | pull_mask) & bit) && !(recipients & bit))
                {
                    ++skipped;
                    ++rows[row_index].skipped;
                }
            }
            for(std::uint32_t row_index = 0; row_index < plan.row_count; ++row_index)
                if(recipients & (std::uint64_t{1} << row_index))
                    send_frame(slot_index, row_index, now);
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
        return work;
    // A puller's End says what the set lost, so it waits until no puller has a frame in flight.
    bool sending = false;
    for(const auto &row : rows)
        sending |= row.pull && row.ready && row.in_flight;
    for(std::uint32_t row_index = 0; row_index < plan.row_count; ++row_index)
    {
        auto &row = rows[row_index];
        if(row.ready && !row.end_sent && row.in_flight == 0 && !(row.pull && sending))
            send_end(row_index);
    }
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
        for(std::uint32_t row_index = 0; row_index < plan.row_count; ++row_index)
            if((ready_mask & every_mask) & (std::uint64_t{1} << row_index))
            {
                if(!has_room(rows[row_index]))
                {
                    room = false;
                    blocked = row_index + 1;
                    break;
                }
            }
        // The puller of the grant's frame index, which is the next one if no slot ahead of it is
        // cancelled. The device waits for that puller or, when none has room for the range, for
        // the one longest without a frame. With no puller left it does not wait.
        auto *holder = room && range_frames
                           ? puller(next_frame_index + slots_granted - slots_routed)
                           : nullptr;
        if(room && pull_mask && !(holder && has_room(*holder)))
        {
            auto *slow = holder;
            for(std::uint32_t row_index = 0; !holder && row_index < plan.row_count; ++row_index)
                if(pull_mask >> row_index & 1 &&
                   !(slow && slow->last_send_ns <= rows[row_index].last_send_ns))
                    slow = &rows[row_index];
            room = false;
            blocked = slow->row + 1;
        }
        if(!room)
            break;
        for(std::uint32_t row_index = 0; row_index < plan.row_count; ++row_index)
            if((ready_mask & every_mask) & (std::uint64_t{1} << row_index))
                ++rows[row_index].reserved;
        if(holder)
            ++holder->reserved;
        slots[slot_index].held = true;
        order[slots_granted % plan.slot_count] = slot_index;
        grants.push(slot_index);
        ++slots_granted;
        work = true;
    }
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
        bool late = now - row.close_started_ns > k_close_ns;
        if(row.ep && !row.close_started && late)
        {
            auto *operation = operations.control(row_index, OpKind::Close);
            if(!operation->in_flight)
            {
                operation->in_flight = true;
                ucp_request_param_t params{};
                params.op_attr_mask = UCP_OP_ATTR_FIELD_REQUEST | UCP_OP_ATTR_FIELD_CALLBACK |
                                      UCP_OP_ATTR_FIELD_USER_DATA | UCP_OP_ATTR_FIELD_FLAGS;
                params.request = operation;
                params.cb.send = send_cb;
                params.user_data = operation;
                params.flags = UCP_EP_CLOSE_FLAG_FORCE;
                row.close_started = true;
                row.forced = true;
                auto *request = ucp_ep_close_nbx(row.ep, &params);
                if(!UCS_PTR_IS_PTR(request))
                    on_control(operation, UCS_PTR_STATUS(request));
                work = true;
            }
        }
        else if(row.ep && !row.close_started && !row.flush_started && row.in_flight == 0)
        {
            auto *operation = operations.control(row_index, OpKind::Flush);
            if(!operation->in_flight)
            {
                operation->in_flight = true;
                ucp_request_param_t params{};
                params.op_attr_mask = UCP_OP_ATTR_FIELD_REQUEST | UCP_OP_ATTR_FIELD_CALLBACK |
                                      UCP_OP_ATTR_FIELD_USER_DATA;
                params.request = operation;
                params.cb.send = send_cb;
                params.user_data = operation;
                row.flush_started = true;
                auto *request = ucp_ep_flush_nbx(row.ep, &params);
                if(!UCS_PTR_IS_PTR(request))
                    on_control(operation, UCS_PTR_STATUS(request));
                work = true;
            }
        }
        else if(row.ep && row.flush_done && !row.close_started && !row.in_flight)
        {
            auto *operation = operations.control(row_index, OpKind::Close);
            if(!operation->in_flight)
            {
                operation->in_flight = true;
                ucp_request_param_t params{};
                params.op_attr_mask = UCP_OP_ATTR_FIELD_REQUEST | UCP_OP_ATTR_FIELD_CALLBACK |
                                      UCP_OP_ATTR_FIELD_USER_DATA;
                params.request = operation;
                params.cb.send = send_cb;
                params.user_data = operation;
                row.close_started = true;
                auto *request = ucp_ep_close_nbx(row.ep, &params);
                if(!UCS_PTR_IS_PTR(request))
                    on_control(operation, UCS_PTR_STATUS(request));
                work = true;
            }
        }
        if(row.close_done && row.in_flight == 0 &&
           !operations.control(row_index, OpKind::Flush)->in_flight &&
           !operations.control(row_index, OpKind::Close)->in_flight)
        {
            if(row.close_reply)
            {
                control.reply(*row.close_reply, {wire::Ok});
                row.close_reply = nullptr;
            }
            row.used = false;
            row.closing = false;
            row.end_due = false;
            row.end_sent = false;
            retiring_mask &= ~(std::uint64_t{1} << row_index);
            work = true;
        }
        else if(row.close_started && now - row.close_started_ns > k_close_ns + k_quarantine_ns)
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
        bool blocking = waiting && (every_mask >> row_index & 1
                                        ? !has_room(row)
                                        : pull_mask >> row_index & 1 && blocked == row_index + 1);
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
    Observation observation;
    observation.frames = next_frame_index;
    observation.skipped = skipped;
    observation.missing = missing;
    observation.failed_every = failed_every;
    observation.pressure_from = grants.empty() && blocked ? rows[blocked - 1].ordinal : 0;
    for(std::uint32_t slot_index = 0; slot_index < plan.slot_count; ++slot_index)
    {
        observation.slots_free += !slots[slot_index].held && slots[slot_index].sends == 0;
        observation.slots_held += slots[slot_index].held;
    }
    observation.memory_hooks = context->memory_hooks();
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
    self_keep = shared_from_this();
    quarantine().keep(*this, plan.charge);
}
void Pub::shutdown()
{
    auto now = now_ns();
    for(std::uint32_t row_index = 0; row_index < plan.row_count; ++row_index)
        if(rows[row_index].used && !rows[row_index].closing)
        {
            retire(row_index, wire::Closing, false);
            rows[row_index].end_due = true; // the subscriber learns why its session ended
        }
    auto deadline = now + k_close_ns + k_quarantine_ns;
    while(now_ns() < deadline)
    {
        bool active = false;
        for(auto &row : rows)
            active |= row.used;
        if(!active)
            break;
        control_pass();
        worker->progress();
        retire_pass(now_ns());
        std::this_thread::yield();
    }
    bool active = false;
    for(auto &row : rows)
        active |= row.used;
    if(active)
    {
        quarantine_owner();
        return;
    }
    source.unmap();
    if(worker)
        worker->destroy();
    if(context)
        context->destroy();
}
} // namespace TangoUcx::transport

namespace TangoUcx
{
Slot::Slot(std::weak_ptr<transport::Pub> pub, std::uint32_t index)
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
            if(auto pub = state_.lock())
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
        if(auto pub = state_.lock())
            pub->cancel_slot(index_);
}
std::span<std::byte> Slot::payload()
{
    if(auto pub = state_.lock())
        return pub->payload(index_);
    return {};
}
std::span<std::byte> Slot::fields()
{
    if(auto pub = state_.lock())
        return pub->fields(index_);
    return {};
}
void Slot::publish(std::optional<std::uint64_t> timestamp) &&
{
    if(!active_)
        throw std::logic_error("slot already published");
    if(auto pub = state_.lock())
        pub->publish_slot(index_, timestamp, {});
    active_ = false;
}
void Slot::publish_bytes(std::size_t bytes, std::optional<std::uint64_t> timestamp) &&
{
    if(!active_)
        throw std::logic_error("slot already published");
    if(auto pub = state_.lock())
        pub->publish_slot(index_, timestamp, bytes);
    active_ = false;
}
Publisher::Publisher(Description description, PublisherLimits limits)
    : p_(std::make_shared<transport::Pub>(std::move(description), limits))
{
    p_->start();
}
Publisher::Publisher(Publisher &&) noexcept = default;
Publisher &Publisher::operator=(Publisher &&) noexcept = default;
Publisher::~Publisher()
{
    if(p_)
    {
        p_->stopping.store(true);
        p_->control.wake();
        if(p_->loop.joinable())
            p_->loop.join();
        if(p_->quarantined)
            p_.reset();
    }
}
std::optional<Slot> Publisher::acquire(Deadline deadline)
{
    if(!p_ || p_->finishing.load())
        return {};
    int slot_index =
        p_->grants.pop_until(deadline, [&] { return p_->finishing.load() || p_->stopping.load(); });
    if(slot_index < 0)
        return {};
    return Slot(p_, slot_index);
}
// No slot is granted before this, and no every Open is accepted after it.
void Publisher::ucx_start()
{
    if(p_)
        p_->started.store(true, std::memory_order_release);
}
void Publisher::finish()
{
    if(p_)
    {
        p_->finishing.store(true, std::memory_order_release);
        p_->grants.signal();
    }
}
Health Publisher::health() const
{
    if(!p_)
        return {};
    auto observation = p_->health_copy.read();
    Health result{.frames = observation.frames,
                  .skipped = observation.skipped,
                  .failed_every = observation.failed_every,
                  .pressure = observation.pressure_from != 0,
                  .pressure_from = observation.pressure_from,
                  .slots_free = observation.slots_free,
                  .slots_held = observation.slots_held,
                  .quarantined_bytes = transport::quarantine().total.load(),
                  .memory_hooks = observation.memory_hooks,
                  .started = p_->started.load(std::memory_order_acquire)};
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
const Description &Publisher::description() const
{
    if(!p_)
        throw std::logic_error("moved publisher");
    return p_->description;
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
std::string ucx_stream(const Publisher &publisher)
{
    if(!publisher.p_)
        throw std::logic_error("moved publisher");
    return wire::format(wire::StreamMessage{publisher.p_->instance_id, publisher.p_->description,
                                            publisher.p_->limits.allow_every,
                                            publisher.p_->limits.max_sessions});
}
// Formatted on the calling thread, from the loop's fixed Health copy.
std::string ucx_sessions(const Publisher &publisher)
{
    if(!publisher.p_)
        throw std::logic_error("moved publisher");
    auto observation = publisher.p_->health_copy.read();
    wire::SessionsMessage sessions_message{publisher.p_->instance_id, {}, observation.missing};
    for(const auto &row : std::span(observation.rows).first(observation.count))
        sessions_message.sessions.push_back(
            {row.ordinal, std::string(row.label.view()), row.every, row.retiring, row.depth,
             row.room, row.sent, row.skipped, std::uint64_t(row.transfer_max_ns) / 1000,
             std::string(row.transport.view()), row.pull, row.passes});
    return wire::format(sessions_message);
}
} // namespace TangoUcx::transport
