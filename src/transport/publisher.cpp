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
    std::uint64_t timestamp = 0, bytes = 0;
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
    std::uint64_t ring = 0, sent_bytes = 0, credited_bytes = 0;
    std::uint32_t depth = 0, in_flight = 0, reserved = 0;
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
    void send_ready(std::uint32_t);
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
void send_cb(void *, ucs_status_t st, void *arg)
{
    auto *o = static_cast<OpMeta *>(arg);
    auto *p = static_cast<Pub *>(o->owner);
    if(o->kind == OpKind::Frame)
        p->on_send(o, st);
    else
        p->on_control(o, st);
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
    loop = std::jthread([self = shared_from_this()] { self->run(); });
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
    auto req = context->request_bytes();
    plan = plan_publisher(description, limits, req, worker->max_am_header());
    source = Region::allocate(context->get(), plan.source_bytes);
    records.resize(plan.records_bytes);
    slots = std::make_unique<SourceSlot[]>(plan.S);
    order.resize(plan.S);
    operations.allocate(this, plan, req);
    grants.reserve(plan.S);
    range_pullers.assign(plan.S + 2, 0); // the grants outstanding span fewer ranges than this
    for(std::uint32_t r = 0; r < plan.M; ++r)
    {
        rows[r].pub = this;
        rows[r].row = std::uint8_t(r);
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
std::uint64_t Pub::id_for(std::uint32_t r) const
{
    return rows[r].id;
}
int Pub::find_row(std::uint64_t id) const
{
    for(std::uint32_t r = 0; r < plan.M; ++r)
        if(rows[r].used && rows[r].id == id)
            return int(r);
    return -1;
}
// Room for more frames, some of them granted now: rows of the receive tables for the frames
// and, in a packed ring, the bytes for the grants. One of each is room for one more grant.
bool Pub::has_room(const PubRow &row, std::uint64_t frames, std::uint64_t grants) const
{
    return row.sent + row.reserved - row.credited + frames <= row.depth &&
           (!row.ring || ring_room(row.sent_bytes - row.credited_bytes, row.reserved + grants - 1,
                                   plan.sizes.frame_bytes, row.ring));
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
        auto r = (last_taker + step) % rows.size();
        if(!(pull_mask >> r & 1))
            continue;
        if(has_room(rows[r], untaken ? range_frames : std::max<std::uint64_t>(outstanding, 1),
                    std::max<std::uint64_t>(outstanding, 1)))
        {
            for(; passed_over; passed_over &= passed_over - 1)
                ++rows[std::countr_zero(passed_over)].passes;
            rows[r].reserved += std::uint32_t(outstanding);
            ranges_taken += untaken;
            return &rows[holder = last_taker = std::uint8_t(r)];
        }
        passed_over |= std::uint64_t{1} << r;
    }
    if(untaken && !pull_mask) // no puller is left: the range is nobody's, and its frames missing
        holder = last_taker, ++ranges_taken;
    return nullptr;
}
std::span<std::byte> Pub::payload(std::uint32_t k)
{
    if(k >= plan.S)
        throw std::out_of_range("source slot");
    return {source.at(std::uint64_t(k) * plan.sizes.frame_bytes),
            std::size_t(plan.sizes.frame_bytes)};
}
std::span<std::byte> Pub::fields(std::uint32_t k)
{
    if(k >= plan.S)
        throw std::out_of_range("source slot");
    return {records.data() + std::uint64_t(k) * plan.sizes.record_stride + 16,
            plan.sizes.field_bytes};
}
// An opaque frame names its length; an array frame has the one its shape gives.
void Pub::publish_slot(std::uint32_t k, std::optional<std::uint64_t> timestamp,
                       std::optional<std::size_t> bytes)
{
    if(k >= plan.S)
        return;
    if(bytes.has_value() != plan.sizes.opaque)
        throw std::logic_error(plan.sizes.opaque ? "an opaque stream publishes with publish_bytes"
                                                 : "publish_bytes needs an opaque stream");
    if(bytes && (!*bytes || *bytes > plan.sizes.frame_bytes))
        throw std::invalid_argument(std::format("a frame of {} bytes; an opaque frame has 1 to {}",
                                                *bytes, plan.sizes.frame_bytes));
    auto &s = slots[k];
    s.bytes = bytes.value_or(plan.sizes.frame_bytes);
    s.timestamp = timestamp.value_or(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                         std::chrono::system_clock::now().time_since_epoch())
                                         .count());
    s.cancel = false;
    s.posted.store(true, std::memory_order_release);
}
void Pub::cancel_slot(std::uint32_t k)
{
    if(k < plan.S)
    {
        slots[k].cancel = true;
        slots[k].posted.store(true, std::memory_order_release);
    }
}

void Pub::send_control(std::uint32_t r, OpKind kind, std::uint8_t type,
                       std::span<const std::byte> b)
{
    auto &o = *operations.control(r, kind);
    if(o.in_flight)
        return;
    auto h = operations.header(&o);
    assert(b.size() <= h.size());
    std::memcpy(h.data(), b.data(), b.size());
    o.in_flight = true;
    auto &row = rows[r];
    if(kind == OpKind::Ready || kind == OpKind::End)
        ++row.in_flight;
    ucp_request_param_t p{};
    p.op_attr_mask =
        UCP_OP_ATTR_FIELD_REQUEST | UCP_OP_ATTR_FIELD_CALLBACK | UCP_OP_ATTR_FIELD_USER_DATA;
    p.request = &o;
    p.cb.send = send_cb;
    p.user_data = &o;
    auto *q = ucp_am_send_nbx(row.ep, type, h.data(), b.size(), nullptr, 0, &p);
    if(!UCS_PTR_IS_PTR(q))
        on_control(&o, UCS_PTR_STATUS(q));
}
void Pub::send_ready(std::uint32_t r)
{
    auto b = wire::encode(wire::SessionMessage{rows[r].id});
    send_control(r, OpKind::Ready, wire::Ready, b);
}
void Pub::send_end(std::uint32_t r)
{
    auto &row = rows[r];
    wire::EndMessage m{row.id, row.sent, next_frame_index, row.reason, row.pull ? missing : 0};
    auto b = wire::encode(m);
    send_control(r, OpKind::End, wire::End, b);
    row.end_sent = true;
}

void Pub::send_frame(std::uint32_t k, std::uint32_t r, std::int64_t now)
{
    auto &row = rows[r];
    auto &o = *operations.frame(k, r);
    auto *rec = records.data() + std::uint64_t(k) * plan.sizes.record_stride;
    wire::FrameMessage f{};
    f.session = row.id;
    f.sequence = row.sent;
    f.payload_bytes = slots[k].bytes;
    f.packed = row.ring != 0; // the publisher places the frame, and says where
    f.position = f.packed ? place(row.sent_bytes, slots[k].bytes, row.ring) : 0;
    f.index = wire::get64(rec);
    f.timestamp_ns = wire::get64(rec + 8);
    f.record_bytes = plan.sizes.padded_fields;
    std::memcpy(f.record.data(), rec + 16, f.record_bytes);
    auto h = operations.header(&o);
    auto hn = wire::encode_into(h, f);
    if(!hn)
    {
        retire(r, wire::Protocol, true);
        return;
    }
    o.sent_ns = now;
    o.in_flight = true;
    row.last_send_ns = now;
    ++row.sent;
    if(row.ring)
        row.sent_bytes = f.position + slots[k].bytes;
    ++row.in_flight;
    ++slots[k].sends;
    ucp_request_param_t p{};
    p.op_attr_mask = UCP_OP_ATTR_FIELD_REQUEST | UCP_OP_ATTR_FIELD_CALLBACK |
                     UCP_OP_ATTR_FIELD_USER_DATA | UCP_OP_ATTR_FIELD_MEMH |
                     UCP_OP_ATTR_FIELD_MEMORY_TYPE | UCP_OP_ATTR_FIELD_FLAGS;
    p.flags = row.gpu ? UCP_AM_SEND_FLAG_RNDV : 0; // an eager frame cannot land in GPU memory
    p.request = &o;
    p.cb.send = send_cb;
    p.user_data = &o;
    p.memh = source.memh();
    p.memory_type = UCS_MEMORY_TYPE_HOST;
    auto *q =
        ucp_am_send_nbx(row.ep, wire::Frame, h.data(), hn,
                        source.at(std::uint64_t(k) * plan.sizes.frame_bytes), slots[k].bytes, &p);
    if(!UCS_PTR_IS_PTR(q))
        on_send(&o, UCS_PTR_STATUS(q));
}
void Pub::on_send(OpMeta *o, ucs_status_t st)
{
    if(!o->in_flight)
        return;
    o->in_flight = false;
    std::uint32_t r = o->row;
    std::uint32_t k = o->slot;
    auto &row = rows[r];
    if(row.in_flight)
        --row.in_flight;
    if(k < plan.S && slots[k].sends)
        --slots[k].sends;
    auto t = now_ns() - o->sent_ns;
    row.transfer_max_ns = std::max(row.transfer_max_ns, t);
    row.transfer_sum_ns += t;
    if(st != UCS_OK && st != UCS_ERR_CANCELED)
        retire(r, st == UCS_ERR_REJECTED ? wire::Protocol : wire::Closing, true);
}
void Pub::on_control(OpMeta *o, ucs_status_t st)
{
    if(!o->in_flight)
        return;
    o->in_flight = false;
    auto r = o->row;
    auto &row = rows[r];
    switch(o->kind)
    {
        case OpKind::Ready:
        case OpKind::End:
            if(row.in_flight)
                --row.in_flight;
            if(st != UCS_OK && st != UCS_ERR_CANCELED && !row.closing)
                retire(r, wire::Closing, true);
            return;
        case OpKind::Flush:
            if(st != UCS_OK)
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
void Pub::retire(std::uint32_t r, wire::Reason why, bool failed)
{
    if(r >= plan.M)
        return;
    auto &row = rows[r];
    if(!row.used || row.closing)
        return;
    auto bit = std::uint64_t{1} << r;
    bool held = ((every_mask | pull_mask) & bit) != 0;
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
    if(failed && held)
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
        [&](ControlEntry &e)
        {
            if(!e.in.open)
            {
                int r = find_row(e.in.close.session);
                if(r < 0 || rows[r].close_reply)
                {
                    control.reply(e, {wire::UnknownSession});
                    return;
                }
                rows[r].close_reply = &e;
                retire(r, wire::Closing, false);
                return;
            }
            const auto &m = *e.in.open;
            wire::Status status = wire::Ok;
            std::uint32_t r = plan.M;
            bool holds = m.every || m.range; // an every row or a puller: it can hold the device
            if(m.instance_id != instance_id)
                status = wire::InstanceId;
            else if(finishing.load())
                status = wire::Finished;
            else if(holds && !limits.allow_every)
                status = wire::EveryRefused;
            else if(holds && started.load(std::memory_order_acquire))
                status = wire::Started; // an every row and a puller join before the first grant
            else if(find_row(m.session) >= 0 ||
                    (plan.sizes.opaque && m.ring_bytes < 2 * plan.sizes.frame_bytes))
                status = wire::Malformed; // a packed ring holds a frame and its wrap at least
            else if(m.range && (m.range % m.batch || m.range > m.depth ||
                                (pull_mask && m.range != range_frames)))
                status = wire::Range; // whole batches, within its ring, and the set's
            else
            {
                for(std::uint32_t i = 0; i < plan.M; ++i)
                    if(!rows[i].used)
                    {
                        r = i;
                        break;
                    }
                if(r == plan.M)
                    status = wire::NoSession;
            }
            if(status != wire::Ok)
            {
                control.reply(e, {status});
                return;
            }
            auto &row = rows[r];
            row = PubRow{};
            row.pub = this;
            row.row = std::uint8_t(r);
            row.ordinal = ++ordinals;
            row.used = true;
            row.every = m.every;
            row.pull = m.range != 0;
            row.depth = m.depth;
            row.ring = plan.sizes.opaque ? m.ring_bytes : 0;
            row.gpu = m.gpu;
            row.id = m.session;
            row.min_interval_ns =
                std::int64_t(std::min<std::uint64_t>(m.min_interval_ns, INT64_MAX));
            row.label.set({m.label.data(), m.label_bytes});
            try
            {
                row.ep = worker->endpoint({m.address.data(), m.address_bytes}, &row, ep_error);
            }
            catch(...)
            {
                row.used = false;
                control.reply(e, {wire::Endpoint});
                return;
            }
            // The subscriber registered its rings and handlers before Open: it is ready now.
            row.ready = true;
            row.transport.n = std::uint8_t(worker->transports(row.ep, row.transport.c));
            ready_mask |= std::uint64_t{1} << r;
            if(m.every)
                every_mask |= std::uint64_t{1} << r;
            if(m.range)
            {
                pull_mask |= std::uint64_t{1} << r;
                range_frames = m.range;
            }
            send_ready(r);
            wire::Reply reply{wire::Ok};
            auto a = worker->address();
            reply.address_bytes = std::uint16_t(a.size());
            std::memcpy(reply.address.data(), a.data(), a.size());
            if(!control.reply(e, reply))
                retire(r, wire::Closing, false); // its caller gave up: nobody will use the session
        });
}
// The grant ring is first in, first out, so the grants still queued are the newest issued, and
// the device never saw them. Un-issued, they use no frame index and their slots are free at once.
// Each ready every row reserved room for each of them: every rows join before the first grant.
// So did the puller of each one's frame index, and they were to have the last frame indices.
void Pub::unissue_grants()
{
    std::lock_guard l(grants.m);
    auto n = grants.size;
    for(int k; (k = grants.take_locked()) >= 0;)
        slots[k].held = false;
    for(std::uint32_t g = 0; range_frames && g < n; ++g)
        if(auto *p = puller(next_frame_index + slots_granted - slots_routed - 1 - g);
           p && p->reserved)
            --p->reserved;
    slots_granted -= n;
    for(std::uint32_t r = 0; r < plan.M; ++r)
        if((ready_mask & every_mask) >> r & 1)
            rows[r].reserved -= n;
}

bool Pub::route_pass(std::int64_t now)
{
    bool work = false;
    if(finishing.load(std::memory_order_relaxed))
        unissue_grants();
    while(slots_routed < slots_granted)
    {
        auto k = order[slots_routed % plan.S];
        auto &s = slots[k];
        if(!s.posted.load(std::memory_order_acquire))
            break;
        s.posted.store(false, std::memory_order_relaxed);
        s.held = false;
        // The frame's puller. A cancelled slot uses no frame index: every grant after it is for
        // the index before, and the puller of the last index granted has one grant fewer.
        auto *to_puller =
            range_frames ? puller(s.cancel ? next_frame_index + slots_granted - slots_routed - 1
                                           : next_frame_index)
                         : nullptr;
        ++slots_routed;
        for(std::uint32_t r = 0; r < plan.M; ++r)
            if((every_mask & (std::uint64_t{1} << r)) && rows[r].reserved)
                --rows[r].reserved;
        if(to_puller && to_puller->reserved)
            --to_puller->reserved;
        if(!s.cancel)
        {
            wire::put64(records.data() + std::uint64_t(k) * plan.sizes.record_stride,
                        next_frame_index);
            wire::put64(records.data() + std::uint64_t(k) * plan.sizes.record_stride + 8,
                        s.timestamp);
            ++next_frame_index;
            // Its range's puller has gone and no other could take the range: a puller with room
            // for one more takes the frame, and with none it is missing.
            for(std::uint32_t r = 0; range_frames && !to_puller && r < plan.M; ++r)
                if(pull_mask >> r & 1 && has_room(rows[r]))
                    to_puller = &rows[r];
            missing += range_frames && !to_puller;
            std::uint64_t to = 0;
            for(std::uint32_t r = 0; r < plan.M; ++r)
            {
                auto bit = std::uint64_t{1} << r;
                auto &row = rows[r];
                if(!row.ready || row.closing)
                    continue;
                if(every_mask & bit || &row == to_puller)
                    to |= bit; // it joined before the first grant
                else if(!row.pull && row.sent - row.credited < row.depth && row.in_flight == 0 &&
                        now - row.last_send_ns >= row.min_interval_ns &&
                        (!row.ring ||
                         place(row.sent_bytes, s.bytes, row.ring) + s.bytes - row.credited_bytes <=
                             row.ring))
                    to |= bit;
            }
            for(std::uint32_t r = 0; r < plan.M; ++r)
            {
                auto bit = std::uint64_t{1} << r;
                if(ready_mask & bit && !((every_mask | pull_mask) & bit) && !(to & bit))
                {
                    ++skipped;
                    ++rows[r].skipped;
                }
            }
            for(std::uint32_t r = 0; r < plan.M; ++r)
                if(to & (std::uint64_t{1} << r))
                    send_frame(k, r, now);
        }
        else
        {
            s.cancel = false;
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
    for(std::uint32_t r = 0; r < plan.M; ++r)
    {
        auto &row = rows[r];
        if(row.ready && !row.end_sent && row.in_flight == 0 && !(row.pull && sending))
            send_end(r);
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
        int k = -1;
        for(std::uint32_t i = 0; i < plan.S; ++i)
        {
            auto q = (scan + i) % plan.S;
            if(!slots[q].held && slots[q].sends == 0)
            {
                k = q;
                scan = (q + 1) % plan.S;
                break;
            }
        }
        if(k < 0)
            break;
        bool room = true;
        for(std::uint32_t r = 0; r < plan.M; ++r)
            if((ready_mask & every_mask) & (std::uint64_t{1} << r))
            {
                if(!has_room(rows[r]))
                {
                    room = false;
                    blocked = r + 1;
                    break;
                }
            }
        // The puller of the grant's frame index, which is the next one if no slot ahead of it is
        // cancelled. The device waits for that puller or, when none has room for the range, for
        // the one longest without a frame. With no puller left it does not wait.
        auto *p = room && range_frames ? puller(next_frame_index + slots_granted - slots_routed)
                                       : nullptr;
        if(room && pull_mask && !(p && has_room(*p)))
        {
            auto *slow = p;
            for(std::uint32_t r = 0; !p && r < plan.M; ++r)
                if(pull_mask >> r & 1 && !(slow && slow->last_send_ns <= rows[r].last_send_ns))
                    slow = &rows[r];
            room = false;
            blocked = slow->row + 1;
        }
        if(!room)
            break;
        for(std::uint32_t r = 0; r < plan.M; ++r)
            if((ready_mask & every_mask) & (std::uint64_t{1} << r))
                ++rows[r].reserved;
        if(p)
            ++p->reserved;
        slots[k].held = true;
        order[slots_granted % plan.S] = k;
        grants.push(k);
        ++slots_granted;
        work = true;
    }
    return work;
}
bool Pub::retire_pass(std::int64_t now)
{
    bool work = false;
    for(std::uint32_t r = 0; r < plan.M; ++r)
    {
        auto &row = rows[r];
        if(!row.used || !row.closing)
            continue;
        if(row.end_due && !row.end_sent && row.in_flight == 0 && row.ep)
        {
            send_end(r);
            work = true;
            continue;
        }
        bool late = now - row.close_started_ns > k_close_ns;
        if(row.ep && !row.close_started && late)
        {
            auto *o = operations.control(r, OpKind::Close);
            if(!o->in_flight)
            {
                o->in_flight = true;
                ucp_request_param_t p{};
                p.op_attr_mask = UCP_OP_ATTR_FIELD_REQUEST | UCP_OP_ATTR_FIELD_CALLBACK |
                                 UCP_OP_ATTR_FIELD_USER_DATA | UCP_OP_ATTR_FIELD_FLAGS;
                p.request = o;
                p.cb.send = send_cb;
                p.user_data = o;
                p.flags = UCP_EP_CLOSE_FLAG_FORCE;
                row.close_started = true;
                row.forced = true;
                auto *q = ucp_ep_close_nbx(row.ep, &p);
                if(!UCS_PTR_IS_PTR(q))
                    on_control(o, UCS_PTR_STATUS(q));
                work = true;
            }
        }
        else if(row.ep && !row.close_started && !row.flush_started && row.in_flight == 0)
        {
            auto *o = operations.control(r, OpKind::Flush);
            if(!o->in_flight)
            {
                o->in_flight = true;
                ucp_request_param_t p{};
                p.op_attr_mask = UCP_OP_ATTR_FIELD_REQUEST | UCP_OP_ATTR_FIELD_CALLBACK |
                                 UCP_OP_ATTR_FIELD_USER_DATA;
                p.request = o;
                p.cb.send = send_cb;
                p.user_data = o;
                row.flush_started = true;
                auto *q = ucp_ep_flush_nbx(row.ep, &p);
                if(!UCS_PTR_IS_PTR(q))
                    on_control(o, UCS_PTR_STATUS(q));
                work = true;
            }
        }
        else if(row.ep && row.flush_done && !row.close_started && !row.in_flight)
        {
            auto *o = operations.control(r, OpKind::Close);
            if(!o->in_flight)
            {
                o->in_flight = true;
                ucp_request_param_t p{};
                p.op_attr_mask = UCP_OP_ATTR_FIELD_REQUEST | UCP_OP_ATTR_FIELD_CALLBACK |
                                 UCP_OP_ATTR_FIELD_USER_DATA;
                p.request = o;
                p.cb.send = send_cb;
                p.user_data = o;
                row.close_started = true;
                auto *q = ucp_ep_close_nbx(row.ep, &p);
                if(!UCS_PTR_IS_PTR(q))
                    on_control(o, UCS_PTR_STATUS(q));
                work = true;
            }
        }
        if(row.close_done && row.in_flight == 0 &&
           !operations.control(r, OpKind::Flush)->in_flight &&
           !operations.control(r, OpKind::Close)->in_flight)
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
            retiring_mask &= ~(std::uint64_t{1} << r);
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
    for(std::uint32_t r = 0; r < plan.M; ++r)
    {
        auto &row = rows[r];
        bool blocking = waiting && (every_mask >> r & 1 ? !has_room(row)
                                                        : pull_mask >> r & 1 && blocked == r + 1);
        if(!blocking)
            row.blocked_since_ns = 0;
        else if(!row.blocked_since_ns)
            row.blocked_since_ns = now;
        else if(timeout && now - row.blocked_since_ns > timeout)
            retire(r, wire::Evicted, true);
    }
    publish_health();
}
void Pub::publish_health()
{
    Observation o;
    o.frames = next_frame_index;
    o.skipped = skipped;
    o.missing = missing;
    o.failed_every = failed_every;
    o.pressure_from = grants.empty() && blocked ? rows[blocked - 1].ordinal : 0;
    for(std::uint32_t k = 0; k < plan.S; ++k)
    {
        o.slots_free += !slots[k].held && slots[k].sends == 0;
        o.slots_held += slots[k].held;
    }
    o.memory_hooks = context->memory_hooks();
    // The frames a row can still be granted: rows of its tables and, in a packed ring, bytes.
    auto room = [&](const PubRow &w) -> std::uint64_t
    {
        auto frames = w.depth - std::min<std::uint64_t>(w.depth, w.sent - w.credited);
        if(!w.ring)
            return frames;
        auto fit = (w.ring - (w.sent_bytes - w.credited_bytes) + 1) / plan.sizes.frame_bytes;
        return std::min(frames, fit ? fit - 1 : 0);
    };
    for(const auto &w : rows)
        if(w.used)
            o.rows[o.count++] = {
                w.ordinal, w.depth,           std::uint32_t(room(w)),
                w.every,   w.closing,         w.sent,
                w.skipped, w.transfer_max_ns, w.sent ? w.transfer_sum_ns / std::int64_t(w.sent) : 0,
                w.label,   w.transport,       w.pull,
                w.passes};
    health_copy.write(o);
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
    for(std::uint32_t r = 0; r < plan.M; ++r)
        if(rows[r].used && !rows[r].closing)
        {
            retire(r, wire::Closing, false);
            rows[r].end_due = true; // the subscriber learns why its session ended
        }
    auto deadline = now + k_close_ns + k_quarantine_ns;
    while(now_ns() < deadline)
    {
        bool active = false;
        for(auto &r : rows)
            active |= r.used;
        if(!active)
            break;
        control_pass();
        worker->progress();
        retire_pass(now_ns());
        std::this_thread::yield();
    }
    bool active = false;
    for(auto &r : rows)
        active |= r.used;
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
Slot::Slot(std::weak_ptr<transport::Pub> p, std::uint32_t i)
    : state_(std::move(p)), index_(i), active_(true)
{
}
Slot::Slot(Slot &&o) noexcept
    : state_(std::move(o.state_)), index_(o.index_), active_(std::exchange(o.active_, false))
{
}
Slot &Slot::operator=(Slot &&o) noexcept
{
    if(this != &o)
    {
        if(active_)
            if(auto p = state_.lock())
                p->cancel_slot(index_);
        state_ = std::move(o.state_);
        index_ = o.index_;
        active_ = std::exchange(o.active_, false);
    }
    return *this;
}
Slot::~Slot()
{
    if(active_)
        if(auto p = state_.lock())
            p->cancel_slot(index_);
}
std::span<std::byte> Slot::payload()
{
    if(auto p = state_.lock())
        return p->payload(index_);
    return {};
}
std::span<std::byte> Slot::fields()
{
    if(auto p = state_.lock())
        return p->fields(index_);
    return {};
}
void Slot::publish(std::optional<std::uint64_t> t) &&
{
    if(!active_)
        throw std::logic_error("slot already published");
    if(auto p = state_.lock())
        p->publish_slot(index_, t, {});
    active_ = false;
}
void Slot::publish_bytes(std::size_t bytes, std::optional<std::uint64_t> t) &&
{
    if(!active_)
        throw std::logic_error("slot already published");
    if(auto p = state_.lock())
        p->publish_slot(index_, t, bytes);
    active_ = false;
}
Publisher::Publisher(Description d, PublisherLimits l)
    : p_(std::make_shared<transport::Pub>(std::move(d), l))
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
std::optional<Slot> Publisher::acquire(Deadline d)
{
    if(!p_ || p_->finishing.load())
        return {};
    int k = p_->grants.pop_until(d, [&] { return p_->finishing.load() || p_->stopping.load(); });
    if(k < 0)
        return {};
    return Slot(p_, k);
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
    auto o = p_->health_copy.read();
    Health h{.frames = o.frames,
             .skipped = o.skipped,
             .failed_every = o.failed_every,
             .pressure = o.pressure_from != 0,
             .pressure_from = o.pressure_from,
             .slots_free = o.slots_free,
             .slots_held = o.slots_held,
             .quarantined_bytes = transport::quarantine().total.load(),
             .memory_hooks = o.memory_hooks,
             .started = p_->started.load(std::memory_order_acquire)};
    for(const auto &w : std::span(o.rows).first(o.count))
    {
        if(!w.retiring)
            ++(w.every || w.pull ? h.every : h.latest); // a puller holds the device as every does
        h.transfer_max_ns = std::max<std::uint64_t>(h.transfer_max_ns, w.transfer_max_ns);
        h.transfer_mean_ns = std::max<std::uint64_t>(h.transfer_mean_ns, w.transfer_mean_ns);
        if(w.transport.n && h.transport.find(w.transport.view()) == std::string::npos)
            h.transport += (h.transport.empty() ? "" : ";") + std::string(w.transport.view());
    }
    return h;
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
std::string handle(Publisher &p, std::string_view document)
{
    wire::Request request;
    auto status = wire::parse(document, request);
    if(status != wire::Ok || !p.p_)
        return wire::format(wire::Reply{status != wire::Ok ? status : wire::Finished});
    return wire::format(
        p.p_->control.call(request, std::chrono::steady_clock::now() + std::chrono::seconds(3)));
}
std::string ucx_stream(const Publisher &p)
{
    if(!p.p_)
        throw std::logic_error("moved publisher");
    return wire::format(wire::StreamMessage{p.p_->instance_id, p.p_->description,
                                            p.p_->limits.allow_every, p.p_->limits.max_sessions});
}
// Formatted on the calling thread, from the loop's fixed Health copy.
std::string ucx_sessions(const Publisher &p)
{
    if(!p.p_)
        throw std::logic_error("moved publisher");
    auto o = p.p_->health_copy.read();
    wire::SessionsMessage m{p.p_->instance_id, {}, o.missing};
    for(const auto &w : std::span(o.rows).first(o.count))
        m.sessions.push_back({w.ordinal, std::string(w.label.view()), w.every, w.retiring, w.depth,
                              w.room, w.sent, w.skipped, std::uint64_t(w.transfer_max_ns) / 1000,
                              std::string(w.transport.view()), w.pull, w.passes});
    return wire::format(m);
}
} // namespace TangoUcx::transport
