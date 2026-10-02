// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-ucx project
// SPDX-License-Identifier: LGPL-3.0-or-later
#include "tables.h"

#include <algorithm>
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
    std::uint64_t id = 0, sent = 0, credited = 0, skipped = 0;
    // An opaque stream's packed ring, and where its last sent and last credited frames end.
    std::uint64_t ring = 0, sent_bytes = 0, credited_bytes = 0;
    std::uint32_t depth = 0, in_flight = 0, reserved = 0;
    std::int64_t min_interval_ns = 0, last_send_ns = 0, blocked_since_ns = 0;
    std::int64_t transfer_max_ns = 0, transfer_sum_ns = 0;
    Text<64> label, transport;
    bool used = false, ready = false, every = false, closing = false, end_due = false,
         end_sent = false, gpu = false;
    bool flush_started = false, flush_done = false, close_started = false, close_done = false,
         forced = false;
    wire::Reason reason = wire::FinishedReason;
    std::int64_t close_started_ns = 0;
    ControlEntry *close_reply = nullptr;
};
struct OpMeta
{
    void *owner = nullptr;
    std::uint32_t slot = 0;
    std::uint16_t row = 0;
    std::uint8_t kind = 0;
    bool in_flight = false;
    std::int64_t sent_ns = 0;
};
struct RowObservation
{
    std::uint32_t ordinal = 0, depth = 0, room = 0;
    bool every = false, retiring = false;
    std::uint64_t sent = 0, skipped = 0;
    std::int64_t transfer_max_ns = 0, transfer_mean_ns = 0;
    Text<64> label, transport;
};
struct Observation
{
    std::uint64_t frames = 0, skipped = 0;
    std::uint32_t failed_every = 0, pressure_from = 0, slots_free = 0, slots_held = 0, count = 0;
    bool memory_hooks = true;
    std::array<RowObservation, 64> rows{};
};
struct Pub final : Owner, std::enable_shared_from_this<Pub>
{
    Description description;
    PublisherLimits limits;
    std::uint64_t incarnation = 0;
    PublisherPlan plan;
    std::unique_ptr<ucp::Context> context;
    std::unique_ptr<ucp::Worker> worker;
    Region source;
    std::vector<std::byte> records, ops, control_ops;
    std::unique_ptr<SourceSlot[]> slots;
    std::vector<std::uint16_t> order;
    std::array<PubRow, 64> rows{};
    std::uint64_t ready_mask = 0, every_mask = 0, retiring_mask = 0;
    std::uint64_t issued = 0, routed = 0, index = 0, skipped = 0, turn = 0;
    std::uint32_t scan = 0, failed_every = 0, blocked = 0, ordinals = 0;
    std::uint32_t req = 0;
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
    OpMeta *send_op(std::uint32_t, std::uint32_t);
    OpMeta *ctrl_op(std::uint32_t, unsigned);
    std::byte *header(OpMeta *, bool control = false);
    std::uint64_t id_for(std::uint32_t) const;
    int find_row(std::uint64_t) const;
    bool has_room(const PubRow &) const;
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
    void send_control(std::uint32_t, unsigned, std::uint8_t, std::span<const std::byte>);
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
    if(o->kind < 2)
        p->on_send(o, st);
    else
        p->on_control(o, st);
}
void ep_error(void *arg, ucp_ep_h, ucs_status_t)
{
    auto *r = static_cast<PubRow *>(arg);
    if(r && r->pub)
        r->pub->retire(r->row, wire::Closing, true);
}
ucs_status_t on_credit(void *arg, const void *h, size_t n, void *, size_t,
                       const ucp_am_recv_param_t *)
{
    auto *p = static_cast<Pub *>(arg);
    wire::CreditMessage m;
    if(!wire::decode({static_cast<const std::byte *>(h), n}, m))
        return UCS_OK;
    int r = p->find_row(m.session);
    if(r < 0)
        return UCS_OK;
    auto &row = p->rows[r];
    if(!row.ready || row.closing)
        return UCS_OK;
    if(m.credit > row.sent || m.bytes > row.sent_bytes)
        p->retire(r, wire::Protocol, true);
    else // a Credit may be overtaken by a later one
    {
        row.credited = std::max(row.credited, m.credit);
        row.credited_bytes = std::max(row.credited_bytes, m.bytes);
    }
    return UCS_OK;
}
} // namespace

Pub::Pub(Description d, PublisherLimits l) : description(std::move(d)), limits(l)
{
    incarnation = (random64() >> 8) | 1;
    if(description.max_payload_bytes)
        description.element = Element::Bytes;
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
    req = context->request_bytes();
    plan = plan_publisher(description, limits, req, worker->max_am_header());
    source = Region::allocate(context->get(), plan.source_bytes);
    records.resize(plan.records_bytes);
    slots = std::make_unique<SourceSlot[]>(plan.S);
    order.resize(plan.S);
    ops.resize(std::uint64_t(plan.S) * plan.M * plan.op_stride);
    control_ops.resize(std::uint64_t(plan.M) * 4 * plan.op_stride);
    grants.reserve(plan.S);
    for(std::uint32_t k = 0; k < plan.S; ++k)
        for(std::uint32_t r = 0; r < plan.M; ++r)
        {
            auto *o = send_op(k, r);
            std::construct_at(o);
            o->owner = this;
            o->slot = k;
            o->row = r;
            o->kind = 0;
        }
    for(std::uint32_t r = 0; r < plan.M; ++r)
    {
        rows[r].pub = this;
        rows[r].row = std::uint8_t(r);
        for(unsigned c = 0; c < 4; ++c)
        {
            auto *o = ctrl_op(r, c);
            std::construct_at(o);
            o->owner = this;
            o->row = r;
            o->kind = std::uint8_t(2 + c);
        }
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
// Room for one more grant: a row of the receive tables and, in a packed ring, the bytes.
bool Pub::has_room(const PubRow &row) const
{
    return row.sent + row.reserved - row.credited < row.depth &&
           (!row.ring || ring_room(row.sent_bytes - row.credited_bytes, row.reserved,
                                   plan.sizes.frame_bytes, row.ring));
}
OpMeta *Pub::send_op(std::uint32_t k, std::uint32_t r)
{
    auto *b = ops.data() + (std::uint64_t(k) * plan.M + r) * plan.op_stride;
    return reinterpret_cast<OpMeta *>(b + req);
}
OpMeta *Pub::ctrl_op(std::uint32_t r, unsigned c)
{
    auto *b = control_ops.data() + (std::uint64_t(r) * 4 + c) * plan.op_stride;
    return reinterpret_cast<OpMeta *>(b + req);
}
std::byte *Pub::header(OpMeta *o, bool)
{
    return reinterpret_cast<std::byte *>(o) + sizeof(OpMeta);
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

void Pub::send_control(std::uint32_t r, unsigned c, std::uint8_t type, std::span<const std::byte> b)
{
    auto &o = *ctrl_op(r, c);
    if(o.in_flight)
        return;
    auto *h = header(&o);
    std::memcpy(h, b.data(), b.size());
    o.in_flight = true;
    auto &row = rows[r];
    if(c < 2)
        ++row.in_flight;
    ucp_request_param_t p{};
    p.op_attr_mask =
        UCP_OP_ATTR_FIELD_REQUEST | UCP_OP_ATTR_FIELD_CALLBACK | UCP_OP_ATTR_FIELD_USER_DATA;
    p.request = &o;
    p.cb.send = send_cb;
    p.user_data = &o;
    auto *q = ucp_am_send_nbx(row.ep, type, h, b.size(), nullptr, 0, &p);
    if(!UCS_PTR_IS_PTR(q))
        on_control(&o, UCS_PTR_STATUS(q));
}
void Pub::send_ready(std::uint32_t r)
{
    auto b = wire::encode(wire::SessionMessage{rows[r].id});
    send_control(r, 0, wire::Ready, b);
}
void Pub::send_end(std::uint32_t r)
{
    auto &row = rows[r];
    wire::EndMessage m{row.id, row.sent, index, row.reason};
    auto b = wire::encode(m);
    send_control(r, 1, wire::End, b);
    row.end_sent = true;
}

void Pub::send_frame(std::uint32_t k, std::uint32_t r, std::int64_t now)
{
    auto &row = rows[r];
    auto &o = *send_op(k, r);
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
    auto hn = wire::encode_into({header(&o), plan.sizes.header_bytes}, f);
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
        ucp_am_send_nbx(row.ep, wire::Frame, header(&o), hn,
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
    if(o->kind < 4)
    {
        if(row.in_flight)
            --row.in_flight;
        if(st != UCS_OK && st != UCS_ERR_CANCELED && !row.closing)
            retire(r, wire::Closing, true);
        return;
    }
    if(o->kind == 4)
    {
        if(st != UCS_OK)
        {
            row.forced = true;
            row.flush_done = true;
        }
        else
            row.flush_done = true;
        return;
    }
    if(o->kind == 5)
    {
        row.close_done = true;
        row.ep = nullptr;
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
    bool was_every = (every_mask & bit) != 0;
    if(row.ready)
        ready_mask &= ~bit;
    every_mask &= ~bit;
    if(failed && was_every)
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
            if(m.incarnation != incarnation)
                status = wire::Incarnation;
            else if(finishing.load())
                status = wire::Finished;
            else if(m.every && !limits.allow_every)
                status = wire::EveryRefused;
            else if(m.every && started.load(std::memory_order_acquire))
                status = wire::Started; // an every row joins before the first grant
            else if(find_row(m.session) >= 0 ||
                    (plan.sizes.opaque && m.ring_bytes < 2 * plan.sizes.frame_bytes))
                status = wire::Malformed; // a packed ring holds a frame and its wrap at least
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
void Pub::unissue_grants()
{
    std::lock_guard l(grants.m);
    auto n = grants.size;
    for(int k; (k = grants.take_locked()) >= 0;)
        slots[k].held = false;
    issued -= n;
    for(std::uint32_t r = 0; r < plan.M; ++r)
        if((ready_mask & every_mask) >> r & 1)
            rows[r].reserved -= n;
}

bool Pub::route_pass(std::int64_t now)
{
    bool work = false;
    if(finishing.load(std::memory_order_relaxed))
        unissue_grants();
    while(routed < issued)
    {
        auto k = order[routed % plan.S];
        auto &s = slots[k];
        if(!s.posted.load(std::memory_order_acquire))
            break;
        s.posted.store(false, std::memory_order_relaxed);
        s.held = false;
        ++routed;
        for(std::uint32_t r = 0; r < plan.M; ++r)
            if((every_mask & (std::uint64_t{1} << r)) && rows[r].reserved)
                --rows[r].reserved;
        if(!s.cancel)
        {
            wire::put64(records.data() + std::uint64_t(k) * plan.sizes.record_stride, index);
            wire::put64(records.data() + std::uint64_t(k) * plan.sizes.record_stride + 8,
                        s.timestamp);
            ++index;
            std::uint64_t to = 0;
            for(std::uint32_t r = 0; r < plan.M; ++r)
            {
                auto bit = std::uint64_t{1} << r;
                auto &row = rows[r];
                if(!row.ready || row.closing)
                    continue;
                if(every_mask & bit)
                    to |= bit; // it joined before the first grant
                else if(row.sent - row.credited < row.depth && row.in_flight == 0 &&
                        now - row.last_send_ns >= row.min_interval_ns &&
                        (!row.ring ||
                         place(row.sent_bytes, s.bytes, row.ring) + s.bytes - row.credited_bytes <=
                             row.ring))
                    to |= bit;
            }
            for(std::uint32_t r = 0; r < plan.M; ++r)
            {
                auto bit = std::uint64_t{1} << r;
                if(ready_mask & bit && !(every_mask & bit) && !(to & bit))
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
    if(finishing.load() && routed == issued && !ended)
    {
        ended = true;
    }
    if(ended)
        for(std::uint32_t r = 0; r < plan.M; ++r)
        {
            auto &row = rows[r];
            if(row.ready && !row.end_sent && row.in_flight == 0)
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
        if(!room)
            break;
        for(std::uint32_t r = 0; r < plan.M; ++r)
            if((ready_mask & every_mask) & (std::uint64_t{1} << r))
                ++rows[r].reserved;
        slots[k].held = true;
        order[issued % plan.S] = k;
        grants.push(k);
        ++issued;
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
            auto *o = ctrl_op(r, 3);
            if(!o->in_flight)
            {
                o->kind = 5;
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
            auto *o = ctrl_op(r, 2);
            if(!o->in_flight)
            {
                o->kind = 4;
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
            auto *o = ctrl_op(r, 3);
            if(!o->in_flight)
            {
                o->kind = 5;
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
        if(row.close_done && row.in_flight == 0 && !ctrl_op(r, 2)->in_flight &&
           !ctrl_op(r, 3)->in_flight)
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
// the device waits for no grant.
void Pub::timer_pass(std::int64_t now)
{
    bool waiting = !finishing.load(std::memory_order_relaxed) && grants.empty() && blocked;
    auto timeout = std::chrono::nanoseconds(limits.pressure_timeout).count();
    for(std::uint32_t r = 0; r < plan.M; ++r)
    {
        auto &row = rows[r];
        bool blocking = waiting && (ready_mask & every_mask) >> r & 1 && !has_room(row);
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
    o.frames = index;
    o.skipped = skipped;
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
                w.label,   w.transport};
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
            ++(w.every ? h.every : h.latest);
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
    return wire::format(wire::StreamMessage{p.p_->incarnation, p.p_->description,
                                            p.p_->limits.allow_every, p.p_->limits.max_sessions});
}
// Formatted on the calling thread, from the loop's fixed Health copy.
std::string ucx_sessions(const Publisher &p)
{
    if(!p.p_)
        throw std::logic_error("moved publisher");
    auto o = p.p_->health_copy.read();
    wire::SessionsMessage m{p.p_->incarnation, {}};
    for(const auto &w : std::span(o.rows).first(o.count))
        m.sessions.push_back({w.ordinal, std::string(w.label.view()), w.every, w.retiring, w.depth,
                              w.room, w.sent, w.skipped, std::uint64_t(w.transfer_max_ns) / 1000,
                              std::string(w.transport.view())});
    return wire::format(m);
}
} // namespace TangoUcx::transport
