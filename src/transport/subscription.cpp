// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-ucx project
// SPDX-License-Identifier: LGPL-3.0-or-later
#include "idle.h"
#include "tables.h"
#include "topology.h"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <format>
#include <pthread.h>
#include <stdexcept>

namespace TangoUcx::transport
{
namespace
{
constexpr std::int64_t k_end_grace_ns = 2'000'000'000LL, k_quarantine_ns = 1'000'000'000LL;
// The longest the loop waits before another pass, and the longest while a GPU event is pending.
constexpr std::int64_t k_wait_ns = 1'000'000LL, k_event_wait_ns = 100'000LL;
std::int64_t now_ns()
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}
std::int64_t deadline_ns(Deadline d)
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(d.time_since_epoch()).count();
}
std::string status_text(wire::Status s)
{
    switch(s)
    {
        case wire::InstanceId:
            return "publisher instance ID changed";
        case wire::EveryRefused:
            return "publisher refuses every and pull";
        case wire::NoSession:
            return "publisher has no free session row";
        case wire::Endpoint:
            return "publisher could not create endpoint";
        case wire::Finished:
            return "publisher has finished";
        case wire::Busy:
            return "publisher is busy";
        case wire::Version:
            return "publisher speaks another protocol major";
        case wire::Timeout:
            return "publisher did not answer in time";
        case wire::Started:
            return "publisher has started: every and pull join before its start";
        case wire::Range:
            return "a range is whole batches, fits the receive ring, and is the same for a set";
        default:
            return "malformed or refused control message";
    }
}
void send_cb(void *, ucs_status_t st, void *arg)
{
    auto *o = static_cast<SubOpMeta *>(arg);
    static_cast<Sub *>(o->owner)->on_ctrl(o, st);
}
void recv_cb(void *, ucs_status_t st, size_t, void *arg)
{
    auto *o = static_cast<SubOpMeta *>(arg);
    static_cast<Sub *>(o->owner)->on_recv(o, st);
}
void ep_error(void *arg, ucp_ep_h, ucs_status_t)
{
    auto *s = static_cast<Sub *>(arg);
    if(!s->stop.load())
        s->fail("the publisher is unreachable");
}
ucs_status_t on_frame(void *arg, const void *h, size_t hn, void *data, size_t n,
                      const ucp_am_recv_param_t *param)
{
    auto *s = static_cast<Sub *>(arg);
    if(s->stop.load(std::memory_order_acquire))
        return UCS_OK;
    auto rndv = (param->recv_attr & UCP_AM_RECV_ATTR_FLAG_RNDV) != 0;
    wire::FrameMessage f;
    const auto &sizes = s->plan.sizes;
    bool valid =
        wire::decode({static_cast<const std::byte *>(h), hn}, sizes.padded_field_bytes, f) &&
        f.session == s->session && f.payload_bytes == n && f.sequence >= s->credited &&
        !(s->plan.gpu && !rndv) && f.sequence < s->credited + s->plan.ring_depth &&
        !s->rings->bit_test(s->rings->busy, f.sequence % s->plan.ring_depth);
    auto k = std::uint32_t(f.sequence % s->plan.ring_depth);
    auto at = std::uint64_t(k) * sizes.frame_bytes;
    if(valid && sizes.opaque)
    {
        // A packed ring: the frame is where its publisher states, within the bytes Credit
        // returned, whole before the ring's end, and on no frame held.
        auto ring = s->plan.payload_bytes;
        at = f.position;
        valid = f.packed && n && n <= sizes.frame_bytes && at >= s->freed &&
                at - s->freed <= ring - n && at % ring + n <= ring && s->fits(f.sequence, at, n);
    }
    else
        valid = valid && !f.packed && n == sizes.frame_bytes;
    if(!valid)
    {
        s->fail("the publisher sent an invalid frame");
        return rndv ? UCS_ERR_REJECTED : UCS_OK;
    }
    s->rings->bit_set(s->rings->busy, k);
    if(sizes.opaque)
    {
        s->arrived = std::max(s->arrived, f.sequence + 1);
        s->rings->placements[2 * k] = at;
        s->rings->placements[2 * k + 1] = n;
        at %= s->plan.payload_bytes;
    }
    auto *r = s->rings->record(k);
    wire::put64(r, f.index);
    wire::put64(r + 8, f.timestamp_ns);
    std::memcpy(r + 16, f.record.data(), sizes.padded_field_bytes);
    if(!rndv)
    {
        if(!data)
        {
            s->fail("an eager frame had no payload");
            return UCS_OK;
        }
        std::memcpy(s->rings->payload + at, data, n);
        s->landed(k);
        return UCS_OK;
    }
    auto *o = s->recv_op(k);
    if(o->in_flight)
    {
        s->fail("a receive slot was reused while busy");
        return UCS_ERR_REJECTED;
    }
    o->in_flight = true;
    ++s->receiving;
    ucp_request_param_t p{};
    p.op_attr_mask = UCP_OP_ATTR_FIELD_REQUEST | UCP_OP_ATTR_FIELD_CALLBACK |
                     UCP_OP_ATTR_FIELD_USER_DATA | UCP_OP_ATTR_FIELD_MEMH |
                     UCP_OP_ATTR_FIELD_MEMORY_TYPE;
    p.request = o;
    p.cb.recv_am = recv_cb;
    p.user_data = o;
    p.memh = s->receive.memh();
    p.memory_type = s->plan.gpu ? UCS_MEMORY_TYPE_CUDA : UCS_MEMORY_TYPE_HOST;
    auto *q = ucp_am_recv_data_nbx(s->worker->get(), data, s->rings->payload + at, n, &p);
    if(!UCS_PTR_IS_PTR(q))
    {
        s->on_recv(o, UCS_PTR_STATUS(q));
        return UCS_OK;
    }
    return UCS_INPROGRESS;
}
ucs_status_t on_ready(void *arg, const void *h, size_t n, void *, size_t,
                      const ucp_am_recv_param_t *)
{
    auto *s = static_cast<Sub *>(arg);
    wire::SessionMessage m;
    if(wire::decode({static_cast<const std::byte *>(h), n}, m) && m.session == s->session)
        s->ready = true;
    return UCS_OK;
}
ucs_status_t on_end(void *arg, const void *h, size_t n, void *, size_t, const ucp_am_recv_param_t *)
{
    auto *s = static_cast<Sub *>(arg);
    wire::EndMessage m;
    if(wire::decode({static_cast<const std::byte *>(h), n}, m) && m.session == s->session &&
       !s->has_end)
    {
        s->end = m;
        s->has_end = true;
        s->end_at_ns = now_ns();
    }
    return UCS_OK;
}
} // namespace

Sub::Sub(Offer o, bool e, SubscriptionOptions opts)
    : offer(std::move(o)), options(std::move(opts)), every(e)
{
    reply_future = reply_promise.get_future();
    started_result = started.get_future();
}
Sub::~Sub()
{
    if(loop.joinable())
    {
        stop = true;
        close = true;
        queue.signal();
        loop.join();
    }
}
void Sub::start_loop()
{
    loop = std::jthread([this] { run(); });
}
void Sub::setup()
{
    context = std::make_unique<ucp::Context>(options.net_devices);
    worker = std::make_unique<ucp::Worker>(*context);
    req = context->request_bytes();
    plan = plan_subscription(offer, every, options, req);
    rings = std::make_shared<Rings>();
    rings->plan = plan;
    rings->records.resize(plan.records_bytes);
    rings->batches = std::vector<BatchRow>(plan.ring_depth);
    if(plan.sizes.opaque)
        rings->placements.assign(2ull * plan.ring_depth, 0);
    if(plan.event_gpu >= 0)
    {
        std::vector<GpuEvent> events(plan.ring_depth);
        rings->storage = gpu_storage(plan.event_gpu, plan.payload_bytes, !plan.gpu, events);
        for(std::size_t i = 0; i < events.size(); ++i)
            rings->batches[i].event = events[i];
    }
    else
        rings->storage = std::shared_ptr<void>(new std::byte[plan.payload_bytes], [](void *p)
                                               { delete[] static_cast<std::byte *>(p); });
    rings->payload = static_cast<std::byte *>(rings->storage.get());
    rings->released.reserve(plan.ring_depth);
    auto words = (plan.ring_depth + 63) / 64;
    rings->busy.assign(words, 0);
    rings->landed.assign(words, 0);
    rings->returned.assign(words, 0);
    rings->event_wait.assign(words, 0);
    receive = Region::adopt(context->get(), rings->storage, plan.payload_bytes,
                            plan.gpu ? UCS_MEMORY_TYPE_CUDA : UCS_MEMORY_TYPE_HOST);
    recv_ops.resize(std::uint64_t(plan.ring_depth) * plan.op_stride);
    ctrl_ops.resize(std::uint64_t(3) * plan.op_stride);
    queue.reserve(plan.ring_depth);
    for(std::uint32_t k = 0; k < plan.ring_depth; ++k)
    {
        auto *o = recv_op(k);
        std::construct_at(o);
        o->owner = this;
        o->slot = k;
        o->kind = 4;
    }
    for(unsigned k = 0; k < 3; ++k)
    {
        auto *o = ctrl_op(k);
        std::construct_at(o);
        o->owner = this;
        o->slot = k;
        o->kind = k;
    }
    do
        session = random64();
    while(!session);
    auto a = worker->address();
    own_address_bytes = std::uint16_t(a.size());
    std::memcpy(own_address.data(), a.data(), a.size());
    worker->set_am(wire::Frame, on_frame, this);
    worker->set_am(wire::Ready, on_ready, this);
    worker->set_am(wire::End, on_end, this);
    publish_health(now_ns());
}
void Sub::run()
{
    pthread_setname_np(pthread_self(), "tango-ucx-sub"); // as the publisher's loop is named
    ProgressCore core;
    try
    {
        core.pin(options.progress_cpu);
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
        receive.unmap();
        if(worker)
            worker->destroy();
        if(context)
            context->destroy();
        return;
    }
    Idle idle;
    while(!close.load(std::memory_order_acquire))
    {
        auto now = now_ns();
        bool work = false;
        if(start_requested.load(std::memory_order_acquire) && !begun)
        {
            open_reply = reply_future.get();
            begin();
            begun = true;
            work = true;
        }
        if(worker)
            work |= worker->progress() != 0;
        if(begun && !started_sent)
        {
            if(ready && !failed && plan.gpu && !options.allow_gpu_over_tcp && worker->only_tcp(ep))
            {
                std::array<char, 128> lanes{};
                std::array<char, 256> why{};
                worker->transports(ep, lanes);
                std::snprintf(why.data(), why.size(),
                              "UCX chose TCP for this GPU subscription (%s); set "
                              "allow_gpu_over_tcp to accept host staging",
                              lanes.data());
                fail(why.data());
            }
            if(ready && !failed) // Ready may arrive before start, and an endpoint still fail
            {
                transport.n = std::uint8_t(worker->transports(ep, transport.c));
                topology.network_devices = endpoint_network_devices(ep);
                last_topology_ns = 0;
                publish_health(now);
                started.set_value("");
                started_sent = true;
                work = true;
            }
            else if(failed || std::chrono::steady_clock::now() >= start_by)
            {
                if(!failed)
                    fail("no Ready from the publisher before the deadline");
                started.set_value(std::string(failure.data()));
                started_sent = true;
                work = true;
            }
        }
        if(begun)
        {
            release_pass();
            work |= assemble(now);
            credit_pass();
            decide(now);
            publish_health(now);
        }
        quiet_turns = work ? 0 : quiet_turns + 1;
        if(idle.due(work, now) && begun) // the reply to Open has no fd: poll until the start
        {
            // A released batch signals its fd. A released batch's GPU event and a partial batch's
            // max_wait have none, so they shorten the wait.
            auto wait_ns = k_wait_ns;
            if(std::ranges::any_of(rings->event_wait, [](std::uint64_t word) { return word != 0; }))
                wait_ns = k_event_wait_ns;
            if(first_landed_ns)
                wait_ns =
                    std::min(wait_ns, std::int64_t(plan.max_wait_ns) - (now - first_landed_ns));
            // A batch released from here on ends the wait; one released before is seen now.
            rings->wake.park();
            if(rings->released.empty() && Idle::wait(*worker, rings->wake.fd(), wait_ns))
                quiet_turns += 64; // a wait is as quiet as many turns of a loop that only polls
            rings->wake.unpark();
        }
    }
    finish_close();
}
void Sub::begin()
{
    try
    {
        ep =
            worker->endpoint({open_reply.address.data(), open_reply.address_bytes}, this, ep_error);
    }
    catch(const std::exception &error)
    {
        fail(std::format("could not create an endpoint to the publisher: {}", error.what()));
    }
}
SubOpMeta *Sub::recv_op(std::uint32_t k)
{
    return reinterpret_cast<SubOpMeta *>(recv_ops.data() + std::uint64_t(k) * plan.op_stride + req);
}
SubOpMeta *Sub::ctrl_op(unsigned k)
{
    return reinterpret_cast<SubOpMeta *>(ctrl_ops.data() + std::uint64_t(k) * plan.op_stride + req);
}
void Sub::send_credit(std::uint64_t credit)
{
    auto *o = ctrl_op(0);
    if(!ep || o->in_flight)
        return;
    credit_header = wire::encode(wire::CreditMessage{session, credit, freed});
    o->kind = 0;
    o->in_flight = true;
    ucp_request_param_t p{};
    p.op_attr_mask =
        UCP_OP_ATTR_FIELD_REQUEST | UCP_OP_ATTR_FIELD_CALLBACK | UCP_OP_ATTR_FIELD_USER_DATA;
    p.request = o;
    p.cb.send = send_cb;
    p.user_data = o;
    auto *q = ucp_am_send_nbx(ep, wire::Credit, credit_header.data(), plan.sizes.opaque ? 24 : 16,
                              nullptr, 0, &p);
    if(!UCS_PTR_IS_PTR(q))
        on_ctrl(o, UCS_PTR_STATUS(q));
}
void Sub::on_recv(SubOpMeta *o, ucs_status_t st)
{
    if(!o->in_flight)
        return;
    o->in_flight = false;
    if(receiving)
        --receiving;
    if(st == UCS_OK)
        landed(o->slot);
    else if(!stop.load())
        fail("a frame transfer failed");
}
void Sub::on_ctrl(SubOpMeta *o, ucs_status_t st)
{
    if(!o->in_flight)
        return;
    o->in_flight = false;
    if(o->kind == 0)
    {
        if(st != UCS_OK && !stop.load())
            fail("Credit could not be sent");
        return;
    }
    if(o->kind == 1)
    {
        if(st != UCS_OK)
            fenced = false;
        flush_done = true;
        return;
    }
    if(o->kind == 2)
    {
        close_done = true;
        ep = nullptr;
        return;
    }
}
void Sub::fail(std::string_view why)
{
    if(failed)
        return;
    failed = true;
    auto n = std::min(why.size(), failure.size() - 1);
    std::memcpy(failure.data(), why.data(), n);
    failure[n] = 0;
}
void Sub::landed(std::uint32_t k)
{
    rings->bit_set(rings->landed, k);
    last_landed_ns = now_ns();
}
// A packed ring's frame of sequence q, at the position its header states. It lies after every
// frame of a lower sequence that has arrived and before every one of a higher, leaving a byte at
// least for each frame between them that has not; next to a neighbour it is exactly where place()
// puts it. So no two frames share a byte, whatever order their headers arrive in.
bool Sub::fits(std::uint64_t q, std::uint64_t at, std::uint64_t n) const
{
    const auto &p = rings->placements;
    auto arrived_at = [&](std::uint64_t s)
    { return rings->bit_test(rings->busy, s % plan.ring_depth); };
    auto low = q; // the lowest sequence of the run not arrived that ends below q
    while(low > credited && !arrived_at(low - 1))
        if(q - --low > k_max_gap)
            return false;
    auto end = low == credited ? freed
                               : p[2 * ((low - 1) % plan.ring_depth)] +
                                     p[2 * ((low - 1) % plan.ring_depth) + 1];
    if(low == q ? at != place(end, n, plan.payload_bytes) : at < end + (q - low))
        return false;
    auto high = q + 1; // the next sequence above q that has arrived
    while(high < arrived && !arrived_at(high))
        if(++high - q > k_max_gap)
            return false;
    if(high >= arrived)
        return true;
    auto next = p[2 * (high % plan.ring_depth)], bytes = p[2 * (high % plan.ring_depth) + 1];
    return high == q + 1 ? next == place(at + n, bytes, plan.payload_bytes)
                         : at + n + (high - q - 1) <= next;
}
bool Sub::assemble(std::int64_t now)
{
    bool work = false;
    auto bound = has_end ? end.sequences : UINT64_MAX;
    for(;;)
    {
        if(assembled >= bound)
            break;
        auto k = std::uint32_t(assembled % plan.ring_depth);
        auto cap =
            std::min<std::uint64_t>({plan.batch_size, plan.ring_depth - k, bound - assembled});
        std::uint32_t n = 0;
        // A batch is one run of bytes, and of one range: it ends where the next frame starts at
        // the ring's front, or is of another range. A puller may be sent part of a range.
        auto index = [&](std::uint32_t i) { return wire::get64(rings->record(i)); };
        auto apart = [&](std::uint32_t i)
        { return rings->wraps(i) || (range && index(i) / range != index(i - 1) / range); };
        while(n < cap && rings->bit_test(rings->landed, k + n) && !(n && apart(k + n)))
            if(++n; range && (index(k + n - 1) + 1) % range == 0)
                cap = n; // the last frame of a range: nothing more is waited for
        if(!n)
            break;
        if(!first_landed_ns) // max_wait runs from the pass that first sees the batch's first frame
            first_landed_ns = now;
        // The next frame has arrived, and is apart: a row busy beyond the ring's depth is an old
        // frame's, not yet credited.
        bool cut = n < cap && assembled + n < credited + plan.ring_depth &&
                   rings->bit_test(rings->busy, k + n) && apart(k + n);
        // No frame can follow until a batch is released: every row is in use, or the packed ring
        // has no room for a grant. Waiting out max_wait would hold the device for nothing.
        auto *last = plan.sizes.opaque ? &rings->placements[2 * (k + n - 1)] : nullptr;
        cut |= assembled + n == credited + plan.ring_depth ||
               (last && !ring_room(last[0] + last[1] - freed, 0, plan.sizes.frame_bytes,
                                   plan.payload_bytes));
        if(n < cap && !cut && now - first_landed_ns < std::int64_t(plan.max_wait_ns) && !failed &&
           !has_end)
            break;
        deliver(k, n);
        work = true;
    }
    return work;
}
void Sub::deliver(std::uint32_t k, std::uint32_t n)
{
    auto &b = rings->batches[k];
    b.first_sequence = assembled;
    b.frames = n;
    for(std::uint32_t i = 0; i < n; ++i)
    {
        rings->bit_clear(rings->landed, k + i);
        auto idx = wire::get64(rings->record(k + i));
        if(!range && last_index != UINT64_MAX && idx > last_index + 1) // a puller's ranges jump
        {
            skipped += idx - last_index - 1;
            gap = true;
        }
        last_index = idx;
    }
    assembled += n;
    frames += n;
    first_landed_ns = 0;
    if(!every)
        for(int old; (old = queue.try_pop()) >= 0;)
            release_batch(old);
    queue.push(k);
}
void Sub::decide(std::int64_t now)
{
    if(outcome.load())
        return;
    // Only the next sequence matters: a frame landed beyond a missing one must not hold off
    // the End grace.
    bool drained = receiving == 0 && !rings->bit_test(rings->landed, assembled % plan.ring_depth);
    if(has_end)
    {
        if(end.reason != wire::FinishedReason && drained)
        {
            fail(end.reason == wire::Evicted    ? "evicted: the publisher ended the session"
                 : end.reason == wire::Protocol ? "the publisher ended the session: protocol"
                                                : "the publisher closed");
            outcome.store(int(TangoUcx::Outcome::Failed));
            queue.signal();
            return;
        }
        if(assembled == end.sequences)
        {
            if(end.missing) // a puller's own part is whole, and the set's is not
                fail(std::format("{} frames reached no puller", end.missing));
            bool complete = !end.missing && (!every || end.sequences == 0 || !gap);
            outcome.store(int(complete ? TangoUcx::Outcome::End : TangoUcx::Outcome::Failed));
            queue.signal();
            return;
        }
        auto last = std::max(last_landed_ns, end_at_ns);
        if(drained && quiet_turns > 64 && now - last > k_end_grace_ns)
        {
            fail("a sequence below End never arrived");
            outcome.store(int(TangoUcx::Outcome::Failed));
            queue.signal();
            return;
        }
    }
    if(failed && drained)
    {
        outcome.store(int(TangoUcx::Outcome::Failed));
        queue.signal();
    }
}
void Sub::release_batch(std::uint32_t k)
{
    if(k >= plan.ring_depth)
        return;
    auto &row = rings->batches[k];
    for(std::uint32_t i = 0; i < row.frames; ++i)
        rings->bit_set(rings->returned, (row.first_sequence + i) % plan.ring_depth);
    row.frames = 0;
    row.viewed = false;
    row.recorded.store(false, std::memory_order_relaxed);
}
void Sub::release_pass()
{
    for(int k; (k = rings->released.try_pop()) >= 0;)
    {
        if(std::uint32_t(k) < plan.ring_depth && rings->batches[k].viewed)
            rings->bit_set(rings->event_wait, k); // released; its GPU event decides when it is free
        else
            release_batch(std::uint32_t(k));
    }
    for(std::uint32_t w = 0; plan.event_gpu >= 0 && w < rings->event_wait.size(); ++w)
        for(auto bits = rings->event_wait[w]; bits; bits &= bits - 1)
        {
            auto k = w * 64 + std::uint32_t(std::countr_zero(bits));
            auto &row = rings->batches[k];
            // A CUDA event that was never recorded reads as done: wait for the view to end.
            if(!row.recorded.load(std::memory_order_acquire))
                continue;
            auto state = gpu_runtime()->query(row.event);
            if(state == GpuEventState::Pending)
                continue;
            rings->bit_clear(rings->event_wait, k);
            if(state == GpuEventState::Done)
                release_batch(k);
            else
            {
                keep_rings = true; // quarantined at close: a kernel may still read the memory
                fail("a GPU event failed; its slots can never be credited");
            }
        }
}
void Sub::credit_pass()
{
    if(stop.load() || failed)
        return;
    auto c = credited;
    while(c < assembled && rings->bit_test(rings->returned, c % plan.ring_depth))
    {
        rings->bit_clear(rings->returned, c % plan.ring_depth);
        rings->bit_clear(rings->busy, c % plan.ring_depth);
        if(plan.sizes.opaque)
            freed = rings->placements[2 * (c % plan.ring_depth)] +
                    rings->placements[2 * (c % plan.ring_depth) + 1];
        ++c;
    }
    credited = c;
    if(c > credit_sent && !ctrl_op(0)->in_flight)
    {
        credit_sent = c;
        send_credit(c);
    }
}
void Sub::publish_health(std::int64_t now)
{
    auto current_outcome = outcome.load(std::memory_order_relaxed);
    // Health is a sampled observation, as on the publisher. Keep setup, Ready and outcome
    // changes immediate, without locking and counting ring bits on every progress turn.
    if(last_topology_ns && last_health_ns && now - last_health_ns < k_wait_ns &&
       current_outcome == last_health_outcome)
        return;
    last_health_ns = now;
    last_health_outcome = current_outcome;
    bool sample = !last_topology_ns || now - last_topology_ns >= 1'000'000'000LL;
    if(sample)
    {
        last_topology_ns = now;
        sample_topology(topology, plan.gpu ? nullptr : receive.base(), receive.bytes());
    }
    std::lock_guard l(health_mutex);
    observation.frames = frames;
    observation.skipped = skipped;
    observation.last_index = last_index == UINT64_MAX ? 0 : last_index;
    observation.slots_held = rings->bit_count(rings->busy);
    observation.slots_free = plan.ring_depth - observation.slots_held;
    observation.memory_hooks = context->memory_hooks();
    if(sample)
        observation.topology = topology;
}
void Sub::cancel_receives()
{
    for(std::uint32_t k = 0; k < plan.ring_depth; ++k)
    {
        auto *o = recv_op(k);
        if(o->in_flight)
            ucp_request_cancel(worker->get(), o);
    }
}
void Sub::finish_close()
{
    if(!context || !worker)
    {
        if(!outcome.load())
        {
            outcome = int(TangoUcx::Outcome::Cancelled);
            queue.signal();
        }
        return;
    }
    cancel_receives();
    auto hard = std::max(deadline_ns(close_by), now_ns()) + k_quarantine_ns;
    bool force = !fenced;
    if(!ep)
        close_done = true;
    while(now_ns() < hard)
    {
        worker->progress();
        release_pass();
        if(receiving == 0 && ep && !close_started)
        {
            auto *o = ctrl_op(force ? 2 : 1);
            if(!o->in_flight)
            {
                o->kind = force ? 2 : 1;
                o->in_flight = true;
                ucp_request_param_t p{};
                p.op_attr_mask = UCP_OP_ATTR_FIELD_REQUEST | UCP_OP_ATTR_FIELD_CALLBACK |
                                 UCP_OP_ATTR_FIELD_USER_DATA |
                                 (force ? UCP_OP_ATTR_FIELD_FLAGS : 0);
                p.request = o;
                p.cb.send = send_cb;
                p.user_data = o;
                if(force)
                {
                    p.flags = UCP_EP_CLOSE_FLAG_FORCE;
                    close_started = true;
                    auto *q = ucp_ep_close_nbx(ep, &p);
                    if(!UCS_PTR_IS_PTR(q))
                        on_ctrl(o, UCS_PTR_STATUS(q));
                }
                else
                {
                    auto *q = ucp_ep_flush_nbx(ep, &p);
                    if(!UCS_PTR_IS_PTR(q))
                        on_ctrl(o, UCS_PTR_STATUS(q));
                }
            }
        }
        if(receiving == 0 && ep && flush_done && !close_started)
        {
            force = now_ns() >= deadline_ns(close_by);
            auto *o = ctrl_op(2);
            if(!o->in_flight)
            {
                o->kind = 2;
                o->in_flight = true;
                ucp_request_param_t p{};
                p.op_attr_mask = UCP_OP_ATTR_FIELD_REQUEST | UCP_OP_ATTR_FIELD_CALLBACK |
                                 UCP_OP_ATTR_FIELD_USER_DATA |
                                 (force ? UCP_OP_ATTR_FIELD_FLAGS : 0);
                p.request = o;
                p.cb.send = send_cb;
                p.user_data = o;
                if(force)
                    p.flags = UCP_EP_CLOSE_FLAG_FORCE;
                close_started = true;
                auto *q = ucp_ep_close_nbx(ep, &p);
                if(!UCS_PTR_IS_PTR(q))
                    on_ctrl(o, UCS_PTR_STATUS(q));
            }
        }
        if(receiving == 0 && close_done && !ctrl_op(0)->in_flight && !ctrl_op(1)->in_flight &&
           !ctrl_op(2)->in_flight)
            break;
        if(now_ns() >= deadline_ns(close_by))
            force = true;
        std::this_thread::yield();
    }
    queue.clear();
    if(!outcome.load())
    {
        outcome = int(TangoUcx::Outcome::Cancelled);
        queue.signal();
    }
    if(receiving || ep || ctrl_op(0)->in_flight || ctrl_op(1)->in_flight || ctrl_op(2)->in_flight)
    {
        quarantine_owner();
        return;
    }
    receive.unmap();
    worker->destroy();
    context->destroy();
    if(keep_rings) // UCX is closed and safe; the rings behind the failed GPU event stay until exit
        quarantine_owner();
}
void Sub::quarantine_owner()
{
    if(quarantined)
        return;
    quarantined = true;
    quarantine().keep(*this, plan.charge);
}

void Rings::bit_set(std::vector<std::uint64_t> &v, std::uint32_t i)
{
    v[i / 64] |= std::uint64_t{1} << (i % 64);
}
void Rings::bit_clear(std::vector<std::uint64_t> &v, std::uint32_t i)
{
    v[i / 64] &= ~(std::uint64_t{1} << (i % 64));
}
bool Rings::bit_test(const std::vector<std::uint64_t> &v, std::uint32_t i) const
{
    return (v[i / 64] >> (i % 64)) & 1;
}
std::uint32_t Rings::bit_count(const std::vector<std::uint64_t> &v) const
{
    std::uint32_t n = 0;
    for(auto x : v)
        n += std::popcount(x);
    return n;
}

struct PendingSubscription::Impl
{
    std::unique_ptr<Sub> sub;
    explicit Impl(std::unique_ptr<Sub> s) : sub(std::move(s)) {};
};
PendingSubscription::PendingSubscription(std::unique_ptr<Impl> p) : p_(std::move(p)) {}
PendingSubscription::PendingSubscription(PendingSubscription &&) noexcept = default;
PendingSubscription &PendingSubscription::operator=(PendingSubscription &&) noexcept = default;
PendingSubscription::~PendingSubscription()
{
    if(p_ && p_->sub)
    {
        auto &s = *p_->sub;
        s.stop = true;
        s.close = true;
        s.close_by = std::chrono::steady_clock::now();
        s.queue.signal();
        if(s.loop.joinable())
            s.loop.join();
        if(s.quarantined)
            p_->sub.release();
    }
}
std::string PendingSubscription::open_message() const
{
    if(!p_ || !p_->sub)
        throw std::logic_error("empty pending subscription");
    auto &s = *p_->sub;
    wire::OpenMessage m{};
    m.session = s.session;
    m.instance_id = s.offer.instance_id;
    m.depth = s.plan.ring_depth;
    m.batch = s.plan.batch_size;
    m.every = s.every && !s.range;
    m.range = s.range;
    if(s.options.max_rate > 0)
        m.min_interval_ns = std::uint64_t(std::min(1e9 / s.options.max_rate, 9.2e18));
    m.gpu = s.plan.gpu;
    m.ring_bytes = s.plan.sizes.opaque ? s.plan.payload_bytes : 0;
    m.label_bytes = std::uint8_t(s.options.label.size());
    std::memcpy(m.label.data(), s.options.label.data(), m.label_bytes);
    m.address_bytes = s.own_address_bytes;
    std::memcpy(m.address.data(), s.own_address.data(), m.address_bytes);
    return wire::format(m);
}
Subscription PendingSubscription::start(std::string_view reply, Deadline d, Closer closer) &&
{
    if(!p_ || !p_->sub)
        throw std::logic_error("empty pending subscription");
    auto &s = *p_->sub;
    s.closer = std::move(closer);
    s.start_by = d;
    wire::Reply m;
    if(!wire::parse(reply, m))
        throw std::runtime_error("invalid reply to Open");
    if(m.status != wire::Ok)
    {
        std::string detail(m.detail.data(), m.detail_bytes);
        throw std::runtime_error(status_text(m.status) + (detail.empty() ? "" : ": " + detail));
    }
    // The Open was accepted, so the publisher holds a row: a failure from here on closes the
    // session. A reply without an address fails at the endpoint.
    s.reply_promise.set_value(m);
    s.start_requested.store(true, std::memory_order_release);
    auto why = s.started_result.get();
    Subscription sub(std::move(p_->sub));
    p_.reset();
    if(!why.empty())
    {
        sub.close(std::chrono::steady_clock::now() + std::chrono::seconds(2));
        throw std::runtime_error(why);
    }
    return sub;
}
PendingSubscription prepare(const Offer &o, bool every, SubscriptionOptions options,
                            std::uint32_t range)
{
    auto s = std::make_unique<Sub>(o, every || range, std::move(options));
    s->range = range;
    auto ready = s->setup_done.get_future();
    s->start_loop();
    ready.get();
    return PendingSubscription(std::make_unique<PendingSubscription::Impl>(std::move(s)));
}
} // namespace TangoUcx::transport

namespace TangoUcx
{
Subscription::Subscription(std::unique_ptr<transport::Sub> s) : s_(std::move(s)) {}
Subscription::Subscription(Subscription &&) noexcept = default;
Subscription &Subscription::operator=(Subscription &&other) noexcept
{
    if(this != &other)
    {
        close(std::chrono::steady_clock::now() + std::chrono::seconds(2));
        s_ = std::move(other.s_);
    }
    return *this;
}
Subscription::~Subscription()
{
    close(std::chrono::steady_clock::now() + std::chrono::seconds(2));
}
std::optional<Batch> Subscription::read(Deadline d)
{
    if(!s_)
        return {};
    int k = s_->queue.pop_until(d, [&] { return s_->outcome.load() != 0; });
    if(k < 0)
        return {};
    return Batch(s_->rings, std::uint32_t(k));
}
std::optional<Outcome> Subscription::outcome() const
{
    if(!s_ || !s_->outcome.load() || !s_->queue.empty())
        return {};
    return Outcome(s_->outcome.load());
}
int Subscription::fd() const
{
    return s_ ? s_->queue.watch() : -1;
}
const Description &Subscription::description() const
{
    if(!s_)
        throw std::logic_error("moved subscription");
    return s_->offer.description;
}
void Subscription::close(Deadline d)
{
    if(!s_ || !s_->loop.joinable())
        return;
    auto *s = s_.get();
    s->stop.store(true, std::memory_order_release);
    bool fenced = false;
    if(s->closer)
    {
        auto doc = wire::format(wire::CloseMessage{s->session});
        try
        {
            fenced = s->closer(doc, d);
        }
        catch(...)
        {
            fenced = false;
        }
    }
    s->fenced = fenced;
    s->close_by = d;
    s->close.store(true, std::memory_order_release);
    s->queue.signal();
    s->loop.join();
    if(s->quarantined)
        s_.release();
}
Health Subscription::health() const
{
    if(!s_)
        return {};
    std::lock_guard l(s_->health_mutex);
    auto h = s_->observation;
    h.failure = s_->failure.data();
    h.transport = s_->transport.view();
    h.quarantined_bytes = transport::quarantine().total.load();
    return h;
}
} // namespace TangoUcx
