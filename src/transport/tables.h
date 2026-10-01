// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
// SPDX-License-Identifier: LGPL-3.0-or-later
#pragma once

#include "memory.h"
#include "transport.h"
#include "ucx.h"
#include "wire.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <future>
#include <mutex>
#include <random>
#include <span>
#include <string_view>
#include <sys/eventfd.h>
#include <thread>
#include <unistd.h>
#include <vector>

namespace TangoBulk::transport
{
struct EventFd
{
    int value = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    ~EventFd()
    {
        if(value >= 0)
            ::close(value);
    }
    EventFd() = default;
    EventFd(const EventFd &) = delete;
    EventFd &operator=(const EventFd &) = delete;
    int fd() const noexcept
    {
        return value;
    }
    void signal() noexcept
    {
        std::uint64_t n = 1;
        (void)::write(value, &n, sizeof(n));
    }
    void clear() noexcept
    {
        std::uint64_t n;
        while(::read(value, &n, sizeof(n)) > 0)
        {
        }
    }
};

// Near the clock's range limit (Deadline::max() is "no deadline"), glibc refuses the timeout and
// libstdc++ reports it as not reached, so cv.wait_until spins holding the mutex. Wait untimed.
template <class Ready>
bool wait_until(std::condition_variable &cv, std::unique_lock<std::mutex> &l, Deadline d,
                Ready ready)
{
    if(d > Deadline::clock::now() + std::chrono::hours(24 * 365 * 100))
    {
        cv.wait(l, ready);
        return true;
    }
    return cv.wait_until(l, d, ready);
}

struct IndexRing
{
    std::mutex m;
    std::condition_variable cv;
    std::vector<std::uint32_t> v;
    std::uint32_t head = 0, size = 0;
    EventFd fd;
    void reserve(std::uint32_t n)
    {
        v.assign(n, 0);
    }
    void push(std::uint32_t x)
    {
        std::lock_guard l(m);
        v[(head + size++) % v.size()] = x;
        fd.signal();
        cv.notify_one();
    }
    void post(std::uint32_t x)
    {
        push(x);
    }
    int take_locked()
    {
        if(!size)
            return -1;
        auto x = v[head];
        head = (head + 1) % v.size();
        --size;
        if(!size)
            fd.clear();
        return int(x);
    }
    int try_pop()
    {
        std::lock_guard l(m);
        return take_locked();
    }
    template <class Done> int pop_until(Deadline d, Done done)
    {
        std::unique_lock l(m);
        wait_until(cv, l, d, [&] { return size || done(); });
        return take_locked();
    }
    bool empty()
    {
        std::lock_guard l(m);
        return size == 0;
    }
    void clear()
    {
        std::lock_guard l(m);
        head = size = 0;
        fd.clear();
    }
    void signal()
    {
        std::lock_guard l(m);
        fd.signal();
        cv.notify_all();
    }
};

// The loop writes a fixed copy and never waits on a thread reading it.
template <class T> struct HealthCopy
{
    mutable std::mutex m;
    T value{};
    void write(const T &t)
    {
        std::unique_lock l(m, std::try_to_lock);
        if(l)
            value = t;
    }
    T read() const
    {
        std::lock_guard l(m);
        return value;
    }
};
template <std::size_t N> struct Text
{
    std::array<char, N> c{};
    std::uint8_t n = 0;
    void set(std::string_view s)
    {
        n = std::uint8_t(std::min(s.size(), N));
        std::memcpy(c.data(), s.data(), n);
    }
    std::string_view view() const
    {
        return {c.data(), n};
    }
};
struct Owner
{
    Owner *kept_next = nullptr;
    virtual ~Owner() = default;
};
struct Quarantine
{
    std::atomic<Owner *> head{nullptr};
    std::atomic<std::uint64_t> total{0};
    void keep(Owner &o, std::uint64_t n) noexcept
    {
        auto *old = head.load(std::memory_order_relaxed);
        do
        {
            o.kept_next = old;
        } while(!head.compare_exchange_weak(old, &o, std::memory_order_release,
                                            std::memory_order_relaxed));
        total.fetch_add(n, std::memory_order_relaxed);
    }
};
inline Quarantine &quarantine()
{
    static Quarantine q;
    return q;
}
inline std::uint64_t random64()
{
    static std::mutex m;
    static std::mt19937_64 gen(std::random_device{}());
    std::lock_guard l(m);
    return gen();
}

struct ControlEntry
{
    enum State : std::uint8_t
    {
        Free,
        Filling,
        Posted,
        Taken,
        Replied,
        Abandoned
    };
    std::atomic<std::uint8_t> state{Free};
    wire::Request in;
    wire::Reply out;
};
struct ControlTable
{
    std::array<ControlEntry, 8> entries{};
    std::mutex m;
    std::condition_variable cv;
    ControlEntry *claim()
    {
        for(auto &e : entries)
        {
            std::uint8_t f = ControlEntry::Free;
            if(e.state.compare_exchange_strong(f, ControlEntry::Filling))
                return &e;
        }
        return nullptr;
    }
    wire::Reply call(const wire::Request &request, Deadline d)
    {
        auto *e = claim();
        if(!e)
            return {wire::Busy};
        e->in = request;
        e->state.store(ControlEntry::Posted, std::memory_order_release);
        cv.notify_all();
        std::unique_lock l(m);
        if(!wait_until(
               cv, l, d,
               [&] { return e->state.load(std::memory_order_acquire) == ControlEntry::Replied; }))
        {
            // Not taken yet: nothing else will free the entry. Taken: the loop frees it.
            std::uint8_t expected = ControlEntry::Posted;
            if(e->state.compare_exchange_strong(expected, ControlEntry::Free))
                return {wire::Timeout};
            expected = ControlEntry::Taken;
            if(e->state.compare_exchange_strong(expected, ControlEntry::Abandoned))
                return {wire::Timeout};
            cv.wait(l, [&] { return e->state.load() == ControlEntry::Replied; });
        }
        auto out = e->out;
        e->state.store(ControlEntry::Free, std::memory_order_release);
        return out;
    }
    template <class F> bool drain(F &&fn)
    {
        bool any = false;
        for(auto &e : entries)
        {
            std::uint8_t p = ControlEntry::Posted;
            if(e.state.compare_exchange_strong(p, ControlEntry::Taken))
            {
                fn(e);
                any = true;
            }
        }
        return any;
    }
    // False when the caller had given up: nobody reads the reply.
    bool reply(ControlEntry &e, const wire::Reply &r)
    {
        e.out = r;
        std::uint8_t t = ControlEntry::Taken;
        bool delivered = e.state.compare_exchange_strong(t, ControlEntry::Replied);
        if(!delivered)
            e.state.store(ControlEntry::Free);
        std::lock_guard l(m); // a caller between its check and its wait must not miss this
        cv.notify_all();
        return delivered;
    }
    void wake()
    {
        cv.notify_all();
    }
};

struct Sub;
struct SubOpMeta
{
    void *owner = nullptr;
    std::uint32_t slot = 0;
    std::uint8_t kind = 0;
    bool in_flight = false;
};
struct BatchRow
{
    std::uint64_t first_sequence = 0;
    std::uint32_t frames = 0;
    bool viewed = false; // by the application before it posts the row; the loop reads it after
    std::atomic<bool> recorded{false}; // the view ended and recorded the event
    GpuStream gpu_stream;
    GpuEvent event; // made at setup, recorded again for each view of the row
};
struct Rings
{
    SubscriptionPlan plan;
    std::shared_ptr<void> storage; // host bytes, or a GPU allocation with the rows' events
    std::byte *payload = nullptr;
    std::vector<std::byte> records;
    std::vector<BatchRow> batches;
    IndexRing released;
    std::vector<std::uint64_t> busy, landed, returned, event_wait;
    void bit_set(std::vector<std::uint64_t> &, std::uint32_t);
    void bit_clear(std::vector<std::uint64_t> &, std::uint32_t);
    bool bit_test(const std::vector<std::uint64_t> &, std::uint32_t) const;
    std::uint32_t bit_count(const std::vector<std::uint64_t> &) const;
    std::byte *record(std::uint32_t k)
    {
        return records.data() + std::uint64_t(k) * plan.sizes.record_stride;
    }
};
struct Sub final : Owner
{
    Offer offer;
    SubscriptionOptions options;
    bool every = true;
    wire::Reply open_reply{};
    std::uint64_t session = 0;
    SubscriptionPlan plan;
    std::shared_ptr<Rings> rings;
    std::unique_ptr<ucx::Context> context;
    std::unique_ptr<ucx::Worker> worker;
    Region receive;
    ucp_ep_h ep = nullptr;
    std::vector<std::byte> recv_ops, ctrl_ops;
    std::uint32_t req = 0;
    std::array<std::byte, 4096> own_address{};
    std::uint16_t own_address_bytes = 0;
    std::array<std::byte, 16> credit_header{};
    IndexRing queue;
    std::atomic<bool> stop{false}, close{false};
    bool ready = false, failed = false, quarantined = false, fenced = false, gap = false;
    wire::EndMessage end{};
    bool has_end = false;
    std::uint64_t assembled = 0, credited = 0, credit_sent = 0, frames = 0, skipped = 0,
                  last_index = UINT64_MAX;
    std::uint32_t receiving = 0, quiet_turns = 0;
    std::int64_t first_landed_ns = 0, last_landed_ns = 0, end_at_ns = 0;
    std::array<char, 256> failure{};
    std::array<char, 32> transport{};
    std::uint8_t transport_bytes = 0;
    std::atomic<int> outcome{0};
    mutable std::mutex health_mutex;
    Health observation;
    std::promise<void> setup_done;
    std::promise<std::string> started;
    std::promise<wire::Reply> reply_promise;
    std::future<wire::Reply> reply_future;
    std::future<std::string> started_result;
    std::jthread loop;
    Closer closer;
    Deadline start_by{}, close_by{};
    std::atomic<bool> start_requested{false};
    bool begun = false, started_sent = false, keep_rings = false, flush_done = false,
         close_started = false, close_done = false;
    Sub(Offer, bool, SubscriptionOptions);
    ~Sub();
    void start_loop();
    void run();
    void setup();
    void begin();
    SubOpMeta *recv_op(std::uint32_t);
    SubOpMeta *ctrl_op(unsigned);
    void send_credit(std::uint64_t);
    void on_recv(SubOpMeta *, ucs_status_t);
    void on_ctrl(SubOpMeta *, ucs_status_t);
    void fail(std::string_view);
    void landed(std::uint32_t);
    bool assemble(std::int64_t);
    void deliver(std::uint32_t, std::uint32_t);
    void decide(std::int64_t);
    void release_pass();
    void credit_pass();
    void publish_health();
    void finish_close();
    void quarantine_owner();
    void release_batch(std::uint32_t);
    void cancel_receives();
};
} // namespace TangoBulk::transport
