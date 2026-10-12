// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-ucx project
// SPDX-License-Identifier: LGPL-3.0-or-later
#pragma once

#include "memory.h"
#include "moodycamel/blockingconcurrentqueue.h"
#include "transport.h"
#include "ucp.h"
#include "wire.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
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

namespace TangoUcx::transport
{
struct EventFd
{
    int value;
    ~EventFd()
    {
        if(value >= 0)
            ::close(value);
    }
    explicit EventFd(bool enabled = true)
        : value(enabled ? eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK) : -1)
    {
    }
    EventFd(const EventFd &) = delete;
    EventFd &operator=(const EventFd &) = delete;
    int fd() const noexcept
    {
        return value;
    }
    void signal() noexcept
    {
        if(value < 0)
            return;
        std::uint64_t n = 1;
        (void)::write(value, &n, sizeof(n));
    }
    void clear() noexcept
    {
        if(value < 0)
            return;
        std::uint64_t n;
        while(::read(value, &n, sizeof(n)) > 0)
        {
        }
    }
};

// Deadline::max() is "no deadline", and so is anything near the clock's range limit: wait untimed.
inline bool no_deadline(Deadline d)
{
    return d > Deadline::clock::now() + std::chrono::hours(24 * 365 * 100);
}
// The semaphore's and the queue's timed waits are on the wall clock: the C library this is built
// with has no other. A wait that ends early, as when that clock is set forward, is taken up again
// here and in IndexRing::pop_until, which ask the steady clock. A clock set back can still hold a
// wait past its deadline.
inline bool wait_until(moodycamel::LightweightSemaphore &semaphore, Deadline d)
{
    if(no_deadline(d))
        return semaphore.wait();
    for(;;)
    {
        auto left =
            std::chrono::duration_cast<std::chrono::microseconds>(d - Deadline::clock::now());
        if(left.count() <= 0) // the semaphore takes a negative time as no deadline at all
            return semaphore.tryWait();
        if(semaphore.wait(left.count()))
            return true;
    }
}

// What ends a loop's wait when another thread has made work for it. The loop calls park, looks
// once more for the work, waits on fd() and calls unpark; the other thread makes the work and
// then calls notify. The loop stores parked and then loads the work; the other thread stores the
// work and then loads parked. All four are sequentially consistent, so at least one of the two
// sees the other's store, and no wake is lost. A notify makes no system call unless the loop has
// parked.
struct Wake
{
    void park()
    {
        parked.store(true);
    }
    void unpark()
    {
        parked.store(false);
        event.clear();
    }
    void notify()
    {
        if(parked.load())
            event.signal();
    }
    int fd() const noexcept
    {
        return event.fd();
    }

  private:
    EventFd event;
    std::atomic<bool> parked{false};
};

// The indices one thread hands another: slots granted, batches delivered, batches released. The
// queue is lock-free, so a push and a pop never wait for each other, and a caller that waits for
// an index sleeps on the queue's semaphore. An fd is written only for someone who polls it.
struct IndexRing
{
    // What the fd is for.
    //   None   there is none: the consumer only ever waits in pop_until, or asks every turn.
    //   Lazy   readable while the ring holds an index, from the first time watch asks for the
    //          fd. Until then a push and a pop make no system call.
    enum class Fd
    {
        None,
        Lazy
    };
    explicit IndexRing(Fd mode = Fd::None) : fd(mode != Fd::None) {}
    // The most indices the ring holds at once. The queue grows if it is given more. Its blocks
    // are made here for two threads that push, the loop and one that signals, so that neither
    // allocates as indices go round; a block the queue makes later is freed when it empties.
    void reserve(std::uint32_t n)
    {
        q = moodycamel::BlockingConcurrentQueue<std::uint32_t>(n, 0, 2);
    }
    void push(std::uint32_t x)
    {
        if(level.load())
        {
            std::lock_guard l(m);
            q.enqueue(x);
            if(count.fetch_add(1) == 0)
                fd.signal();
        }
        else
        {
            q.enqueue(x);
            count.fetch_add(1);
            // This thread adds to count and then loads level; watch stores level and then loads
            // count. Both are sequentially consistent, so one of the two sees the other.
            if(level.load())
            {
                std::lock_guard l(m);
                if(count.load())
                    fd.signal();
            }
        }
    }
    int try_pop()
    {
        if(count.load(std::memory_order_acquire) == 0)
            return -1; // a loop asks every turn
        std::uint32_t x;
        std::uint32_t wakes = 0;
        int taken_index = -1;
        while(taken_index < 0 && q.try_dequeue(x))
            if(x == k_woken)
                ++wakes; // meant for a caller that waits in pop_until
            else
                taken_index = taken(x);
        for(; wakes; --wakes)
            q.enqueue(k_woken);
        return taken_index;
    }
    // An index, or -1 once `done` says so or the deadline has passed. `done` is asked before each
    // wait and after each signal, and never while the caller sleeps.
    template <class Done> int pop_until(Deadline d, Done done)
    {
        for(;;)
        {
            // try_pop leaves a wake in the queue: it is for a caller that already waits, which
            // would sleep on if this one took it.
            if(int index = try_pop(); index >= 0)
                return index;
            // This thread adds to waiting and then asks done; signal is called after what done
            // reads has changed, and then loads waiting. So one of the two sees the other.
            waiting.fetch_add(1);
            std::uint32_t x = k_woken;
            bool ended = done(), got = false;
            auto left = d - Deadline::clock::now();
            if(!ended && no_deadline(d))
            {
                q.wait_dequeue(x);
                got = true;
            }
            else if(!ended && left > left.zero()) // a negative time is no deadline to the queue
                got = q.wait_dequeue_timed(x, left);
            waiting.fetch_sub(1);
            if(got && x != k_woken)
                return taken(x);
            // A timed wait may end early (wait_until, above): only the steady clock says so.
            if(ended || (!got && Deadline::clock::now() >= d))
                return -1;
        }
    }
    // Sequentially consistent, for a loop that asks after Wake::park.
    bool empty() const
    {
        return count.load() == 0;
    }
    void clear()
    {
        std::lock_guard l(m);
        std::uint32_t wakes = 0;
        for(std::uint32_t x; q.try_dequeue(x);)
            wakes += x == k_woken;
        count.store(0);
        if(level.load())
            fd.clear();
        for(; wakes; --wakes)
            q.enqueue(k_woken); // still owed to a caller that waits
    }
    // Ends the wait of every caller in pop_until, so that each asks `done` again, and makes a
    // polled fd readable.
    void signal()
    {
        raised.store(true);
        for(auto sleepers = waiting.load(); sleepers; --sleepers)
            q.enqueue(k_woken);
        if(level.load())
        {
            std::lock_guard l(m);
            fd.signal();
        }
    }
    // The fd, for a caller that will poll it: from here on it is readable while the ring holds an
    // index, and after a signal.
    int watch()
    {
        std::lock_guard l(m);
        if(!level.exchange(true) && (count.load() || raised.load()))
            fd.signal();
        return fd.fd();
    }
    EventFd fd;

  private:
    // Not an index: it ends a wait, so that the waiter asks why it was woken.
    static constexpr std::uint32_t k_woken = ~std::uint32_t{0};
    int taken(std::uint32_t x)
    {
        if(level.load())
        {
            std::lock_guard l(m);
            if(count.fetch_sub(1) == 1)
                fd.clear();
        }
        else
            count.fetch_sub(1);
        return int(x);
    }
    moodycamel::BlockingConcurrentQueue<std::uint32_t> q;
    std::atomic<std::uint32_t> count{0}, waiting{0}; // indices queued; callers in pop_until
    std::atomic<bool> level{false}, raised{false};
    std::mutex m; // of a polled fd only: its writes and reads follow count in one order
};

// The loop tries to publish a copy and never waits on a thread reading it.
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

// A packed ring: an opaque stream's frames, each placed after the one before it. Positions count
// every byte since the session began, with the bytes skipped at the ring's end. A frame never
// straddles the end: it starts at the front. The publisher places each frame and states its
// position in the Frame header; the subscriber checks it against the frames it holds (Sub::fits)
// and takes nothing from the order in which headers arrive, which UCX does not promise.
inline std::uint64_t place(std::uint64_t end, std::uint64_t bytes, std::uint64_t ring)
{
    return end % ring + bytes > ring ? end + ring - end % ring : end;
}
// The longest run of frames not yet arrived that a subscriber looks across for a neighbour.
inline constexpr std::uint64_t k_max_gap = 1024;
// Room for one more grant: the most a frame may have, for it and for each grant outstanding.
// The frames a ring holds wrap at most once, and a wrap skips less than one frame, so one more
// covers it. `used` runs from the end of the last credited frame to the end of the last sent.
inline bool ring_room(std::uint64_t used, std::uint64_t grants, std::uint64_t max,
                      std::uint64_t ring)
{
    return used + (grants + 2) * max - 1 <= ring;
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
    // Signalled once for each reply that reaches Replied. A caller sleeps at once: the loop
    // answers in its own time.
    moodycamel::LightweightSemaphore replied{0, 0};
};
struct ControlTable
{
    std::array<ControlEntry, 8> entries{};
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
        if(!wait_until(e->replied, d))
        {
            // Not taken yet: nothing else will free the entry. Taken: the loop frees it.
            std::uint8_t expected = ControlEntry::Posted;
            if(e->state.compare_exchange_strong(expected, ControlEntry::Free))
                return {wire::Timeout};
            expected = ControlEntry::Taken;
            if(e->state.compare_exchange_strong(expected, ControlEntry::Abandoned))
                return {wire::Timeout};
            e->replied.wait(); // replied meanwhile: its signal must not reach the next caller
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
            // Control requests are rare beside frames. A failed compare-exchange still
            // takes the cache line exclusively, so leave inactive entries as plain reads.
            if(e.state.load(std::memory_order_acquire) != ControlEntry::Posted)
                continue;
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
        if(delivered)
            e.replied.signal();
        else
            e.state.store(ControlEntry::Free);
        return delivered;
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
    std::vector<std::uint64_t> placements; // an opaque stream's frames: position, length
    std::vector<BatchRow> batches;
    IndexRing released; // the loop asks every turn, and parks on wake to wait
    Wake wake;
    std::vector<std::uint64_t> busy, landed, returned, event_wait;
    void bit_set(std::vector<std::uint64_t> &, std::uint32_t);
    void bit_clear(std::vector<std::uint64_t> &, std::uint32_t);
    bool bit_test(const std::vector<std::uint64_t> &, std::uint32_t) const;
    std::uint32_t bit_count(const std::vector<std::uint64_t> &) const;
    std::byte *record(std::uint32_t k)
    {
        return records.data() + std::uint64_t(k) * plan.sizes.record_stride;
    }
    // A batch's owner gives it back, on whatever thread it is: the loop frees its frames.
    void release(std::uint32_t batch)
    {
        released.push(batch);
        wake.notify();
    }
    // Frame k is not directly after frame k - 1: it starts at the front of a packed ring.
    bool wraps(std::uint32_t k) const
    {
        return plan.sizes.opaque &&
               placements[2 * k] != placements[2 * k - 2] + placements[2 * k - 1];
    }
};
struct Sub final : Owner
{
    Offer offer;
    SubscriptionOptions options;
    bool every = true;
    std::uint32_t range = 0; // a puller's: the frames of a range. It is an every subscription too
    wire::Reply open_reply{};
    std::uint64_t session = 0;
    SubscriptionPlan plan;
    std::shared_ptr<Rings> rings;
    std::unique_ptr<ucp::Context> context;
    std::unique_ptr<ucp::Worker> worker;
    Region receive;
    ucp_ep_h ep = nullptr;
    std::vector<std::byte> recv_ops, ctrl_ops;
    std::uint32_t req = 0;
    std::array<std::byte, 4096> own_address{};
    std::uint16_t own_address_bytes = 0;
    std::array<std::byte, 24> credit_header{};
    IndexRing queue{IndexRing::Fd::Lazy}; // read sleeps on the queue; fd() makes it polled
    std::atomic<bool> stop{false}, close{false};
    bool ready = false, failed = false, quarantined = false, fenced = false, gap = false;
    wire::EndMessage end{};
    bool has_end = false;
    std::uint64_t assembled = 0, credited = 0, credit_sent = 0, frames = 0, skipped = 0,
                  last_index = UINT64_MAX;
    // A packed ring: one past the highest sequence arrived, and where the last credited frame ends.
    std::uint64_t arrived = 0, freed = 0;
    std::uint32_t receiving = 0, quiet_turns = 0;
    std::int64_t first_landed_ns = 0, last_landed_ns = 0, end_at_ns = 0;
    std::array<char, 256> failure{};
    Text<64> transport; // every lane, as a publisher's row keeps them: two RDMA rails take 35
    std::atomic<int> outcome{0};
    mutable std::mutex health_mutex;
    Health observation;
    std::int64_t last_health_ns = 0;
    int last_health_outcome = 0;
    Topology topology;
    std::int64_t last_topology_ns = 0;
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
    bool fits(std::uint64_t, std::uint64_t, std::uint64_t) const;
    bool assemble(std::int64_t);
    void deliver(std::uint32_t, std::uint32_t);
    void decide(std::int64_t);
    void release_pass();
    void credit_pass();
    void publish_health(std::int64_t);
    void finish_close();
    void quarantine_owner();
    void release_batch(std::uint32_t);
    void cancel_receives();
};
} // namespace TangoUcx::transport
