// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-ucx project
// SPDX-License-Identifier: LGPL-3.0-or-later
#include "idle.h"
#include "publisher.h"

#include <algorithm>
#include <pthread.h>
#include <stdexcept>
#include <sys/mman.h>
#include <unistd.h>
#include <utility>

namespace TangoUcx::transport
{
namespace
{
// On the progress loop, which runs it inside worker->progress().
ucs_status_t on_credit(void *arg, const void *header, size_t header_bytes, void *, size_t,
                       const ucp_am_recv_param_t *)
{
    auto *engine = static_cast<Engine *>(arg);
    wire::CreditMessage credit_message;
    if(!wire::decode({static_cast<const std::byte *>(header), header_bytes}, credit_message))
        return UCS_OK;

    // The instance that holds the session: the active one or one that is draining.
    auto *publisher = engine->owner_of(credit_message.session);
    if(!publisher)
        return UCS_OK;
    const int row_index = publisher->find_row(credit_message.session);
    if(row_index < 0)
        return UCS_OK;

    auto &row = publisher->rows[row_index];
    if(!row.live())
        return UCS_OK;

    // A subscriber cannot release more frames or bytes than the publisher has sent.
    if(!row.ring.credit(credit_message.credit, credit_message.bytes))
        publisher->retire(row_index, RetireCause::Protocol);
    return UCS_OK;
}
} // namespace

#ifndef MADV_POPULATE_WRITE
#define MADV_POPULATE_WRITE 23 // Linux 5.14; an older kernel refuses it, and the pages are touched
#endif
namespace
{
// Faults in every page of a new arena, so that no frame pays for it on the device's thread.
// TODO(numa): this runs on a helper thread that is not pinned, so first touch puts the pages on
// whatever node it runs on, not the node of the thread that writes the frames (Basler's grab
// thread) or of the NIC that reads them. On one socket that is the same node. On several, choose
// the node explicitly: a PublisherLimits field (e.g. arena_numa_node, defaulting to the node of
// net_devices or progress_cpu), then mbind(MPOL_BIND) or set_mempolicy before this touch, and
// report the result in Health's ring_numa_node, which sample_topology already measures. Measure
// a two-socket host before deciding the default.
void prefault(std::byte *base, std::uint64_t bytes) noexcept
{
    if(!base || !bytes)
        return;
    auto page = std::uintptr_t(::sysconf(_SC_PAGESIZE));
    auto begin = reinterpret_cast<std::uintptr_t>(base), end = begin + bytes;
    auto first = begin & ~(page - 1);
    if(::madvise(reinterpret_cast<void *>(first), end - first, MADV_POPULATE_WRITE) == 0)
        return;
    for(auto at = begin; at < end; at = (at & ~(page - 1)) + page)
        *reinterpret_cast<volatile std::byte *>(at) = std::byte{0};
}
} // namespace

Engine::Engine(PublisherLimits limits) : limits(std::move(limits))
{
    check_limits(this->limits);
    control.wake = &wake;
}
Engine::~Engine()
{
    stop();
}
// Ends the loop and waits for it. The loop holds a reference to its engine, so letting go of
// the last handle does not end it.
void Engine::stop()
{
    stopping = true;
    wake.poke();
    if(loop.joinable())
    {
        if(loop.get_id() == std::this_thread::get_id())
            loop.detach(); // the loop let go of the last reference: it is ending already
        else
            loop.join();
    }
}
void Engine::poke()
{
    wake.poke();
}
void Engine::start()
{
    auto ready = setup_done.get_future();
    loop = std::jthread(&Engine::run, shared_from_this());
    // A refused setup: join here, or the loop may drop the last reference and join itself.
    ready.wait();
    if(!setup_ok)
        loop.join();
    ready.get();
}
void Engine::setup()
{
    context = std::make_unique<ucp::Context>(limits.net_devices);
    worker = std::make_unique<ucp::Worker>(*context);
    request_bytes = context->request_bytes();
    max_header = worker->max_am_header();
    memory_hooks = context->memory_hooks();
    worker->set_am(wire::Credit, on_credit, this);
    setup_ok = true;
}
std::shared_ptr<Pub> Engine::instance() const
{
    std::lock_guard lock(current_lock);
    return current;
}
void Engine::set_current(std::shared_ptr<Pub> instance)
{
    std::lock_guard lock(current_lock);
    current.swap(instance); // the previous one is released after the lock
}
Pub *Engine::owner_of(std::uint64_t session) const
{
    if(active && active->find_row(session) >= 0)
        return active.get();
    for(const auto &instance : draining)
        if(instance->find_row(session) >= 0)
            return instance.get();
    return nullptr;
}
// Short waits while an instance publishes, has sessions or is draining: eviction, End and
// teardown run on the timer. Otherwise only a poke, a control request or a worker event ends it.
bool Engine::busy() const
{
    if(!draining.empty())
        return true;
    if(!active)
        return false;
    if(active->phase.was_started() && active->phase.load() != Phase::Ended)
        return true;
    return std::ranges::any_of(active->rows, &PubRow::in_use);
}
// Validated on the caller's thread, so a refused description leaves the active instance alone.
void Engine::begin(Description description)
{
    if(description.max_payload_bytes) // a maximum payload size makes this an opaque stream
        description.element = Element::Bytes;
    if(!wire::valid_text(description.application_text))
        throw std::invalid_argument("the application text is not UTF-8 or is over 2,048 bytes");
    auto request = std::make_shared<BeginRequest>();
    request->plan = plan_publisher(description, limits, request_bytes, max_header);
    request->description = std::move(description);
    auto done = request->done.get_future();
    std::lock_guard serial(begin_serial);
    // A device thread waiting in acquire returns at once.
    if(auto previous = instance())
    {
        previous->phase.finish();
        previous->grants.signal();
    }
    {
        std::lock_guard lock(begin_lock);
        if(begin_closed)
            throw std::logic_error("the publisher has closed");
        begin_request = request;
    }
    begin_posted.store(true);
    poke();
    done.get();
}
// Finishes the active instance, waits until it is quiet and moves it to the draining ones, then
// makes the requested instance. One that is still not quiet after k_close_ns may still be read
// by a send: it takes the arena with it, and the new instance gets another.
bool Engine::begin_pass(std::int64_t now)
{
    if(!making && !begin_posted.load())
        return false;
    if(active && !making)
    {
        if(active->phase.finish())
            active->grants.signal();
        if(!begin_since_ns)
            begin_since_ns = now;
        bool quiet = active->quiet();
        if(!quiet && now - begin_since_ns <= k_close_ns)
            return false;
        if(limits.gpu < 0 && (!quiet || active->quarantined))
            active->kept_arena = std::move(arena);
        active->retire_all();
        draining.push_back(std::move(active));
        active.reset();
        // Other threads still see it, finished, until the next instance replaces it: UcxStream
        // and Health keep answering while the next is made.
    }
    begin_since_ns = 0;
    if(!making)
    {
        {
            std::lock_guard lock(begin_lock);
            making = std::move(begin_request);
            begin_posted.store(false);
        }
        // A ring larger than the arena needs another. Allocating it here is quick; its pages are
        // faulted in on a helper thread, so the loop goes on serving the draining instances.
        // Only a quiet instance can be draining with this arena, and it sends no more.
        if(limits.gpu < 0 && arena.bytes() < making->plan.source_bytes)
            try
            {
                arena.unmap();
                arena = Region::allocate(context->get(), making->plan.source_bytes);
                prefaulting = std::async(std::launch::async,
                                         [this, base = arena.base(), bytes = arena.bytes()]
                                         {
                                             prefault(base, bytes);
                                             poke();
                                         });
            }
            catch(...)
            {
                set_current(nullptr);
                making->done.set_exception(std::current_exception());
                making.reset();
                return true;
            }
    }
    if(prefaulting.valid())
    {
        if(prefaulting.wait_for(std::chrono::seconds(0)) != std::future_status::ready)
            return false;
        prefaulting.get();
    }
    try
    {
        make_instance(*making);
        making->done.set_value();
    }
    catch(...)
    {
        set_current(nullptr); // the previous instance has finished, and no other was made
        making->done.set_exception(std::current_exception());
    }
    making.reset();
    return true;
}
void Engine::make_instance(BeginRequest &request)
{
    auto instance =
        std::make_shared<Pub>(shared_from_this(), std::move(request.description), request.plan);
    if(limits.gpu < 0)
    {
        instance->source_base = arena.base();
        instance->source_memh = arena.memh();
    }
    instance->setup();
    active = instance;
    set_current(std::move(instance));
}
// Opens go to the active instance, which checks the instance ID. Closes go to the instance that
// holds the session.
bool Engine::control_pass()
{
    return control.drain(
        [&](ControlEntry &entry)
        {
            if(!entry.in.open)
            {
                if(auto *owner = owner_of(entry.in.close.session))
                    owner->close(entry);
                else
                    control.reply(entry, {wire::UnknownSession});
                return;
            }
            if(active)
            {
                active->open(entry);
                return;
            }
            bool earlier = std::ranges::any_of(
                draining, [&](const auto &instance)
                { return instance->instance_id == entry.in.open->instance_id; });
            control.reply(entry, {earlier ? wire::Finished : wire::InstanceId});
        });
}
void Engine::run()
{
    // What a thread list and a profile show: it tells the loop's CPU time from the device's.
    pthread_setname_np(pthread_self(), "tango-ucx-pub");
    ProgressCore core;
    try
    {
        core.pin(limits.progress_cpu);
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
        shutdown(); // no instance was made, so this only releases what setup created
        return;
    }
    Idle idle;
    std::int64_t last_timer_ns = 0;
    while(!stopping.load(std::memory_order_acquire))
    {
        auto now = now_ns();
        bool work = control_pass();
        work |= begin_pass(now);
        if(active)
            work |= active->route_pass(now);
        for(auto &instance : draining)
            work |= instance->route_pass(now);
        work |= worker->progress() != 0;
        if(active)
        {
            work |= active->grant_pass();
            work |= active->retire_pass(now);
        }
        for(auto &instance : draining)
            work |= instance->retire_pass(now);
        work |= std::erase_if(draining,
                              [](const std::shared_ptr<Pub> &instance)
                              {
                                  if(instance->quarantined)
                                      return true; // it keeps what it holds
                                  if(!instance->done())
                                      return false;
                                  instance->release_memory();
                                  return true;
                              }) != 0;
        if(now - last_timer_ns >= k_timer_ns)
        {
            last_timer_ns = now;
            if(active)
                active->timer_pass(now);
        }
        if(idle.due(work, now))
        {
            // From here on a posted slot or a poke ends the wait. Either may have come before
            // that, or the next slot may wait for its GPU event, which has no fd and is short:
            // then the loop polls on. Everything but a slot pokes.
            wake.park();
            bool posted = wake.poked() || (active && active->next_posted());
            if(!posted)
                Idle::wait(*worker, wake.fd(), busy() ? k_timer_ns : k_idle_ns);
            wake.unpark();
        }
    }
    shutdown();
}
// Every session of every instance is retired and closed within one bound. What cannot be shown
// safe to release, because a session did not close or an instance was quarantined, is kept.
void Engine::shutdown()
{
    if(prefaulting.valid())
        prefaulting.wait(); // it writes to the arena, which is unmapped below
    if(making)
        making->done.set_exception(
            std::make_exception_ptr(std::logic_error("the publisher has closed")));
    making.reset();
    {
        std::lock_guard lock(begin_lock);
        begin_closed = true;
        if(begin_request)
            begin_request->done.set_exception(
                std::make_exception_ptr(std::logic_error("the publisher has closed")));
        begin_request.reset();
        begin_posted.store(false);
    }
    if(active)
        draining.push_back(std::move(active));
    active.reset();
    set_current(nullptr);
    for(auto &instance : draining)
        instance->retire_all();
    auto in_use = [&]
    {
        return std::ranges::any_of(
            draining, [](const auto &instance)
            { return std::ranges::any_of(instance->rows, &PubRow::in_use); });
    };
    auto deadline = now_ns() + k_close_ns + k_quarantine_ns;
    while(worker && in_use() && now_ns() < deadline)
    {
        control_pass();
        worker->progress();
        for(auto &instance : draining)
            if(!instance->quarantined)
                instance->retire_pass(now_ns());
        std::this_thread::yield();
    }
    if(in_use() || tainted)
    {
        if(!quarantined)
        {
            quarantined = true;
            self_keep = shared_from_this();
            quarantine().keep(*this, arena.bytes());
        }
        return;
    }
    for(auto &instance : draining)
        instance->release_memory();
    draining.clear(); // an instance a slot still holds keeps the engine, not its memory
    arena.unmap();
    if(worker)
        worker->destroy();
    if(context)
        context->destroy();
}
} // namespace TangoUcx::transport
