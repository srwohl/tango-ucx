// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-ucx project
// SPDX-License-Identifier: LGPL-3.0-or-later
#pragma once

#include "ucp.h"

#include <algorithm>
#include <cstdint>
#include <poll.h>
#include <thread>
#include <time.h>

namespace TangoUcx::transport
{
// How long a loop goes on polling after its last work. Work that follows within it, as in a
// stream at full rate, is found without the cost of a wait.
inline constexpr std::int64_t k_spin_ns = 200'000;

// What a publisher's or a subscription's loop does when a pass finds no work. It polls for
// k_spin_ns after its last work and then waits: for the worker's next event, for one more fd that
// another thread signals, or for a timeout. The timeout bounds what has no fd: a GPU event, a
// timer, a flag another thread set. With TANGO_UCX_IDLE=spin (ucp.cpp) it polls and never waits.
struct Idle
{
    const bool waits = ucp::idle_waits();
    std::int64_t last_work_ns = 0;
    unsigned turns = 0;
    // Counts a pass, and yields after 64 that found no work. True when the loop has polled for
    // long enough and may wait. A loop that is refused its wait polls on, and so still yields.
    bool due(bool work, std::int64_t now)
    {
        if(work)
        {
            last_work_ns = now;
            turns = 0;
            return false;
        }
        if(++turns > 64)
        {
            turns = 0;
            std::this_thread::yield();
        }
        return waits && now - last_work_ns >= k_spin_ns;
    }
    // Waits for an event of the worker, for fd or for the timeout. It follows a progress() that
    // returned 0, and a whole pass follows it. False, without waiting, while the worker still has
    // events: progress again.
    static bool wait(const ucp::Worker &worker, int fd, std::int64_t timeout_ns)
    {
        if(!worker.arm())
            return false;
        pollfd fds[2] = {{worker.event_fd(), POLLIN, 0}, {fd, POLLIN, 0}};
        timespec timeout{0, long(std::clamp<std::int64_t>(timeout_ns, 1'000, 999'999'999))};
        (void)::ppoll(fds, 2, &timeout, nullptr);
        return true;
    }
};
} // namespace TangoUcx::transport
