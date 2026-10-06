// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-ucx project
// SPDX-License-Identifier: LGPL-3.0-or-later
#pragma once

#include <ucp/api/ucp.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <span>

namespace TangoUcx::transport::ucp
{
// False with TANGO_UCX_IDLE=spin: the loops only poll, and no worker has an event fd.
bool idle_waits();
class Context
{
  public:
    Context();
    ~Context();
    Context(const Context &) = delete;
    Context &operator=(const Context &) = delete;
    ucp_context_h get() const noexcept
    {
        return context_;
    }
    std::uint32_t request_bytes() const noexcept
    {
        return request_bytes_;
    }
    bool memory_hooks() const noexcept
    {
        return memory_hooks_;
    }
    void destroy() noexcept;

  private:
    ucp_context_h context_ = nullptr;
    std::uint32_t request_bytes_ = 0;
    bool memory_hooks_ = false;
};
class Worker
{
  public:
    explicit Worker(Context &);
    ~Worker();
    Worker(const Worker &) = delete;
    Worker &operator=(const Worker &) = delete;
    ucp_worker_h get() const noexcept
    {
        return worker_;
    }
    std::span<const std::byte> address() const noexcept
    {
        return {address_.data(), address_bytes_};
    }
    std::uint64_t max_am_header() const noexcept
    {
        return max_am_header_;
    }
    unsigned progress() const
    {
        return ucp_worker_progress(worker_);
    }
    // The fd that reports the worker's next event once armed; -1 when the loops only poll.
    int event_fd() const noexcept
    {
        return event_fd_;
    }
    // Arms the event fd, after a progress() that returned 0. False while events are still
    // pending: progress again instead of waiting.
    bool arm() const
    {
        return event_fd_ >= 0 && ucp_worker_arm(worker_) == UCS_OK;
    }
    ucp_ep_h endpoint(std::span<const std::byte>, void *, ucp_err_handler_cb_t);
    void set_am(unsigned, ucp_am_recv_callback_t, void *);
    std::size_t transports(ucp_ep_h, std::span<char>) const;
    bool only_tcp(ucp_ep_h) const; // every lane is tcp: no shared memory, no RDMA
    void destroy() noexcept;

  private:
    ucp_worker_h worker_ = nullptr;
    int event_fd_ = -1;
    std::array<std::byte, 4096> address_{};
    std::size_t address_bytes_ = 0, max_am_header_ = 0;
};
void check(ucs_status_t, const char *);
} // namespace TangoUcx::transport::ucp
