// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
// SPDX-License-Identifier: LGPL-3.0-or-later
#pragma once

#include <ucp/api/ucp.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <span>

namespace TangoBulk::transport::ucx
{
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
    ucp_ep_h endpoint(std::span<const std::byte>, void *, ucp_err_handler_cb_t);
    void set_am(unsigned, ucp_am_recv_callback_t, void *);
    std::size_t transports(ucp_ep_h, std::span<char>) const;
    void destroy() noexcept;

  private:
    ucp_worker_h worker_ = nullptr;
    std::array<std::byte, 4096> address_{};
    std::size_t address_bytes_ = 0, max_am_header_ = 0;
};
void check(ucs_status_t, const char *);
} // namespace TangoBulk::transport::ucx
