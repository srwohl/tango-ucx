// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
// SPDX-License-Identifier: LGPL-3.0-or-later
#pragma once

#include <tango-bulk/bulk.h>
#include <ucp/api/ucp.h>

#include <cstddef>
#include <cstdint>
#include <memory>

namespace TangoBulk::transport
{
struct Offer;
struct Sizes
{
    std::uint64_t frame_bytes = 0;
    std::uint32_t field_bytes = 0, padded_fields = 0, record_stride = 0, header_bytes = 0;
};
struct PublisherPlan
{
    Sizes sizes;
    std::uint32_t S = 0, M = 0, op_stride = 0;
    std::uint64_t source_bytes = 0, records_bytes = 0, ops_bytes = 0, charge = 0;
};
struct SubscriptionPlan
{
    Sizes sizes;
    std::uint32_t D = 0, batch = 0, op_stride = 0;
    std::uint64_t payload_bytes = 0, records_bytes = 0, recv_ops_bytes = 0, charge = 0,
                  max_wait_ns = 0;
};

Sizes sizes_of(const Description &);
PublisherPlan plan_publisher(const Description &, const PublisherLimits &,
                             std::uint32_t request_bytes, std::uint64_t max_header);
SubscriptionPlan plan_subscription(const Offer &, bool every, const SubscriptionOptions &,
                                   std::uint32_t request_bytes);

class Region
{
  public:
    Region() = default;
    ~Region();
    Region(Region &&) noexcept;
    Region &operator=(Region &&) noexcept;
    Region(const Region &) = delete;
    Region &operator=(const Region &) = delete;
    static Region allocate(ucp_context_h, std::uint64_t);
    static Region adopt(ucp_context_h, std::shared_ptr<void>, std::uint64_t);
    void unmap() noexcept;
    std::byte *base() const noexcept
    {
        return base_;
    }
    std::uint64_t bytes() const noexcept
    {
        return bytes_;
    }
    ucp_mem_h memh() const noexcept
    {
        return memh_;
    }
    std::byte *at(std::uint64_t offset) const noexcept
    {
        return base_ + offset;
    }
    std::shared_ptr<void> owner() const noexcept
    {
        return owner_;
    }

  private:
    ucp_context_h context_ = nullptr;
    ucp_mem_h memh_ = nullptr;
    std::byte *base_ = nullptr;
    std::uint64_t bytes_ = 0, reserved_ = 0;
    std::shared_ptr<void> owner_;
};
std::uint64_t pinned_bytes() noexcept;
struct GpuRuntime;
GpuRuntime *gpu_runtime() noexcept; // the registered one; null when none is linked
} // namespace TangoBulk::transport
