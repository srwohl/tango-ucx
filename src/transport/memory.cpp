// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
// SPDX-License-Identifier: LGPL-3.0-or-later
#include "memory.h"
#include "transport.h"
#include "wire.h"

#include <algorithm>
#include <atomic>
#include <cstring>
#include <format>
#include <limits>
#include <stdexcept>
#include <sys/resource.h>

namespace TangoBulk::transport
{
namespace
{
constexpr std::uint32_t k_max_sessions = 64, k_max_source_slots = 1024, k_max_depth = 65536,
                        k_max_gpu_depth = 4096; // one GPU event per batch row
constexpr std::uint64_t k_max_frame = std::uint64_t{1} << 31, k_region_charge = 64ull << 10;
constexpr std::uint64_t k_pub_fixed =
    8ull * (sizeof(wire::Request) + sizeof(wire::Reply)) + 64ull * 384 + 16ull * 1024;
constexpr std::uint64_t k_sub_fixed = 48ull * 1024;
std::atomic<std::uint64_t> pinned{0};
GpuRuntime *registered_gpu = nullptr; // set during static initialization, read after it
std::uint64_t add(std::initializer_list<std::uint64_t> xs)
{
    std::uint64_t r = 0;
    for(auto x : xs)
    {
        if(x > UINT64_MAX - r)
            throw std::invalid_argument("size overflows 64 bits");
        r += x;
    }
    return r;
}
std::uint64_t mul(std::uint64_t a, std::uint64_t b)
{
    if(a && b > UINT64_MAX / a)
        throw std::invalid_argument("size overflows 64 bits");
    return a * b;
}
std::uint64_t align(std::uint64_t n, std::uint64_t a)
{
    return add({n, a - 1}) & ~(a - 1);
}
std::uint64_t element_bytes(Element e)
{
    switch(e)
    {
        case Element::U8:
        case Element::I8:
        case Element::Bytes:
            return 1;
        case Element::U16:
        case Element::I16:
            return 2;
        case Element::U32:
        case Element::I32:
        case Element::F32:
            return 4;
        case Element::U64:
        case Element::I64:
        case Element::F64:
            return 8;
    }
    throw std::invalid_argument("unknown element type");
}
void reserve_pin(std::uint64_t n)
{
    rlimit lim{};
    if(getrlimit(RLIMIT_MEMLOCK, &lim) == 0 && lim.rlim_cur != RLIM_INFINITY && n > lim.rlim_cur)
        throw std::runtime_error(
            std::format("registration needs {} bytes, above RLIMIT_MEMLOCK {}", n, lim.rlim_cur));
    auto old = pinned.fetch_add(n, std::memory_order_acq_rel);
    if(n > UINT64_MAX - old)
    {
        pinned.fetch_sub(n);
        throw std::invalid_argument("process registration total overflows");
    }
    if(getrlimit(RLIMIT_MEMLOCK, &lim) == 0 && lim.rlim_cur != RLIM_INFINITY &&
       old + n > lim.rlim_cur)
    {
        pinned.fetch_sub(n);
        throw std::runtime_error(std::format(
            "process registration total {} exceeds RLIMIT_MEMLOCK {}", old + n, lim.rlim_cur));
    }
}
} // namespace

Sizes sizes_of(const Description &d)
{
    if(d.shape.empty() || d.shape.size() > 8)
        throw std::invalid_argument("a shape needs 1 to 8 extents");
    auto n = element_bytes(d.element);
    for(auto e : d.shape)
    {
        if(!e)
            throw std::invalid_argument("a zero extent");
        n = mul(n, e);
    }
    if(n > k_max_frame)
        throw std::invalid_argument("a frame over 2 GiB");
    if(d.field_bytes > 1024)
        throw std::invalid_argument("application fields over 1,024 bytes");
    auto padded = std::uint32_t(align(d.field_bytes, 8));
    return {n, d.field_bytes, padded, std::uint32_t(16 + padded), std::uint32_t(40 + padded)};
}

PublisherPlan plan_publisher(const Description &d, const PublisherLimits &l, std::uint32_t request,
                             std::uint64_t max_header)
{
    PublisherPlan p;
    p.sizes = sizes_of(d);
    if(l.max_sessions < 1 || l.max_sessions > k_max_sessions)
        throw std::invalid_argument("max_sessions must be 1 to 64");
    if(request > 1024)
        throw std::invalid_argument("UCX request is larger than 1 KiB");
    if(p.sizes.header_bytes > max_header)
        throw std::invalid_argument("frame header does not fit UCX max AM header");
    p.M = l.max_sessions;
    p.op_stride = std::uint32_t(align(add({request, 24, p.sizes.header_bytes}), 64));
    auto per = add({p.sizes.frame_bytes, p.sizes.record_stride, mul(p.M, p.op_stride),
                    sizeof(std::uint64_t) + sizeof(std::uint16_t)});
    auto fixed = add({k_pub_fixed, mul(4ull * p.M, p.op_stride), k_region_charge});
    auto fits = l.budget > fixed ? (l.budget - fixed) / per : 0;
    p.S = std::uint32_t(std::min<std::uint64_t>(fits, k_max_source_slots));
    if(p.S < p.M + 2)
        throw std::invalid_argument(
            std::format("budget {} holds {} source slots of {} bytes; {} sessions need {}",
                        l.budget, fits, per, p.M, p.M + 2));
    p.source_bytes = mul(p.S, p.sizes.frame_bytes);
    p.records_bytes = mul(p.S, p.sizes.record_stride);
    p.ops_bytes = mul(mul(p.S, p.M), p.op_stride);
    p.charge = add({fixed, mul(p.S, per)});
    return p;
}

SubscriptionPlan plan_subscription(const Offer &o, bool every, const SubscriptionOptions &options,
                                   std::uint32_t request)
{
    SubscriptionPlan p;
    p.sizes = sizes_of(o.description);
    if(every && !o.limits.allow_every)
        throw std::invalid_argument("publisher refuses every subscriptions");
    if(request > 1024)
        throw std::invalid_argument("UCX request is larger than 1 KiB");
    if(options.batch < 1)
        throw std::invalid_argument("batch must be at least one");
    if(options.label.size() > 64)
        throw std::invalid_argument("label exceeds 64 bytes");
    if(options.gpu >= 0 && !registered_gpu)
        throw std::invalid_argument("GPU receive needs a GPU runtime: link tango-bulk-cuda");
    p.gpu = options.gpu >= 0;
    if(!(options.max_rate >= 0) || (every && options.max_rate > 0))
        throw std::invalid_argument("max_rate must be 0 or more, and applies to latest only");
    p.batch = options.batch;
    p.max_wait_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(options.max_wait).count();
    p.op_stride = std::uint32_t(align(add({request, 16}), 64));
    auto per = add({p.sizes.frame_bytes, p.sizes.record_stride, p.op_stride, 64, 12});
    auto fixed = add({k_sub_fixed, mul(3, p.op_stride), k_region_charge});
    auto fits = options.budget > fixed ? (options.budget - fixed) / per : 0;
    auto depth = std::min<std::uint64_t>(fits, p.gpu ? k_max_gpu_depth : k_max_depth);
    depth -= depth % p.batch;
    if(depth < 2ull * p.batch)
        throw std::invalid_argument(std::format(
            "budget {} holds {} frames of {} bytes; two batches of {} need {} bytes",
            options.budget, fits, per, p.batch, add({fixed, mul(2ull * p.batch, per)})));
    p.D = std::uint32_t(depth);
    p.payload_bytes = mul(p.D, p.sizes.frame_bytes);
    p.records_bytes = mul(p.D, p.sizes.record_stride);
    p.recv_ops_bytes = mul(p.D, p.op_stride);
    p.charge = add({fixed, mul(p.D, per)});
    return p;
}

Region::~Region()
{
    unmap();
}
Region::Region(Region &&o) noexcept
    : context_(o.context_), memh_(o.memh_), base_(o.base_), bytes_(o.bytes_),
      reserved_(o.reserved_), owner_(std::move(o.owner_))
{
    o.memh_ = nullptr;
    o.base_ = nullptr;
    o.bytes_ = o.reserved_ = 0;
}
Region &Region::operator=(Region &&o) noexcept
{
    if(this != &o)
    {
        unmap();
        context_ = o.context_;
        memh_ = o.memh_;
        base_ = o.base_;
        bytes_ = o.bytes_;
        reserved_ = o.reserved_;
        owner_ = std::move(o.owner_);
        o.memh_ = nullptr;
        o.base_ = nullptr;
        o.bytes_ = o.reserved_ = 0;
    }
    return *this;
}
Region Region::allocate(ucp_context_h ctx, std::uint64_t bytes)
{
    Region r;
    r.context_ = ctx;
    reserve_pin(bytes);
    r.reserved_ = bytes;
    ucp_mem_map_params_t p{};
    p.field_mask = UCP_MEM_MAP_PARAM_FIELD_LENGTH | UCP_MEM_MAP_PARAM_FIELD_FLAGS;
    p.length = bytes;
    p.flags = UCP_MEM_MAP_ALLOCATE;
    auto st = ucp_mem_map(ctx, &p, &r.memh_);
    if(st != UCS_OK)
    {
        r.unmap();
        throw std::runtime_error(std::string("ucp_mem_map: ") + ucs_status_string(st));
    }
    ucp_mem_attr_t a{};
    a.field_mask = UCP_MEM_ATTR_FIELD_ADDRESS | UCP_MEM_ATTR_FIELD_LENGTH;
    st = ucp_mem_query(r.memh_, &a);
    if(st != UCS_OK || a.length < bytes)
    {
        r.unmap();
        throw std::runtime_error("ucp_mem_query returned an invalid region");
    }
    if(a.length > r.reserved_)
    {
        auto extra = a.length - r.reserved_;
        reserve_pin(extra);
        r.reserved_ += extra;
    }
    r.base_ = static_cast<std::byte *>(a.address);
    r.bytes_ = a.length;
    return r;
}
Region Region::adopt(ucp_context_h ctx, std::shared_ptr<void> owner, std::uint64_t bytes,
                     ucs_memory_type_t type)
{
    Region r;
    r.context_ = ctx;
    r.owner_ = std::move(owner);
    auto pin = type == UCS_MEMORY_TYPE_HOST ? bytes : 0; // GPU memory is not locked host memory
    reserve_pin(pin);
    r.reserved_ = pin;
    ucp_mem_map_params_t p{};
    p.field_mask = UCP_MEM_MAP_PARAM_FIELD_ADDRESS | UCP_MEM_MAP_PARAM_FIELD_LENGTH |
                   UCP_MEM_MAP_PARAM_FIELD_MEMORY_TYPE;
    p.address = r.owner_.get();
    p.length = bytes;
    p.memory_type = type;
    auto st = ucp_mem_map(ctx, &p, &r.memh_);
    if(st != UCS_OK)
    {
        r.unmap();
        throw std::runtime_error(std::string("ucp_mem_map(adopted): ") + ucs_status_string(st));
    }
    ucp_mem_attr_t a{};
    a.field_mask = UCP_MEM_ATTR_FIELD_ADDRESS | UCP_MEM_ATTR_FIELD_LENGTH;
    st = ucp_mem_query(r.memh_, &a);
    if(st != UCS_OK || a.length < bytes)
    {
        r.unmap();
        throw std::runtime_error("ucp_mem_query returned an invalid adopted region");
    }
    r.base_ = static_cast<std::byte *>(a.address);
    r.bytes_ = a.length;
    return r;
}
void Region::unmap() noexcept
{
    if(memh_)
        ucp_mem_unmap(context_, memh_);
    memh_ = nullptr;
    base_ = nullptr;
    bytes_ = 0;
    owner_.reset();
    if(reserved_)
        pinned.fetch_sub(reserved_);
    reserved_ = 0;
}
std::uint64_t pinned_bytes() noexcept
{
    return pinned.load(std::memory_order_acquire);
}
void register_gpu_runtime(GpuRuntime &r)
{
    registered_gpu = &r;
}
GpuRuntime *gpu_runtime() noexcept
{
    return registered_gpu;
}
} // namespace TangoBulk::transport
