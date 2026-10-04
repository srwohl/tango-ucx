// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-ucx project
// SPDX-License-Identifier: LGPL-3.0-or-later
#pragma once

#include "memory.h"

#include <algorithm>
#include <array>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <vector>

namespace TangoUcx::transport
{
enum class OpKind : std::uint8_t
{
    Frame,
    Ready,
    End,
    Flush,
    Close
};
struct OpMeta
{
    void *owner = nullptr;
    std::uint32_t slot = 0;
    std::uint16_t row = 0;
    OpKind kind = OpKind::Frame;
    bool in_flight = false;
    std::int64_t sent_ns = 0;
};

// UCX's request prefix, metadata and wire header share one allocation, made once at setup.
class PublisherOperations
{
  public:
    PublisherOperations() = default;
    PublisherOperations(const PublisherOperations &) = delete;
    PublisherOperations &operator=(const PublisherOperations &) = delete;
    void allocate(void *owner, const PublisherPlan &p, std::uint32_t request_bytes)
    {
        assert(storage.empty());
        slot_count = p.slot_count;
        row_count = p.row_count;
        op_stride = p.op_stride;
        this->request_bytes = request_bytes;
        header_bytes = p.sizes.header_bytes;
        frame_count = std::uint64_t(slot_count) * row_count;
        assert(slot_count && row_count);
        assert(std::uint64_t(request_bytes) + sizeof(OpMeta) + header_bytes <= op_stride);
        assert(op_stride % alignof(OpMeta) == 0);
        assert(p.ops_bytes == frame_count * op_stride);
        storage.resize(frame_count * op_stride + control_bytes(row_count, op_stride));
        for(std::uint32_t slot = 0; slot < slot_count; ++slot)
            for(std::uint32_t row = 0; row < row_count; ++row)
                std::construct_at(frame(slot, row),
                                  OpMeta{owner, slot, std::uint16_t(row), OpKind::Frame});
        for(std::uint32_t row = 0; row < row_count; ++row)
            for(auto kind : control_kinds)
                std::construct_at(control(row, kind), OpMeta{owner, 0, std::uint16_t(row), kind});
    }
    OpMeta *frame(std::uint32_t slot, std::uint32_t row)
    {
        assert(slot < slot_count && row < row_count);
        return at(std::uint64_t(slot) * row_count + row);
    }
    OpMeta *control(std::uint32_t row, OpKind kind)
    {
        assert(row < row_count);
        auto found = std::find(control_kinds.begin(), control_kinds.end(), kind);
        assert(found != control_kinds.end());
        return at(frame_count + std::uint64_t(row) * control_kinds.size() +
                  (found - control_kinds.begin()));
    }
    std::span<std::byte> header(OpMeta *operation) const
    {
        assert(!storage.empty() && operation);
        return {reinterpret_cast<std::byte *>(operation) + sizeof(OpMeta), header_bytes};
    }
    static std::uint64_t control_bytes(std::uint32_t rows, std::uint32_t stride)
    {
        return std::uint64_t(rows) * control_kinds.size() * stride;
    }

  private:
    static constexpr std::array control_kinds{OpKind::Ready, OpKind::End, OpKind::Flush,
                                              OpKind::Close};
    std::vector<std::byte> storage;
    std::uint64_t frame_count = 0;
    std::uint32_t slot_count = 0, row_count = 0, op_stride = 0, request_bytes = 0, header_bytes = 0;
    OpMeta *at(std::uint64_t index)
    {
        assert(!storage.empty());
        auto *address = storage.data() + index * op_stride + request_bytes;
        assert(reinterpret_cast<std::uintptr_t>(address) % alignof(OpMeta) == 0);
        return reinterpret_cast<OpMeta *>(address);
    }
};
} // namespace TangoUcx::transport
