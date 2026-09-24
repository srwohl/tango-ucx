// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
// SPDX-License-Identifier: LGPL-3.0-or-later
#include "tables.h"

#include <cstring>
#include <stdexcept>
#include <utility>

namespace TangoBulk
{
struct Batch::Copy
{
    std::vector<std::byte> payload, records;
};
Batch::Batch(std::shared_ptr<transport::Rings> r, std::uint32_t first)
    : rings_(std::move(r)), first_(first)
{
    if(!rings_ || first >= rings_->plan.D)
        throw std::out_of_range("batch row");
    const auto &row = rings_->batches[first];
    frames_ = row.frames;
    frame_bytes_ = rings_->plan.sizes.frame_bytes;
    record_stride_ = rings_->plan.sizes.record_stride;
    payload_ = rings_->payload + std::uint64_t(first) * frame_bytes_;
    records_ = rings_->record(first);
}
Batch::Batch(Batch &&o) noexcept
    : rings_(std::move(o.rings_)), copy_(std::move(o.copy_)),
      first_(std::exchange(o.first_, ~std::uint32_t{0})), frames_(o.frames_),
      frame_bytes_(o.frame_bytes_), record_stride_(o.record_stride_), payload_(o.payload_),
      records_(o.records_)
{
    o.frames_ = 0;
    o.payload_ = o.records_ = nullptr;
}
Batch &Batch::operator=(Batch &&o) noexcept
{
    if(this != &o)
    {
        if(rings_ && first_ != ~std::uint32_t{0})
            rings_->released.post(first_);
        rings_ = std::move(o.rings_);
        copy_ = std::move(o.copy_);
        first_ = std::exchange(o.first_, ~std::uint32_t{0});
        frames_ = o.frames_;
        frame_bytes_ = o.frame_bytes_;
        record_stride_ = o.record_stride_;
        payload_ = o.payload_;
        records_ = o.records_;
        o.frames_ = 0;
        o.payload_ = o.records_ = nullptr;
    }
    return *this;
}
Batch::~Batch()
{
    if(rings_ && first_ != ~std::uint32_t{0})
        rings_->released.post(first_);
}
std::size_t Batch::frames() const
{
    return frames_;
}
std::uint64_t Batch::index(std::size_t i) const
{
    if(i >= frames_ || !records_)
        throw std::out_of_range("batch frame");
    return wire::get64(records_ + i * record_stride_);
}
const void *Batch::payload() const
{
    return payload_;
}
const void *Batch::records() const
{
    return records_;
}
bool Batch::gpu() const
{
    return false;
}
Batch Batch::copy() const
{
    if(!payload_ || !records_)
        throw std::logic_error("empty batch");
    auto c = std::make_shared<Copy>();
    c->payload.resize(frames_ * frame_bytes_);
    c->records.resize(frames_ * record_stride_);
    std::memcpy(c->payload.data(), payload_, c->payload.size());
    std::memcpy(c->records.data(), records_, c->records.size());
    Batch b;
    b.copy_ = std::move(c);
    b.frames_ = frames_;
    b.frame_bytes_ = frame_bytes_;
    b.record_stride_ = record_stride_;
    b.payload_ = b.copy_->payload.data();
    b.records_ = b.copy_->records.data();
    return b;
}
} // namespace TangoBulk
