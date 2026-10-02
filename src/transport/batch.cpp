// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-ucx project
// SPDX-License-Identifier: LGPL-3.0-or-later
#include "tables.h"

#include <cstring>
#include <stdexcept>
#include <utility>

namespace TangoUcx
{
struct Batch::Copy
{
    std::vector<std::byte> payload, records;
    std::vector<std::uint64_t> placements;
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
    if(rings_->plan.sizes.opaque)
    {
        placements_ = rings_->placements.data() + 2ull * first;
        payload_ = rings_->payload + placements_[0] % rings_->plan.payload_bytes;
    }
}
Batch::Batch(Batch &&o) noexcept
    : rings_(std::move(o.rings_)), copy_(std::move(o.copy_)),
      first_(std::exchange(o.first_, ~std::uint32_t{0})), frames_(o.frames_),
      frame_bytes_(o.frame_bytes_), record_stride_(o.record_stride_), payload_(o.payload_),
      records_(o.records_), placements_(o.placements_)
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
        placements_ = o.placements_;
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
// An opaque batch's frames follow one another in the ring, so positions counted from its first
// frame's hold in a copy too.
std::span<const std::byte> Batch::payload(std::size_t i) const
{
    if(i >= frames_ || !payload_)
        throw std::out_of_range("batch frame");
    if(gpu())
        throw std::logic_error("a GPU batch's frames are in GPU memory");
    if(!placements_)
        return {payload_ + i * frame_bytes_, frame_bytes_};
    return {payload_ + (placements_[2 * i] - placements_[0]), std::size_t(placements_[2 * i + 1])};
}
const void *Batch::payload() const
{
    if(placements_)
        throw std::logic_error("an opaque batch has no packed array: read each frame");
    return payload_;
}
const void *Batch::records() const
{
    return records_;
}
bool Batch::gpu() const
{
    return rings_ && rings_->plan.gpu;
}
Batch Batch::copy() const
{
    if(!payload_ || !records_)
        throw std::logic_error("empty batch");
    auto c = std::make_shared<Copy>();
    if(placements_)
        c->placements.assign(placements_, placements_ + 2 * frames_);
    c->payload.resize(placements_ ? payload(frames_ - 1).data() + payload(frames_ - 1).size() - payload_
                               : frames_ * frame_bytes_);
    c->records.resize(frames_ * record_stride_);
    if(gpu())
        transport::gpu_runtime()->copy_to_host(c->payload.data(), payload_, c->payload.size());
    else
        std::memcpy(c->payload.data(), payload_, c->payload.size());
    std::memcpy(c->records.data(), records_, c->records.size());
    Batch b;
    b.copy_ = std::move(c);
    b.frames_ = frames_;
    b.frame_bytes_ = frame_bytes_;
    b.record_stride_ = record_stride_;
    b.payload_ = b.copy_->payload.data();
    b.records_ = b.copy_->records.data();
    b.placements_ = placements_ ? b.copy_->placements.data() : nullptr;
    return b;
}
// The view ends when its hold does: the event is recorded after every kernel the application
// queued on the stream, and the loop frees the batch once that event is done.
std::shared_ptr<void> transport::begin_gpu_view(const Batch &b, GpuStream stream)
{
    if(!b.gpu() || b.first_ == ~std::uint32_t{0} || b.rings_->batches[b.first_].viewed)
        throw std::logic_error("gpu_view needs a GPU batch; a batch has one GPU view");
    auto &row = b.rings_->batches[b.first_];
    row.viewed = true;
    row.gpu_stream = stream;
    return std::shared_ptr<void>(
        &row,
        [rings = b.rings_](void *p)
        {
            auto &row = *static_cast<transport::BatchRow *>(p);
            transport::gpu_runtime()->record(row.event, row.gpu_stream);
            row.recorded.store(true, std::memory_order_release); // the loop may query the event now
        });
}
GpuView Batch::gpu_view(GpuStream stream) &&
{
    GpuView view;
    view.hold_ = transport::begin_gpu_view(*this, stream);
    view.batch_ = std::move(*this);
    return view;
}
const void *GpuView::payload() const
{
    return batch_.payload();
}
const void *GpuView::records() const
{
    return batch_.records();
}
std::size_t GpuView::frames() const
{
    return batch_.frames();
}
} // namespace TangoUcx
