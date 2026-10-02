// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-ucx project
// SPDX-License-Identifier: LGPL-3.0-or-later
#pragma once

#include <tango-ucx/ucx.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <iterator>
#include <span>
#include <stdexcept>
#include <sys/uio.h>
#include <system_error>
#include <vector>

// One batch of an opaque stream as one archive chunk, independent of any codec:
//
//   ChunkHeader | frames x {offset, bytes} | frames x record | blob
//
// The blob is the batch's frames as they lie in the receive ring: an opaque batch is one run of
// bytes, so nothing is gathered and the chunk is written with one writev from the ring.
namespace archive
{
struct ChunkHeader
{
    std::uint64_t frames = 0, record_stride = 0, blob_bytes = 0;
};
struct Extent
{
    std::uint64_t offset = 0, bytes = 0; // of one frame, in the blob
};
// A record is its frame index and timestamp, then the application fields padded to 8 bytes.
inline std::size_t record_stride(const TangoUcx::Description &d)
{
    return 16 + ((std::size_t(d.field_bytes) + 7) & ~std::size_t{7});
}

// Writes the batch as one chunk; returns the chunk's size.
inline std::size_t write_chunk(int fd, const TangoUcx::Batch &batch, std::size_t stride)
{
    std::vector<Extent> extents(batch.frames());
    auto first = batch.payload(0);
    for(std::size_t i = 0; i < batch.frames(); ++i)
    {
        auto frame = batch.payload(i);
        extents[i] = {std::uint64_t(frame.data() - first.data()), frame.size()};
    }
    ChunkHeader header{batch.frames(), stride, extents.back().offset + extents.back().bytes};
    iovec parts[] = {{&header, sizeof(header)},
                     {extents.data(), extents.size() * sizeof(Extent)},
                     {const_cast<void *>(batch.records()), batch.frames() * stride},
                     {const_cast<std::byte *>(first.data()), std::size_t(header.blob_bytes)}};
    std::size_t total = 0;
    for(auto &p : parts)
        total += p.iov_len;
    for(std::size_t part = 0, left = total; left;)
    {
        auto n = ::writev(fd, parts + part, int(std::size(parts) - part));
        if(n < 0 && errno == EINTR)
            continue;
        if(n < 0)
            throw std::system_error(errno, std::generic_category(), "writev");
        left -= std::size_t(n);
        for(auto done = std::size_t(n); done;) // a short write: go on from where it stopped
        {
            auto take = std::min(done, parts[part].iov_len);
            parts[part].iov_base = static_cast<char *>(parts[part].iov_base) + take;
            parts[part].iov_len -= take;
            done -= take;
            part += parts[part].iov_len == 0;
        }
    }
    return total;
}

// One chunk of an archive read back: views into the bytes it was given.
struct Chunk
{
    ChunkHeader header;
    std::span<const Extent> extents;
    std::span<const std::byte> records, blob;
    std::size_t bytes = 0; // the whole chunk: the next one starts after it
    std::span<const std::byte> frame(std::size_t i) const
    {
        return blob.subspan(extents[i].offset, extents[i].bytes);
    }
    std::span<const std::byte> record(std::size_t i) const
    {
        return records.subspan(i * header.record_stride, header.record_stride);
    }
};
// The archive must be 8-byte aligned, as a mapped file is.
inline Chunk read_chunk(std::span<const std::byte> archive)
{
    Chunk c;
    if(archive.size() < sizeof(c.header))
        throw std::runtime_error("truncated chunk header");
    std::memcpy(&c.header, archive.data(), sizeof(c.header));
    auto table = c.header.frames * sizeof(Extent),
         records = c.header.frames * c.header.record_stride;
    c.bytes = sizeof(c.header) + table + records + c.header.blob_bytes;
    if(archive.size() < c.bytes)
        throw std::runtime_error("truncated chunk");
    c.extents = {reinterpret_cast<const Extent *>(archive.data() + sizeof(c.header)),
                 std::size_t(c.header.frames)};
    c.records = archive.subspan(sizeof(c.header) + table, records);
    c.blob = archive.subspan(sizeof(c.header) + table + records, c.header.blob_bytes);
    for(auto e : c.extents)
        if(e.offset > c.header.blob_bytes || e.bytes > c.header.blob_bytes - e.offset)
            throw std::runtime_error("a frame outside its chunk's blob");
    return c;
}
} // namespace archive
