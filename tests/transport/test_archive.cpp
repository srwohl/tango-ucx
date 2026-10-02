// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-ucx project
// SPDX-License-Identifier: LGPL-3.0-or-later
#include "fixtures/pair.h"
#include <archive.h>
#include <catch2/catch_test_macros.hpp>

#include <cstdio>
#include <cstdlib>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>

using namespace TangoUcx;
using namespace std::chrono_literals;
using Clock = std::chrono::steady_clock;

namespace
{
// An archive file that is removed when the test ends. TANGO_UCX_ARCHIVE_DIR names the file
// system to write to; the default is the system's temporary directory.
struct File
{
    std::string path;
    int fd = -1;
    File()
    {
        const char *dir = std::getenv("TANGO_UCX_ARCHIVE_DIR");
        path = std::string(dir ? dir : "/tmp") + "/tango-ucx-archive-XXXXXX";
        fd = ::mkstemp(path.data());
        REQUIRE(fd >= 0);
    }
    ~File()
    {
        ::close(fd);
        ::unlink(path.c_str());
    }
};
std::size_t bytes_of(std::uint64_t i, std::size_t about)
{
    return about - about / 8 + std::size_t(i * 2654435761u % (about / 4));
}
// Publishes `frames` opaque frames of about `about` bytes from a device thread, then finishes.
std::jthread device(Publisher &publisher, std::uint64_t frames, std::size_t about)
{
    return std::jthread(
        [&publisher, frames, about]
        {
            for(std::uint64_t i = 0; i < frames; ++i)
                if(!test::publish_opaque(publisher, bytes_of(i, about), i))
                    break;
            publisher.finish();
        });
}
PublisherLimits limits(std::uint64_t budget)
{
    auto l = test::small_limits(1);
    l.budget = budget;
    return l;
}
} // namespace

TEST_CASE("archive chunks of 10 and 100 frames recover every frame and record from one blob")
{
    for(std::uint32_t batch : {10u, 100u})
    {
        const std::uint64_t frames = 350;
        Publisher publisher(test::opaque_description(), limits(8ull << 20));
        auto sub = test::subscribe(publisher, true,
                                   {.budget = 16ull << 20, .batch = batch, .max_wait = 2s});
        auto stride = archive::record_stride(sub.description());
        REQUIRE(stride == 24);
        auto publishing = device(publisher, frames, 20'000);
        File file;
        std::size_t written = 0, chunks = 0;
        while(auto b = sub.read(test::deadline()))
        {
            written += archive::write_chunk(file.fd, *b, stride);
            ++chunks;
        }
        REQUIRE(sub.outcome() == Outcome::End);
        // 350 frames fit the ring, so only the batch size and End cut a batch.
        CHECK(chunks == (frames + batch - 1) / batch);

        struct stat st{};
        REQUIRE(::fstat(file.fd, &st) == 0);
        REQUIRE(std::size_t(st.st_size) == written);
        auto *map = ::mmap(nullptr, written, PROT_READ, MAP_PRIVATE, file.fd, 0);
        REQUIRE(map != MAP_FAILED);
        std::span archive(static_cast<const std::byte *>(map), written);
        std::uint64_t next = 0;
        while(!archive.empty())
        {
            auto chunk = archive::read_chunk(archive);
            for(std::size_t i = 0; i < chunk.header.frames; ++i, ++next)
            {
                auto frame = chunk.frame(i);
                auto record = chunk.record(i);
                REQUIRE(frame.size() == bytes_of(next, 20'000));
                REQUIRE(wire::get64(record.data()) == next);      // the frame index
                REQUIRE(wire::get64(record.data() + 16) == next); // its application field
                bool same = true;
                for(std::size_t b = 0; same && b < frame.size(); ++b)
                    same = frame[b] == test::opaque_byte(next, b);
                REQUIRE(same);
            }
            archive = archive.subspan(chunk.bytes);
        }
        CHECK(next == frames);
        ::munmap(map, written);
    }
}

// Hidden: run it with `test_archive "[.benchmark]"`. The write is from the ring; the gather is
// what Batch::copy costs, for an archiver that releases a batch before it writes it.
TEST_CASE("benchmark: the gather copy and the file write of archive chunks", "[.benchmark]")
{
    const std::size_t about = 300 << 10; // frames of about 300 KiB under a 9 MiB maximum
    for(std::uint32_t batch : {10u, 100u})
    {
        const std::uint64_t frames = 2000;
        Publisher publisher(test::opaque_description(9ull << 20), limits(64ull << 20));
        auto sub = test::subscribe(publisher, true,
                                   {.budget = 256ull << 20, .batch = batch, .max_wait = 2s});
        auto stride = archive::record_stride(sub.description());
        auto publishing = device(publisher, frames, about);
        File file;
        std::size_t written = 0, chunks = 0, copied = 0;
        Clock::duration writing{}, copying{};
        auto begin = Clock::now();
        while(auto b = sub.read(test::deadline()))
        {
            auto start = Clock::now();
            auto copy = b->copy();
            copying += Clock::now() - start;
            copied += std::size_t(copy.payload(copy.frames() - 1).data() - copy.payload(0).data()) +
                      copy.payload(copy.frames() - 1).size();
            start = Clock::now();
            written += archive::write_chunk(file.fd, *b, stride);
            writing += Clock::now() - start;
            ++chunks;
        }
        auto start = Clock::now();
        ::fsync(file.fd);
        auto sync = Clock::now() - start;
        auto total = std::chrono::duration<double>(Clock::now() - begin).count();
        REQUIRE(sub.outcome() == Outcome::End);
        auto rate = [](std::size_t bytes, Clock::duration d)
        { return double(bytes) / (1 << 20) / std::chrono::duration<double>(d).count(); };
        std::printf("batch %3u: %zu chunks, %.0f MiB; gather copy %.0f MiB/s, write %.0f MiB/s, "
                    "write+fsync %.0f MiB/s; %.0f frames/s end to end (%s)\n",
                    batch, chunks, double(written) / (1 << 20), rate(copied, copying),
                    rate(written, writing), rate(written, writing + sync), double(frames) / total,
                    file.path.c_str());
    }
}
