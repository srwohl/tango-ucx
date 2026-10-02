// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-ucx project
// SPDX-License-Identifier: LGPL-3.0-or-later
//
// An archiver for an opaque UCX stream: it joins a device as an every subscription, and writes
// each batch as one chunk (archive.h) until the incarnation ends.
//
//   archive <device> <file> [frames per batch, default 100] [max wait in ms, default 100]
//
// Start it before the device's ucx_start: an every subscription joins before the start.
#include "archive.h"

#include <tango-ucx/tango.h>

#include <tango/tango.h>

#include <chrono>
#include <cstdlib>
#include <fcntl.h>
#include <iostream>
#include <unistd.h>

int main(int argc, char *argv[])
{
    using namespace TangoUcx;
    using Clock = std::chrono::steady_clock;
    if(argc < 3)
    {
        std::cerr << "usage: archive <device> <file> [frames per batch] [max wait in ms]\n";
        return 2;
    }
    try
    {
        Tango::DeviceProxy device(argv[1]);
        SubscriptionOptions options;
        options.batch = argc > 3 ? std::uint32_t(std::atoi(argv[3])) : 100;
        options.max_wait = std::chrono::milliseconds(argc > 4 ? std::atoi(argv[4]) : 100);
        options.label = "archive";
        auto subscription = every(device, options);
        if(!subscription.description().max_payload_bytes)
            throw std::runtime_error("the device's UCX stream is not opaque");
        auto stride = archive::record_stride(subscription.description());
        int fd = ::open(argv[2], O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if(fd < 0)
            throw std::system_error(errno, std::generic_category(), argv[2]);
        std::uint64_t frames = 0, chunks = 0, bytes = 0;
        Clock::duration writing{};
        while(auto batch = subscription.read(Deadline::max()))
        {
            auto start = Clock::now();
            bytes += archive::write_chunk(fd, *batch, stride);
            writing += Clock::now() - start;
            frames += batch->frames();
            ++chunks;
        } // the batch is released here: its bytes go back to the publisher as Credit
        ::fsync(fd);
        ::close(fd);
        auto seconds = std::chrono::duration<double>(writing).count();
        std::cout << frames << " frames in " << chunks << " chunks, " << bytes << " bytes, "
                  << (seconds > 0 ? double(bytes) / (1 << 20) / seconds : 0.0)
                  << " MiB/s while writing\n";
        if(subscription.outcome() != Outcome::End)
        {
            std::cerr << "the subscription failed: " << subscription.health().failure << "\n";
            return 1;
        }
    }
    catch(const Tango::DevFailed &e)
    {
        Tango::Except::print_exception(e);
        return 1;
    }
    catch(const std::exception &e)
    {
        std::cerr << "archive: " << e.what() << "\n";
        return 1;
    }
}
