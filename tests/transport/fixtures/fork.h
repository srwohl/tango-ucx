// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
// SPDX-License-Identifier: LGPL-3.0-or-later
#pragma once

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <unistd.h>

namespace TangoBulk::test
{
struct ForkOffer
{
    std::uint64_t incarnation;
    std::uint32_t sessions;
    std::uint8_t every;
};
inline bool write_all(int fd, const void *data, std::size_t n)
{
    auto *p = static_cast<const std::byte *>(data);
    while(n)
    {
        auto k = ::write(fd, p, n);
        if(k < 0 && errno == EINTR)
            continue;
        if(k <= 0)
            return false;
        p += k;
        n -= k;
    }
    return true;
}
inline bool read_all(int fd, void *data, std::size_t n)
{
    auto *p = static_cast<std::byte *>(data);
    while(n)
    {
        auto k = ::read(fd, p, n);
        if(k < 0 && errno == EINTR)
            continue;
        if(k <= 0)
            return false;
        p += k;
        n -= k;
    }
    return true;
}
inline bool write_text(int fd, std::string_view s)
{
    auto n = std::uint32_t(s.size());
    return write_all(fd, &n, sizeof(n)) && write_all(fd, s.data(), s.size());
}
inline std::string read_text(int fd)
{
    std::uint32_t n = 0;
    if(!read_all(fd, &n, sizeof(n)) || n > 16384)
        return {};
    std::string s(n, '\0');
    if(!read_all(fd, s.data(), s.size()))
        return {};
    return s;
}
} // namespace TangoBulk::test
