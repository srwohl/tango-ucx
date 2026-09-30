// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
// SPDX-License-Identifier: LGPL-3.0-or-later
#pragma once

#include <cerrno>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <string_view>
#include <sys/wait.h>
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
// A forked peer and a pipe each way. The parent kills and reaps it: over shared memory UCX
// checks the peer's process, and sees an unreaped zombie as alive.
struct Forked
{
    int to_child[2]{-1, -1}, to_parent[2]{-1, -1};
    pid_t pid = -1;
    Forked()
    {
        if(::pipe(to_child) != 0 || ::pipe(to_parent) != 0 || (pid = ::fork()) < 0)
            throw std::runtime_error("fork harness");
        ::close(pid == 0 ? to_child[1] : to_child[0]);
        ::close(pid == 0 ? to_parent[0] : to_parent[1]);
    }
    Forked(const Forked &) = delete;
    Forked &operator=(const Forked &) = delete;
    int in() const
    {
        return pid == 0 ? to_child[0] : to_parent[0];
    }
    int out() const
    {
        return pid == 0 ? to_parent[1] : to_child[1];
    }
    // True when the child died of SIGKILL.
    bool kill()
    {
        int status = 0;
        bool killed = ::kill(pid, SIGKILL) == 0 && ::waitpid(pid, &status, 0) == pid &&
                      WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL;
        pid = -1;
        return killed;
    }
    ~Forked()
    {
        if(pid > 0)
            kill();
        ::close(in());
        ::close(out());
    }
};
} // namespace TangoBulk::test
