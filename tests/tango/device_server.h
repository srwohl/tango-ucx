// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
// SPDX-License-Identifier: LGPL-3.0-or-later
#pragma once

#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <chrono>
#include <stdexcept>
#include <string>
#include <vector>

namespace TangoBulk::test
{
// fixture_device as a separate -nodb device server process, one per test process, stopped by the
// static destructor.
class DeviceServer
{
  public:
    static DeviceServer &instance()
    {
        static DeviceServer server;
        return server;
    }
    const std::string &device() const
    {
        return device_;
    }
    DeviceServer(const DeviceServer &) = delete;
    DeviceServer &operator=(const DeviceServer &) = delete;

  private:
    // A port the kernel reports free. Ports 45000 to 45999 are left to other checkouts on this
    // machine, which pick from that range.
    static int free_port()
    {
        for(;;)
        {
            int fd = ::socket(AF_INET, SOCK_STREAM, 0);
            sockaddr_in a{};
            a.sin_family = AF_INET;
            a.sin_addr.s_addr = ::htonl(INADDR_LOOPBACK);
            socklen_t n = sizeof(a);
            bool ok = fd >= 0 && ::bind(fd, reinterpret_cast<sockaddr *>(&a), n) == 0 &&
                      ::getsockname(fd, reinterpret_cast<sockaddr *>(&a), &n) == 0;
            if(fd >= 0)
                ::close(fd);
            if(!ok)
                throw std::runtime_error("could not find a free port");
            int port = ::ntohs(a.sin_port);
            if(port < 45000 || port > 45999)
                return port;
        }
    }
    DeviceServer()
    {
        int port = free_port();
        int fds[2];
        if(::pipe(fds) != 0 || (pid_ = ::fork()) < 0)
            throw std::runtime_error("could not start the device server");
        if(pid_ == 0)
        {
            // Readiness goes out on descriptor 3: Util::init() takes std::cout over.
            ::close(fds[0]);
            ::dup2(fds[1], 3);
            std::vector<std::string> args{TANGO_BULK_FIXTURE_DEVICE,
                                          "test",
                                          "-nodb",
                                          "-dlist",
                                          "test/bulk/1",
                                          "-ORBendPoint",
                                          "giop:tcp:127.0.0.1:" + std::to_string(port)};
            std::vector<char *> argv;
            for(auto &a : args)
                argv.push_back(a.data());
            argv.push_back(nullptr);
            ::execv(TANGO_BULK_FIXTURE_DEVICE, argv.data());
            ::_exit(127);
        }
        ::close(fds[1]);
        await_ready(fds[0]);
        ::close(fds[0]);
        device_ = "127.0.0.1:" + std::to_string(port) + "/test/bulk/1#dbase=no";
    }
    ~DeviceServer()
    {
        if(pid_ <= 0)
            return;
        ::kill(pid_, SIGTERM); // lets delete_device() close the publisher
        int status = 0;
        for(int i = 0; i < 200; ++i, ::usleep(50'000))
            if(::waitpid(pid_, &status, WNOHANG) == pid_)
                return;
        ::kill(pid_, SIGKILL);
        ::waitpid(pid_, &status, 0);
    }
    // Util::init() reverse-resolves every local interface address; a missing PTR record costs
    // seconds each, so the wait is generous.
    void await_ready(int fd)
    {
        auto until = std::chrono::steady_clock::now() + std::chrono::seconds(60);
        std::string said;
        while(std::chrono::steady_clock::now() < until && said.find("READY") == said.npos)
        {
            pollfd p{fd, POLLIN, 0};
            if(::poll(&p, 1, 250) <= 0)
                continue;
            std::array<char, 256> b;
            auto n = ::read(fd, b.data(), b.size());
            if(n <= 0)
                break; // the server died
            said.append(b.data(), std::size_t(n));
        }
        if(said.find("READY") != said.npos)
            return;
        ::kill(pid_, SIGKILL);
        ::waitpid(pid_, nullptr, 0);
        pid_ = -1;
        throw std::runtime_error("the fixture device server did not become ready: " + said);
    }
    pid_t pid_ = -1;
    std::string device_;
};
} // namespace TangoBulk::test
