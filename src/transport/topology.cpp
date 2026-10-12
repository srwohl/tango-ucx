// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-ucx project
// SPDX-License-Identifier: LGPL-3.0-or-later
#include "topology.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <charconv>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string_view>
#include <system_error>

#ifdef __linux__
#include <sched.h>
#include <sys/syscall.h>
#include <unistd.h>
#endif

namespace TangoUcx::transport
{
namespace
{
struct CoreReservations
{
    std::mutex mutex;
    std::map<int, std::vector<int>> cores;
};
CoreReservations &core_reservations()
{
    static CoreReservations reservations;
    return reservations;
}
std::vector<int> cpu_list(std::string_view list)
{
    std::vector<int> cpus;
    while(!list.empty())
    {
        int first = -1, last = -1;
        const auto *end = list.data() + list.size();
        auto [next, error] = std::from_chars(list.data(), end, first);
        if(error != std::errc{} || first < 0)
            return {};
        last = first;
        if(next != end && *next == '-')
        {
            auto range = std::from_chars(next + 1, end, last);
            if(range.ec != std::errc{} || last < first)
                return {};
            next = range.ptr;
        }
        for(int cpu = first;; ++cpu)
        {
            cpus.push_back(cpu);
            if(cpu == last)
                break;
        }
        if(next == end)
            break;
        if(*next != ',')
            return {};
        list.remove_prefix(next - list.data() + 1);
        if(list.empty())
            return {};
    }
    return cpus;
}
std::string read_line(const std::filesystem::path &path)
{
    std::ifstream file(path);
    std::string value;
    std::getline(file, value);
    return value;
}
int read_node(const std::filesystem::path &path)
{
    std::ifstream file(path);
    int node = -1;
    return file >> node && node >= 0 ? node : -1;
}
int cpu_node(int cpu)
{
    std::error_code error;
    auto path = std::filesystem::path("/sys/devices/system/cpu") / ("cpu" + std::to_string(cpu));
    std::filesystem::directory_iterator entries(path, error), end;
    for(; !error && entries != end; entries.increment(error))
    {
        auto name = entries->path().filename().string();
        if(!name.starts_with("node"))
            continue;
        int node = -1;
        auto [last, status] = std::from_chars(name.data() + 4, name.data() + name.size(), node);
        if(status == std::errc{} && last == name.data() + name.size() && node >= 0)
            return node;
    }
    return -1;
}
std::string allowed_cpus()
{
    std::ifstream file("/proc/thread-self/status");
    std::string line;
    while(std::getline(file, line))
        if(line.starts_with("Cpus_allowed_list:"))
        {
            auto begin = line.find_first_not_of(" \t", 18);
            return begin == std::string::npos ? std::string{} : line.substr(begin);
        }
    return {};
}
int ring_node(const void *base, std::uint64_t bytes)
{
#if defined(__linux__) && defined(SYS_move_pages)
    if(!base || !bytes)
        return -1;
    auto page_size = sysconf(_SC_PAGESIZE);
    if(page_size <= 0)
        return -1;
    auto first = reinterpret_cast<std::uintptr_t>(base) / page_size * page_size;
    auto last = (reinterpret_cast<std::uintptr_t>(base) + bytes - 1) / page_size * page_size;
    auto page_count = (last - first) / page_size + 1;
    auto count = std::min<std::uint64_t>(page_count, 3);
    std::array<void *, 3> pages{};
    std::array<int, 3> nodes{-1, -1, -1};
    for(std::uint64_t i = 0; i < count; ++i)
    {
        auto offset = count == 1 ? 0 : i * ((page_count - 1) / (count - 1));
        // Always include the final page, including when the middle offset rounded down.
        if(i + 1 == count)
            offset = page_count - 1;
        pages[i] = reinterpret_cast<void *>(first + offset * page_size);
    }
    if(syscall(SYS_move_pages, 0, count, pages.data(), nullptr, nodes.data(), 0) < 0)
        return -1;
    int observed = -1;
    bool unknown = false;
    for(std::uint64_t i = 0; i < count; ++i)
        if(nodes[i] < 0)
            unknown = true;
        else if(observed >= 0 && observed != nodes[i])
            return -2;
        else
            observed = nodes[i];
    return unknown ? -1 : observed;
#else
    return -1;
#endif
}
} // namespace

ProgressCore::~ProgressCore()
{
    if(cpu_ < 0)
        return;
    auto &reservations = core_reservations();
    std::lock_guard lock(reservations.mutex);
    reservations.cores.erase(cpu_);
}
void ProgressCore::pin(int cpu)
{
    if(cpu < -1)
        throw std::invalid_argument("progress_cpu must be -1 or a non-negative CPU number");
    if(cpu == -1)
        return;
    if(cpu_ >= 0)
        throw std::logic_error("the progress thread already reserved a core");
#ifdef __linux__
    auto allowed = cpu_list(allowed_cpus());
    if(allowed.empty())
        throw std::runtime_error("cannot read the progress thread's allowed CPUs");
    if(std::ranges::find(allowed, cpu) == allowed.end())
        throw std::invalid_argument("progress_cpu " + std::to_string(cpu) +
                                    " is outside the inherited allowed CPU set");
    auto siblings =
        cpu_list(read_line(std::filesystem::path("/sys/devices/system/cpu") /
                           ("cpu" + std::to_string(cpu)) / "topology/thread_siblings_list"));
    if(std::ranges::find(siblings, cpu) == siblings.end())
        throw std::runtime_error("cannot read SMT siblings for progress_cpu " +
                                 std::to_string(cpu));
    auto free_mask = [](cpu_set_t *mask) { CPU_FREE(mask); };
    auto count = std::size_t(cpu) + 1;
    std::unique_ptr<cpu_set_t, decltype(free_mask)> mask(CPU_ALLOC(count), free_mask);
    if(!mask)
        throw std::bad_alloc();
    auto bytes = CPU_ALLOC_SIZE(count);
    CPU_ZERO_S(bytes, mask.get());
    CPU_SET_S(cpu, bytes, mask.get());

    auto &reservations = core_reservations();
    std::lock_guard lock(reservations.mutex);
    for(const auto &[owner, reserved] : reservations.cores)
        if(std::ranges::any_of(siblings, [&](int sibling)
                               { return std::ranges::find(reserved, sibling) != reserved.end(); }))
            throw std::runtime_error("progress_cpu " + std::to_string(cpu) +
                                     " shares a physical core already reserved by progress_cpu " +
                                     std::to_string(owner));
    reservations.cores.emplace(cpu, std::move(siblings));
    if(sched_setaffinity(0, bytes, mask.get()) != 0)
    {
        auto error = errno;
        reservations.cores.erase(cpu);
        throw std::system_error(error, std::generic_category(),
                                "cannot pin the progress thread to CPU " + std::to_string(cpu));
    }
    cpu_ = cpu;
#else
    throw std::runtime_error("progress_cpu requires Linux thread affinity");
#endif
}

void sample_topology(Topology &topology, const void *host_ring, std::uint64_t bytes)
{
#ifdef __linux__
    int cpu = sched_getcpu();
    if(cpu != topology.cpu)
    {
        topology.cpu = cpu;
        topology.numa_node = cpu < 0 ? -1 : cpu_node(cpu);
        topology.sibling_cpus =
            cpu < 0 ? std::string{}
                    : read_line(std::filesystem::path("/sys/devices/system/cpu") /
                                ("cpu" + std::to_string(cpu)) / "topology/thread_siblings_list");
    }
    topology.allowed_cpus = allowed_cpus();
#endif
    topology.ring_numa_node = ring_node(host_ring, bytes);
}

std::vector<NetworkDevice> endpoint_network_devices(ucp_ep_h endpoint)
{
    std::vector<NetworkDevice> devices;
    ucp_transport_entry_t entries[8]{};
    ucp_ep_attr_t attributes{};
    attributes.field_mask = UCP_EP_ATTR_FIELD_TRANSPORTS;
    attributes.transports.entries = entries;
    attributes.transports.num_entries = std::size(entries);
    attributes.transports.entry_size = sizeof(entries[0]);
    if(ucp_ep_query(endpoint, &attributes) != UCS_OK)
        return devices;
    auto count = std::min<unsigned>(attributes.transports.num_entries, std::size(entries));
    for(unsigned i = 0; i < count; ++i)
    {
        const auto &entry = entries[i];
        std::string_view transport = entry.transport_name ? entry.transport_name : "";
        if(!entry.device_name || !(transport == "tcp" || transport.starts_with("rc_") ||
                                   transport.starts_with("ud_") || transport.starts_with("dc_")))
            continue; // shared-memory and CUDA lanes do not describe network devices
        std::string name = entry.device_name;
        if(std::ranges::any_of(devices, [&](const auto &device) { return device.name == name; }))
            continue;
        auto ib_name = name.substr(0, name.find(':')); // the UCX RDMA name includes its port
        auto node = read_node(std::filesystem::path("/sys/class/infiniband") / ib_name /
                              "device/numa_node");
        if(node < 0)
            node = read_node(std::filesystem::path("/sys/class/net") / name / "device/numa_node");
        devices.push_back({std::move(name), node});
    }
    return devices;
}
} // namespace TangoUcx::transport
