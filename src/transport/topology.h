// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-ucx project
// SPDX-License-Identifier: LGPL-3.0-or-later
#pragma once

#include <tango-ucx/ucx.h>
#include <ucp/api/ucp.h>

namespace TangoUcx::transport
{
// Lives on the progress thread until it exits, including failed setup and transport teardown.
class ProgressCore
{
  public:
    ProgressCore() = default;
    ProgressCore(const ProgressCore &) = delete;
    ProgressCore &operator=(const ProgressCore &) = delete;
    ~ProgressCore();
    void pin(int cpu);

  private:
    int cpu_ = -1;
};
// Called on the progress thread. A null host_ring leaves ring locality unknown.
// Queries existing pages only: it neither faults pages in nor changes placement.
void sample_topology(Topology &, const void *host_ring, std::uint64_t bytes);
std::vector<NetworkDevice> endpoint_network_devices(ucp_ep_h);
} // namespace TangoUcx::transport
