// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-ucx project
// SPDX-License-Identifier: LGPL-3.0-or-later
#pragma once

#include "pair.h"
#include <ucp.h>

#include <algorithm>
#include <vector>

namespace TangoUcx::test
{
struct FaultAdapter
{
    static transport::Closer lose_close_fence()
    {
        return [](std::string_view, Deadline) { return false; };
    }
};

// A bare UCX worker that plays a subscriber or a publisher by hand, so a test can send what a
// real peer never would.
struct RawPeer
{
    transport::ucp::Context context;
    transport::ucp::Worker worker{context};
    ucp_ep_h ep = nullptr;
    std::uint64_t session = 0x5e55107;
    std::vector<std::pair<wire::Type, std::vector<std::byte>>> got; // headers, in arrival order
    RawPeer()
    {
        worker.set_am(wire::Ready, on<wire::Ready>, this);
        worker.set_am(wire::Frame, on<wire::Frame>, this);
        worker.set_am(wire::End, on<wire::End>, this);
    }
    ~RawPeer()
    {
        ucp_request_param_t p{};
        p.op_attr_mask = UCP_OP_ATTR_FIELD_FLAGS;
        p.flags = UCP_EP_CLOSE_FLAG_FORCE;
        if(ep)
            wait(ucp_ep_close_nbx(ep, &p));
    }
    template <wire::Type T>
    static ucs_status_t on(void *arg, const void *h, size_t n, void *, size_t,
                           const ucp_am_recv_param_t *)
    {
        auto *b = static_cast<const std::byte *>(h);
        static_cast<RawPeer *>(arg)->got.emplace_back(T, std::vector<std::byte>(b, b + n));
        return UCS_OK;
    }
    std::size_t count(wire::Type t) const
    {
        return std::size_t(std::ranges::count(got, t, &decltype(got)::value_type::first));
    }
    template <class Done> bool progress_until(Done done)
    {
        for(auto until = deadline(); !done() && std::chrono::steady_clock::now() < until;)
            worker.progress();
        return done();
    }
    void wait(ucs_status_ptr_t q)
    {
        if(!UCS_PTR_IS_PTR(q))
            return;
        progress_until([&] { return ucp_request_check_status(q) != UCS_INPROGRESS; });
        ucp_request_free(q);
    }
    void send(wire::Type id, std::span<const std::byte> header,
              std::span<const std::byte> data = {})
    {
        ucp_request_param_t p{};
        wait(ucp_am_send_nbx(ep, id, header.data(), header.size(), data.data(), data.size(), &p));
    }
    void connect(const std::byte *address, std::size_t n)
    {
        ep = worker.endpoint({address, n}, this, [](void *, ucp_ep_h, ucs_status_t) {});
    }
    // Opens a session on a real publisher, as a subscription with this worker's address would.
    wire::Status open(Publisher &publisher, bool every, std::uint32_t depth)
    {
        wire::OpenMessage m{};
        m.session = session;
        m.incarnation = offer_of(publisher).incarnation;
        m.depth = depth;
        m.batch = 1;
        m.every = every;
        m.address_bytes = std::uint16_t(worker.address().size());
        std::ranges::copy(worker.address(), m.address.begin());
        wire::Reply r;
        if(!wire::parse(transport::handle(publisher, wire::format(m)), r))
            throw std::runtime_error("unparsed reply to Open");
        if(r.status == wire::Ok)
            connect(r.address.data(), r.address_bytes);
        return r.status;
    }
    // Plays the publisher for a prepared subscription: connects, sends Ready, and returns the
    // reply to its Open.
    std::string accept(const transport::PendingSubscription &pending)
    {
        wire::Request r;
        wire::parse(pending.open_message(), r);
        session = r.open->session;
        connect(r.open->address.data(), r.open->address_bytes);
        send(wire::Ready, wire::encode(wire::SessionMessage{session}));
        wire::Reply reply{wire::Ok};
        reply.address_bytes = std::uint16_t(worker.address().size());
        std::ranges::copy(worker.address(), reply.address.begin());
        return wire::format(reply);
    }
};
} // namespace TangoUcx::test
