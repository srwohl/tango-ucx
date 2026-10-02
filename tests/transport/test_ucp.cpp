// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
// SPDX-License-Identifier: LGPL-3.0-or-later
#include <catch2/catch_test_macros.hpp>
#include <ucm/api/ucm.h>
#include <ucx.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdlib>
#include <regex>
#include <span>
#include <utility>
#include <vector>

using namespace TangoBulk::transport;

namespace
{
std::vector<std::pair<void *, ucs_status_t>> peer_errors;
void on_error(void *arg, ucp_ep_h, ucs_status_t status)
{
    peer_errors.emplace_back(arg, status);
}
// UCX keeps its request in the kilobyte before an Op, and the Op is the pointer UCX is handed.
struct Op
{
    bool pending = false, finished = true; // an Op never started counts as finished
    int calls = 0;
    void *request = nullptr;
    ucs_status_t status = UCS_INPROGRESS;
};
struct alignas(64) Record
{
    std::byte ucx[1024];
    Op op;
    std::vector<std::byte> data;
};
void completed(void *request, ucs_status_t status, void *user)
{
    auto &o = *static_cast<Op *>(user);
    o = {o.pending, true, o.calls + 1, request, status};
}
ucp_request_param_t param(Op &o, std::uint32_t flags = 0)
{
    ucp_request_param_t p{};
    p.op_attr_mask = UCP_OP_ATTR_FIELD_REQUEST | UCP_OP_ATTR_FIELD_CALLBACK |
                     UCP_OP_ATTR_FIELD_USER_DATA | UCP_OP_ATTR_FIELD_FLAGS;
    p.request = p.user_data = &o;
    p.cb.send = completed;
    p.flags = flags;
    return p;
}
void started(Op &o, ucs_status_ptr_t q)
{
    o.pending = UCS_PTR_IS_PTR(q);
    o.finished = !o.pending;
    o.status = o.pending ? UCS_INPROGRESS : UCS_PTR_STATUS(q);
    CHECK((!o.pending || q == &o));
}
// Two workers with a PEER endpoint each to the other, as a publisher and a subscription have. A
// sends active message 1 to B; its one-byte header indexes out and in.
struct Pair
{
    bool set; // an environment variable for the context, removed once the context has read it
    ucx::Context context;
    std::array<Record, 4> out, in;
    ucs_status_t refuse = UCS_OK;
    int eager = 0, rndv = 0, a_arg = 0, b_arg = 0;
    ucx::Worker a{context}, b{context};
    ucp_ep_h ab = a.endpoint(b.address(), &a_arg, on_error);
    ucp_ep_h ba = b.endpoint(a.address(), &b_arg, on_error);
    explicit Pair(const char *name = nullptr) : set(name && ::setenv(name, "tcp", 1) == 0)
    {
        if(set)
            ::unsetenv(name);
        peer_errors.clear();
        b.set_am(1, on_am, this);
    }
    ~Pair()
    {
        close(ab, UCP_EP_CLOSE_FLAG_FORCE);
        close(ba, UCP_EP_CLOSE_FLAG_FORCE);
        until([&] { return settled(); });
    }
    static ucs_status_t on_am(void *arg, const void *h, size_t, void *desc, size_t n,
                              const ucp_am_recv_param_t *attr)
    {
        auto &p = *static_cast<Pair *>(arg);
        if(!(attr->recv_attr & UCP_AM_RECV_ATTR_FLAG_RNDV))
            return ++p.eager, UCS_OK;
        if(++p.rndv, p.refuse != UCS_OK)
            return p.refuse;
        auto &r = p.in[*static_cast<const std::uint8_t *>(h)];
        auto rp = param(r.op);
        rp.cb.recv_am = [](void *q, ucs_status_t s, size_t, void *u) { completed(q, s, u); };
        r.data.resize(n);
        started(r.op, ucp_am_recv_data_nbx(p.b.get(), desc, r.data.data(), n, &rp));
        return r.op.pending ? UCS_INPROGRESS : UCS_OK;
    }
    void send(std::uint8_t i, std::span<const std::byte> payload, std::uint32_t flags)
    {
        static const std::uint8_t index[] = {0, 1, 2, 3};
        auto p = param(out[i].op, flags);
        started(out[i].op,
                ucp_am_send_nbx(ab, 1, &index[i], 1, payload.data(), payload.size(), &p));
    }
    void close(ucp_ep_h &ep, std::uint32_t flags = 0)
    {
        Record r;
        auto p = param(r.op, flags);
        if(ep)
            started(r.op, ucp_ep_close_nbx(std::exchange(ep, nullptr), &p));
        until([&] { return r.op.finished; });
    }
    template <class Done> bool until(Done done, int seconds = 5)
    {
        auto end = std::chrono::steady_clock::now() + std::chrono::seconds(seconds);
        while(!done() && std::chrono::steady_clock::now() < end)
            a.progress(), b.progress();
        return done();
    }
    bool settled()
    {
        return std::ranges::all_of(out, &Op::finished, &Record::op) &&
               std::ranges::all_of(in, &Op::finished, &Record::op);
    }
};
} // namespace

TEST_CASE("UCX completes caller request memory once, through the caller's pointer")
{
    Pair pair;
    std::vector<std::byte> small(64, std::byte{1}), big(256 << 10, std::byte{2});
    for(std::uint8_t i = 0; i < 4; ++i)
        pair.send(i, i < 2 ? small : big, i < 2 ? UCP_AM_SEND_FLAG_EAGER : UCP_AM_SEND_FLAG_RNDV);
    REQUIRE(pair.until([&] { return pair.eager == 2 && pair.rndv == 2 && pair.settled(); }));
    pair.until([] { return false; }, 1); // a second callback would arrive here
    for(auto *o : {&pair.out[0].op, &pair.out[1].op, &pair.out[2].op, &pair.out[3].op,
                   &pair.in[2].op, &pair.in[3].op})
    {
        CHECK(o->calls == int(o->pending));
        CHECK(o->request == (o->pending ? o : nullptr));
        CHECK(o->status == UCS_OK);
    }
    CHECK((pair.out[2].op.pending && pair.out[3].op.pending));
    CHECK(pair.in[3].data == big);
    CHECK(peer_errors.empty());
}

TEST_CASE("a refused rendezvous reaches the sender's completion, not the endpoint")
{
    Pair pair;
    std::vector<std::byte> big(256 << 10, std::byte{3});
    pair.refuse = UCS_ERR_REJECTED;
    pair.send(0, big, UCP_AM_SEND_FLAG_RNDV);
    REQUIRE(pair.until([&] { return pair.settled(); }));
    CHECK(pair.out[0].op.status == UCS_ERR_REJECTED);
    CHECK(pair.rndv == 1);
    CHECK(peer_errors.empty());
}

TEST_CASE("a force close completes a pending receive; a clean remote close fires no PEER error")
{
    {
        Pair pair("TANGO_BULK_UCX_TLS"); // over TCP, rendezvous data moves only as the sender runs
        std::vector<std::byte> big(4 << 20);
        pair.send(0, big, UCP_AM_SEND_FLAG_RNDV);
        REQUIRE(pair.until([&] { return pair.rndv == 1; }));
        for(int turn = 0; turn < 1000; ++turn)
            pair.b.progress();
        REQUIRE((pair.in[0].op.pending && pair.in[0].op.calls == 0));
        pair.close(pair.ba, UCP_EP_CLOSE_FLAG_FORCE);
        REQUIRE(pair.until([&] { return pair.settled() && !peer_errors.empty(); }));
        CHECK(pair.in[0].op.calls == 1);
        CHECK(pair.in[0].op.request == &pair.in[0].op);
        CHECK(pair.in[0].op.status == UCS_ERR_CANCELED);
        // Over TCP the forced close fails the other side's endpoint; its handler gets its own arg.
        REQUIRE(peer_errors.size() == 1);
        CHECK(peer_errors[0].first == &pair.a_arg);
        CHECK(peer_errors[0].second != UCS_OK);
    }
    // A clean close leaves the remote worker alive, and its endpoint keeps working.
    for(auto *tls : {static_cast<const char *>(nullptr), "TANGO_BULK_UCX_TLS"})
    {
        Pair pair(tls);
        pair.close(pair.ab);
        pair.until([] { return false; }, 5);
        CHECK(peer_errors.empty());
    }
}

TEST_CASE("UCX applies TANGO_BULK_UCX_* and plain UCX_* settings, and reports its memory hooks")
{
    auto transports = [](const char *name)
    {
        Pair pair(name);
        std::array<char, 128> text{};
        return std::string(text.data(), pair.a.transports(pair.ab, text));
    };
    std::regex tcp_only("tcp/[^,]+(,tcp/[^,]+)*");
    CHECK_FALSE(std::regex_match(transports(nullptr), tcp_only)); // shared memory on one host
    CHECK(std::regex_match(transports("TANGO_BULK_UCX_TLS"), tcp_only));
    CHECK(std::regex_match(transports("UCX_TLS"), tcp_only));
    ucx::Context context;
    CHECK(context.memory_hooks() == (ucm_test_events(UCM_EVENT_VM_UNMAPPED) == UCS_OK));
    CHECK(ucx::Worker(context).max_am_header() >= 1064);
}
