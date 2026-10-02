// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-ucx project
// SPDX-License-Identifier: LGPL-3.0-or-later
#include "ucp.h"

#include <ucm/api/ucm.h>

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <mutex>
#include <stdexcept>

namespace TangoUcx::transport::ucp
{
void check(ucs_status_t s, const char *what)
{
    if(s != UCS_OK)
        throw std::runtime_error(std::string(what) + ": " + ucs_status_string(s));
}
Context::Context()
{
    unsigned major = 0, minor = 0, release = 0;
    ucp_get_version(&major, &minor, &release);
    if(major < 1 || (major == 1 && minor < 21))
        throw std::runtime_error("UCX 1.21 or newer is required");
    ucp_config_t *cfg = nullptr;
    check(ucp_config_read("TANGO_UCX", nullptr, &cfg), "ucp_config_read");
    auto st = ucp_config_modify(cfg, "MM_ERROR_HANDLING", "y");
    if(st != UCS_OK)
    {
        ucp_config_release(cfg);
        check(st, "ucp_config_modify(MM_ERROR_HANDLING)");
    }
    st = ucp_config_modify(cfg, "KEEPALIVE_INTERVAL", "2s");
    if(st != UCS_OK)
    {
        ucp_config_release(cfg);
        check(st, "ucp_config_modify(KEEPALIVE_INTERVAL)");
    }
    ucp_params_t params{};
    params.field_mask = UCP_PARAM_FIELD_FEATURES;
    params.features = UCP_FEATURE_AM;
    st = ucp_init(&params, cfg, &context_);
    ucp_config_release(cfg);
    check(st, "ucp_init");
    ucp_context_attr_t attr{};
    attr.field_mask = UCP_ATTR_FIELD_REQUEST_SIZE;
    st = ucp_context_query(context_, &attr);
    if(st != UCS_OK)
    {
        destroy();
        check(st, "ucp_context_query");
    }
    if(attr.request_size > 1024)
    {
        destroy();
        throw std::runtime_error("UCX request size exceeds 1 KiB");
    }
    request_bytes_ = std::uint32_t(attr.request_size);
    static std::once_flag once;
    static ucs_status_t hooks = UCS_ERR_UNSUPPORTED;
    std::call_once(once, [] { hooks = ucm_test_events(UCM_EVENT_VM_UNMAPPED); });
    memory_hooks_ = hooks == UCS_OK;
}
Context::~Context()
{
    destroy();
}
void Context::destroy() noexcept
{
    if(context_)
    {
        ucp_cleanup(context_);
        context_ = nullptr;
    }
}

Worker::Worker(Context &context)
{
    ucp_worker_params_t p{};
    p.field_mask = UCP_WORKER_PARAM_FIELD_THREAD_MODE;
    p.thread_mode = UCS_THREAD_MODE_SINGLE;
    check(ucp_worker_create(context.get(), &p, &worker_), "ucp_worker_create");
    ucp_worker_attr_t a{};
    a.field_mask = UCP_WORKER_ATTR_FIELD_ADDRESS | UCP_WORKER_ATTR_FIELD_MAX_AM_HEADER;
    auto st = ucp_worker_query(worker_, &a);
    if(st != UCS_OK)
    {
        destroy();
        check(st, "ucp_worker_query");
    }
    if(a.address_length > address_.size())
    {
        ucp_worker_release_address(worker_, a.address);
        destroy();
        throw std::runtime_error("UCX worker address exceeds 4096 bytes");
    }
    std::memcpy(address_.data(), a.address, a.address_length);
    address_bytes_ = a.address_length;
    max_am_header_ = a.max_am_header;
    ucp_worker_release_address(worker_, a.address);
}
Worker::~Worker()
{
    destroy();
}
void Worker::destroy() noexcept
{
    if(worker_)
    {
        ucp_worker_destroy(worker_);
        worker_ = nullptr;
    }
}
ucp_ep_h Worker::endpoint(std::span<const std::byte> address, void *arg, ucp_err_handler_cb_t cb)
{
    if(address.empty() || address.size() > 4096)
        throw std::invalid_argument("invalid peer worker address");
    ucp_ep_params_t p{};
    // No USER_DATA field: UCX would pass user_data to the handler in place of err_handler.arg.
    p.field_mask = UCP_EP_PARAM_FIELD_REMOTE_ADDRESS | UCP_EP_PARAM_FIELD_ERR_HANDLING_MODE |
                   UCP_EP_PARAM_FIELD_ERR_HANDLER;
    p.address = reinterpret_cast<const ucp_address_t *>(address.data());
    p.err_mode = UCP_ERR_HANDLING_MODE_PEER;
    p.err_handler.arg = arg;
    p.err_handler.cb = cb;
    ucp_ep_h ep = nullptr;
    check(ucp_ep_create(worker_, &p, &ep), "ucp_ep_create");
    return ep;
}
void Worker::set_am(unsigned id, ucp_am_recv_callback_t cb, void *arg)
{
    ucp_am_handler_param_t p{};
    p.field_mask = UCP_AM_HANDLER_PARAM_FIELD_ID | UCP_AM_HANDLER_PARAM_FIELD_FLAGS |
                   UCP_AM_HANDLER_PARAM_FIELD_CB | UCP_AM_HANDLER_PARAM_FIELD_ARG;
    p.id = id;
    p.flags = UCP_AM_FLAG_WHOLE_MSG;
    p.cb = cb;
    p.arg = arg;
    check(ucp_worker_set_am_recv_handler(worker_, &p), "ucp_worker_set_am_recv_handler");
}
std::size_t Worker::transports(ucp_ep_h ep, std::span<char> out) const
{
    ucp_transport_entry_t entries[8]{};
    ucp_ep_attr_t a{};
    a.field_mask = UCP_EP_ATTR_FIELD_TRANSPORTS;
    a.transports.entries = entries;
    a.transports.num_entries = 8;
    a.transports.entry_size = sizeof(entries[0]);
    auto st = ucp_ep_query(ep, &a);
    if(st != UCS_OK || out.empty())
        return 0;
    std::size_t used = 0;
    for(unsigned i = 0; i < a.transports.num_entries; ++i)
    {
        int n = std::snprintf(out.data() + used, out.size() - used, "%s%s/%s", i ? "," : "",
                              entries[i].transport_name ? entries[i].transport_name : "?",
                              entries[i].device_name ? entries[i].device_name : "?");
        if(n < 0)
            break;
        auto add = std::min<std::size_t>(n, out.size() - used - 1);
        used += add;
        if(std::size_t(n) > add)
            break;
    }
    return used;
}
bool Worker::only_tcp(ucp_ep_h ep) const
{
    ucp_transport_entry_t entries[8]{};
    ucp_ep_attr_t a{};
    a.field_mask = UCP_EP_ATTR_FIELD_TRANSPORTS;
    a.transports.entries = entries;
    a.transports.num_entries = 8;
    a.transports.entry_size = sizeof(entries[0]);
    if(ucp_ep_query(ep, &a) != UCS_OK || !a.transports.num_entries)
        return false;
    for(unsigned i = 0; i < a.transports.num_entries; ++i)
        if(!entries[i].transport_name || std::strcmp(entries[i].transport_name, "tcp") != 0)
            return false;
    return true;
}
} // namespace TangoUcx::transport::ucp
