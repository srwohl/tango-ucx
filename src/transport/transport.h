// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
// SPDX-License-Identifier: LGPL-3.0-or-later
#pragma once

#include <tango-bulk/bulk.h>

#include <functional>
#include <string>
#include <string_view>

namespace TangoBulk::transport
{
// Sends a Close document through the bulk command; true when the reply's status is ok.
using Closer = std::function<bool(std::string_view, Deadline)>;

struct Offer
{
    Description description;
    PublisherLimits limits;
    std::uint64_t incarnation = 0;
};

// A bulk command's JSON document in, its reply document out; parsed on the calling thread.
std::string handle(Publisher &, std::string_view document);
std::string bulk_stream(const Publisher &);
// The BulkSessions document: the sessions of the last Health sample.
std::string bulk_sessions(const Publisher &);
// "tango-bulk/3 open" -> 3; -1 when the description is not a bulk command marker.
int marker_major(std::string_view type_description);
// Throws when the document is malformed or of another major version.
Offer parse_bulk_stream(std::string_view document);

class PendingSubscription
{
  public:
    PendingSubscription(PendingSubscription &&) noexcept;
    PendingSubscription &operator=(PendingSubscription &&) noexcept;
    PendingSubscription(const PendingSubscription &) = delete;
    PendingSubscription &operator=(const PendingSubscription &) = delete;
    ~PendingSubscription();
    std::string open_message() const;
    Subscription start(std::string_view open_reply, Deadline, Closer) &&;

  private:
    friend PendingSubscription prepare(const Offer &, bool, SubscriptionOptions);
    struct Impl;
    explicit PendingSubscription(std::unique_ptr<Impl>);
    std::unique_ptr<Impl> p_;
};

PendingSubscription prepare(const Offer &, bool every, SubscriptionOptions = {});

enum class GpuEventState
{
    Pending,
    Done,
    Failed
};
struct GpuEvent
{
    void *handle = nullptr;
};
struct Allocation
{
    void *base = nullptr;
    std::size_t bytes = 0;
    int gpu = -1;
};
// What the transport needs of a GPU; tango-bulk-cuda implements it and registers itself.
struct GpuRuntime
{
    virtual Allocation allocate(int gpu, std::size_t bytes) = 0;
    virtual void release(Allocation) = 0;
    virtual GpuEvent make_event(int gpu) = 0;
    virtual void destroy_event(GpuEvent) = 0;
    // Never throws: a failed record shows as Failed at query.
    virtual void record(GpuEvent, GpuStream) = 0;
    virtual GpuEventState query(GpuEvent) = 0;
    virtual void copy_to_host(void *to, const void *from, std::size_t bytes) = 0;
    virtual ~GpuRuntime() = default;
};
void register_gpu_runtime(GpuRuntime &);

} // namespace TangoBulk::transport
