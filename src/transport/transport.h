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
Offer offer(const Publisher &);

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

} // namespace TangoBulk::transport
