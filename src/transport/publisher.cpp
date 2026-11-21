// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-ucx project
// SPDX-License-Identifier: LGPL-3.0-or-later
// The public Publisher and Slot, which hand their work to the instance and the engine, and the
// Ucx commands.
#include "publisher.h"

#include <algorithm>
#include <bit>
#include <cassert>
#include <chrono>
#include <cstring>
#include <format>
#include <stdexcept>
#include <thread>
#include <utility>

namespace TangoUcx
{
Slot::Slot(std::shared_ptr<transport::Pub> pub, std::uint32_t index)
    : state_(std::move(pub)), index_(index), active_(true)
{
}
Slot::Slot(Slot &&other) noexcept
    : state_(std::move(other.state_)), index_(other.index_),
      active_(std::exchange(other.active_, false))
{
}
Slot &Slot::operator=(Slot &&other) noexcept
{
    if(this != &other)
    {
        if(active_)
            if(auto pub = state_)
                pub->cancel_slot(index_);
        state_ = std::move(other.state_);
        index_ = other.index_;
        active_ = std::exchange(other.active_, false);
    }
    return *this;
}
Slot::~Slot()
{
    if(active_)
        if(auto pub = state_)
            pub->cancel_slot(index_);
}
std::span<std::byte> Slot::payload()
{
    if(auto pub = active_ ? state_ : nullptr; pub && !pub->closed())
        return pub->payload(index_);
    return {};
}
std::span<std::byte> Slot::fields()
{
    if(auto pub = active_ ? state_ : nullptr; pub && !pub->closed())
        return pub->fields(index_);
    return {};
}
void Slot::publish(std::optional<std::uint64_t> timestamp) &&
{
    if(!active_)
        throw std::logic_error("slot already published");
    if(auto pub = state_)
        pub->publish_slot(index_, timestamp, {});
    active_ = false;
    state_.reset();
}
void *Slot::gpu_payload(GpuStream stream)
{
    if(!active_ || !state_)
        throw std::logic_error("gpu_payload needs an active GPU source slot");
    return state_->gpu_payload(index_, stream);
}
std::size_t Slot::payload_bytes() const
{
    if(!active_ || !state_)
        throw std::logic_error("payload_bytes needs an active source slot");
    return state_->plan.sizes.frame_bytes;
}
void Slot::publish_bytes(std::size_t bytes, std::optional<std::uint64_t> timestamp) &&
{
    if(!active_)
        throw std::logic_error("slot already published");
    if(auto pub = state_)
        pub->publish_slot(index_, timestamp, bytes);
    active_ = false;
    state_.reset();
}
void Slot::publish_from(std::uint32_t buffer, std::function<void()> done,
                        std::optional<std::size_t> bytes, std::optional<std::uint64_t> timestamp) &&
{
    if(!active_)
        throw std::logic_error("slot already published");
    if(auto pub = state_)
        pub->publish_slot(index_, timestamp, bytes, buffer, std::move(done));
    active_ = false;
    state_.reset();
}
Publisher::Publisher(PublisherLimits limits)
    : p_(std::make_shared<transport::Engine>(std::move(limits)))
{
    p_->start();
}
// Constructed once the delegated constructor returns: a refused description stops the engine.
Publisher::Publisher(Description description, PublisherLimits limits) : Publisher(std::move(limits))
{
    begin(std::move(description));
}
Publisher::Publisher(Publisher &&) noexcept = default;
Publisher &Publisher::operator=(Publisher &&other) noexcept
{
    if(this != &other)
    {
        if(p_)
            p_->stop();
        p_ = std::move(other.p_);
    }
    return *this;
}
Publisher::~Publisher()
{
    if(p_)
        p_->stop();
}
void Publisher::begin(Description description)
{
    if(!p_)
        throw std::logic_error("moved publisher");
    p_->begin(std::move(description));
}
std::optional<Slot> Publisher::acquire(Deadline deadline)
{
    auto pub = p_ ? p_->instance() : nullptr;
    if(!pub || pub->phase.load() >= transport::Phase::Finishing)
        return {};
    int slot_index = pub->grants.pop_until(
        deadline,
        [&] { return pub->phase.load() >= transport::Phase::Finishing || pub->closed(); });
    if(slot_index < 0)
        return {};
    return Slot(std::move(pub), slot_index);
}
// No slot is granted before this, and no every Open is accepted after it.
void Publisher::ucx_start()
{
    if(auto pub = p_ ? p_->instance() : nullptr)
    {
        pub->phase.start();
        p_->poke();
    }
}
// The wait polls an exact count the progress thread keeps, and gives up at the deadline. A
// session may still fail between the count and the start: UcxFailedEvery reports it.
bool Publisher::ucx_start(std::uint32_t every, Deadline deadline)
{
    auto pub = p_ ? p_->instance() : nullptr;
    if(!pub)
        return false;
    while(pub->phase.load() == transport::Phase::Before)
    {
        if(pub->holding.load(std::memory_order_acquire) >= every)
        {
            if(!pub->phase.start()) // a finish won the race
                return false;
            p_->poke();
            return true;
        }
        if(std::chrono::steady_clock::now() >= deadline)
            return false;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return pub->phase.was_started();
}
void Publisher::finish()
{
    if(auto pub = p_ ? p_->instance() : nullptr)
    {
        pub->phase.finish();
        pub->grants.signal();
        p_->poke();
    }
}
std::span<std::byte> Publisher::lent_ring() const
{
    // A finishing instance stays current while the next is made, and the loop may replace the
    // arena its ring is in meanwhile.
    auto pub = p_ ? p_->instance() : nullptr;
    if(!pub || !pub->limits.lend_source || pub->phase.load() >= transport::Phase::Finishing)
        return {};
    return {pub->source_base, std::size_t(pub->plan.source_bytes)};
}
Health Publisher::health() const
{
    if(!p_)
        return {};
    auto pub = p_->instance();
    if(!pub)
        return {.quarantined_bytes = transport::quarantine().total.load(),
                .memory_hooks = p_->memory_hooks};
    auto observation = pub->health_copy.read();
    Health result{.frames = observation.frames,
                  .skipped = observation.skipped,
                  .failed_every = observation.failed_every,
                  .pressure = observation.pressure_from != 0,
                  .pressure_from = observation.pressure_from,
                  .slots_free = observation.slots_free,
                  .slots_held = observation.slots_held,
                  .quarantined_bytes = transport::quarantine().total.load(),
                  .memory_hooks = observation.memory_hooks,
                  .started = pub->phase.was_started(),
                  .topology = pub->topology_copy.read()};
    if(observation.source_failed)
        result.failure = "a GPU source event failed; its memory cannot be reused";
    for(const auto &row : std::span(observation.rows).first(observation.count))
    {
        if(!row.retiring)
            ++(row.every || row.pull ? result.every
                                     : result.latest); // a puller holds the device as every does
        result.transfer_max_ns =
            std::max<std::uint64_t>(result.transfer_max_ns, row.transfer_max_ns);
        result.transfer_mean_ns =
            std::max<std::uint64_t>(result.transfer_mean_ns, row.transfer_mean_ns);
        if(row.transport.n && result.transport.find(row.transport.view()) == std::string::npos)
            result.transport +=
                (result.transport.empty() ? "" : ";") + std::string(row.transport.view());
    }
    return result;
}
Description Publisher::description() const
{
    if(!p_)
        throw std::logic_error("moved publisher");
    auto pub = p_->instance();
    if(!pub)
        throw std::logic_error("no publisher instance: begin has not been called");
    return pub->description;
}
} // namespace TangoUcx

namespace TangoUcx::transport
{
std::string handle(Publisher &publisher, std::string_view document)
{
    wire::Request request;
    auto status = wire::parse(document, request);
    if(status != wire::Ok || !publisher.p_)
        return wire::format(wire::Reply{status != wire::Ok ? status : wire::Finished});
    return wire::format(publisher.p_->control.call(request, std::chrono::steady_clock::now() +
                                                                std::chrono::seconds(3)));
}
namespace
{
std::shared_ptr<Pub> required_instance(const std::shared_ptr<Engine> &p)
{
    if(!p)
        throw std::logic_error("moved publisher");
    auto pub = p->instance();
    if(!pub)
        throw std::runtime_error("the publisher has no instance: none has begun, or the next "
                                 "is being made");
    return pub;
}
} // namespace
std::string ucx_stream(const Publisher &publisher)
{
    auto pub = required_instance(publisher.p_);
    return wire::format(wire::StreamMessage{pub->instance_id, pub->description,
                                            pub->limits.allow_every, pub->limits.max_sessions});
}
// Formatted on the calling thread, from the loop's fixed Health copy.
std::string ucx_sessions(const Publisher &publisher)
{
    auto pub = required_instance(publisher.p_);
    auto observation = pub->health_copy.read();
    wire::SessionsMessage sessions_message{pub->instance_id, {}, observation.missing};
    for(const auto &row : std::span(observation.rows).first(observation.count))
        sessions_message.sessions.push_back(
            {row.ordinal, std::string(row.label.view()), row.every, row.retiring, row.depth,
             row.room, row.sent, row.skipped, std::uint64_t(row.transfer_max_ns) / 1000,
             std::string(row.transport.view()), row.pull, row.passes});
    return wire::format(sessions_message);
}
std::string ucx_topology(const Publisher &publisher)
{
    return wire::format(publisher.health().topology);
}
} // namespace TangoUcx::transport
