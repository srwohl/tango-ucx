// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-ucx project
// SPDX-License-Identifier: LGPL-3.0-or-later
#include <tango-ucx/tango.h>
#include <transport.h>
#include <wire.h>

#include <tango/tango.h>

#include <algorithm>
#include <cctype>
#include <map>
#include <mutex>
#include <shared_mutex>
#include <stdexcept>
#include <string>

namespace TangoUcx
{
namespace
{
struct Registry
{
    std::shared_mutex m;
    std::map<const Tango::DeviceImpl *, Publisher *> publishers;
};
Registry &registry()
{
    static Registry r;
    return r;
}
// Holds the registry's shared lock while f runs, so detach_publisher waits for it.
template <class F> auto with_publisher(Tango::DeviceImpl *device, F f)
{
    auto &r = registry();
    std::shared_lock l(r.m);
    auto it = r.publishers.find(device);
    if(it == r.publishers.end())
        Tango::Except::throw_exception("UcxUnavailable",
                                       "no UCX publisher is attached to this device", "tango-ucx");
    try
    {
        return f(*it->second);
    }
    catch(const std::exception &e)
    {
        Tango::Except::throw_exception("UcxFailed", e.what(), "tango-ucx");
    }
}

std::string command_reply(Tango::DeviceImpl *device, std::string_view document)
{
    auto &r = registry();
    std::shared_lock l(r.m);
    auto it = r.publishers.find(device);
    if(it == r.publishers.end())
        return wire::format(wire::Reply{wire::Finished});
    try
    {
        return transport::handle(*it->second, document);
    }
    catch(const std::exception &e)
    {
        Tango::Except::throw_exception("UcxFailed", e.what(), "tango-ucx");
    }
}

class UcxCommand : public Tango::Command
{
  public:
    UcxCommand(const std::string &name, const char *marker)
        : Tango::Command(name, Tango::DEV_STRING, Tango::DEV_STRING, marker,
                         "a tango-ucx-reply JSON document", Tango::OPERATOR)
    {
    }
    CORBA::Any *execute(Tango::DeviceImpl *device, const CORBA::Any &in) override
    {
        const char *document = nullptr;
        extract(in, document);
        auto reply = command_reply(device, document);
        return insert(reply.c_str());
    }
};

// A scalar DevString attribute holding one JSON document.
class DocumentAttr : public Tango::Attr
{
  public:
    DocumentAttr(const char *name, std::string (*get)(const Publisher &))
        : Tango::Attr(name, Tango::DEV_STRING, Tango::READ), get_(get)
    {
    }
    void read(Tango::DeviceImpl *device, Tango::Attribute &a) override
    {
        auto doc = with_publisher(device, [&](Publisher &p) { return get_(p); });
        a.set_value(new Tango::DevString(Tango::string_dup(doc.c_str())), 1, 0, true);
    }

  private:
    std::string (*get_)(const Publisher &);
};
template <class T> class HealthAttr : public Tango::Attr
{
  public:
    HealthAttr(const char *name, long type, T (*get)(const Health &))
        : Tango::Attr(name, type, Tango::READ), get_(get)
    {
    }
    void read(Tango::DeviceImpl *device, Tango::Attribute &a) override
    {
        auto v = with_publisher(device, [&](Publisher &p) { return get_(p.health()); });
        a.set_value(new T(v), 1, 0, true);
    }

  private:
    T (*get_)(const Health &);
};
void add(std::vector<Tango::Attr *> &list, Tango::Attr *attr, const char *description)
{
    Tango::UserDefaultAttrProp properties;
    properties.set_description(description);
    attr->set_default_properties(properties);
    list.push_back(attr);
}
std::string lower(std::string s)
{
    std::transform(s.begin(), s.end(), s.begin(),
                   [](unsigned char c) { return char(std::tolower(c)); });
    return s;
}
} // namespace

void install_ucx_commands(std::vector<Tango::Command *> &list, std::string_view prefix)
{
    std::string open = std::string(prefix) + "Open", close = std::string(prefix) + "Close";
    for(auto *command : list)
        if(command && (lower(command->get_name()) == lower(open) ||
                       lower(command->get_name()) == lower(close)))
            throw std::invalid_argument("the device already has a command named " +
                                        command->get_name());
    list.push_back(new UcxCommand(open, wire::k_open_marker));
    list.push_back(new UcxCommand(close, wire::k_close_marker));
}
void install_ucx_attributes(std::vector<Tango::Attr *> &list)
{
    using U64 = Tango::DevULong64;
    using U32 = Tango::DevULong;
    add(list, new DocumentAttr("UcxStream", transport::ucx_stream),
        "The UCX stream: a tango-ucx-stream JSON document");
    add(list, new DocumentAttr("UcxSessions", transport::ucx_sessions),
        "The sessions, sampled with Health: a tango-ucx-sessions JSON document");
    add(list, new DocumentAttr("UcxTopology", transport::ucx_topology),
        "Publisher topology sampled on its progress thread: CPU, NUMA, ring and network devices "
        "as JSON");
    add(list,
        new HealthAttr<U64>("UcxPublished", Tango::DEV_ULONG64,
                            [](const Health &h) { return U64(h.frames); }),
        "Frames published by this publisher instance");
    add(list,
        new HealthAttr<U64>("UcxSkipped", Tango::DEV_ULONG64,
                            [](const Health &h) { return U64(h.skipped); }),
        "Frames latest sessions skipped");
    add(list,
        new HealthAttr<U32>("UcxPressure", Tango::DEV_ULONG,
                            [](const Health &h) { return U32(h.pressure_from); }),
        "The UcxSessions ordinal of the every or pull session the device waits for; 0: none");
    add(list,
        new HealthAttr<U32>("UcxFailedEvery", Tango::DEV_ULONG,
                            [](const Health &h) { return U32(h.failed_every); }),
        "Every and pull sessions that ended without the subscriber closing them");
}
void attach_publisher(Tango::DeviceImpl &device, Publisher &publisher)
{
    std::unique_lock l(registry().m);
    registry().publishers[&device] = &publisher;
}
void detach_publisher(Tango::DeviceImpl &device)
{
    std::unique_lock l(registry().m);
    registry().publishers.erase(&device);
}
} // namespace TangoUcx
