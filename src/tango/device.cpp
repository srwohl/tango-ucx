// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
// SPDX-License-Identifier: LGPL-3.0-or-later
#include <tango-bulk/tango.h>
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

namespace TangoBulk
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
        Tango::Except::throw_exception(
            "BulkUnavailable", "no bulk publisher is attached to this device", "tango-bulk");
    try
    {
        return f(*it->second);
    }
    catch(const std::exception &e)
    {
        Tango::Except::throw_exception("BulkFailed", e.what(), "tango-bulk");
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
        Tango::Except::throw_exception("BulkFailed", e.what(), "tango-bulk");
    }
}

class BulkCommand : public Tango::Command
{
  public:
    BulkCommand(const std::string &name, const char *marker)
        : Tango::Command(name, Tango::DEV_STRING, Tango::DEV_STRING, marker,
                         "a tango-bulk-reply JSON document", Tango::OPERATOR)
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

void install_bulk_commands(std::vector<Tango::Command *> &list, std::string_view prefix)
{
    std::string open = std::string(prefix) + "Open", close = std::string(prefix) + "Close";
    for(auto *command : list)
        if(command && (lower(command->get_name()) == lower(open) ||
                       lower(command->get_name()) == lower(close)))
            throw std::invalid_argument("the device already has a command named " +
                                        command->get_name());
    list.push_back(new BulkCommand(open, wire::k_open_marker));
    list.push_back(new BulkCommand(close, wire::k_close_marker));
}
void install_bulk_attributes(std::vector<Tango::Attr *> &list)
{
    using U64 = Tango::DevULong64;
    using U32 = Tango::DevULong;
    add(list, new DocumentAttr("BulkStream", transport::bulk_stream),
        "The bulk stream: a tango-bulk-stream JSON document");
    add(list, new DocumentAttr("BulkSessions", transport::bulk_sessions),
        "The sessions, sampled with Health: a tango-bulk-sessions JSON document");
    add(list,
        new HealthAttr<U64>("BulkPublished", Tango::DEV_ULONG64,
                            [](const Health &h) { return U64(h.frames); }),
        "Frames published in this incarnation");
    add(list,
        new HealthAttr<U64>("BulkSkipped", Tango::DEV_ULONG64,
                            [](const Health &h) { return U64(h.skipped); }),
        "Frames latest sessions skipped");
    add(list,
        new HealthAttr<U32>("BulkPressure", Tango::DEV_ULONG,
                            [](const Health &h) { return U32(h.pressure_from); }),
        "The BulkSessions ordinal of the every session the device waits for; 0: none");
    add(list,
        new HealthAttr<U32>("BulkFailedEvery", Tango::DEV_ULONG,
                            [](const Health &h) { return U32(h.failed_every); }),
        "Every sessions that ended without the subscriber closing them");
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
} // namespace TangoBulk
