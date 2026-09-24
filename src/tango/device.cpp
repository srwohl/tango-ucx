// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
// SPDX-License-Identifier: LGPL-3.0-or-later
#include <tango-bulk/tango.h>
#include <transport.h>
#include <wire.h>

#include <tango/tango.h>

#include <map>
#include <mutex>
#include <shared_mutex>
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

class BulkCommand : public Tango::Command
{
  public:
    BulkCommand(const std::string &name, const char *marker)
        : Tango::Command(name, Tango::DEV_STRING, Tango::DEV_STRING, marker,
                         "tango-bulk-reply JSON document", Tango::EXPERT)
    {
    }
    CORBA::Any *execute(Tango::DeviceImpl *device, const CORBA::Any &in) override
    {
        const char *document = nullptr;
        extract(in, document);
        auto reply =
            with_publisher(device, [&](Publisher &p) { return transport::handle(p, document); });
        return insert(reply.c_str());
    }
};

class StreamAttr : public Tango::Attr
{
  public:
    StreamAttr() : Tango::Attr("BulkStream", Tango::DEV_STRING, Tango::READ) {}
    void read(Tango::DeviceImpl *device, Tango::Attribute &a) override
    {
        auto doc = with_publisher(device, [](Publisher &p) { return transport::bulk_stream(p); });
        a.set_value(new Tango::DevString(Tango::string_dup(doc.c_str())), 1, 0, true);
    }
};
class SessionsAttr : public Tango::SpectrumAttr
{
  public:
    SessionsAttr() : Tango::SpectrumAttr("BulkSessions", Tango::DEV_STRING, Tango::READ, 64) {}
    void read(Tango::DeviceImpl *device, Tango::Attribute &a) override
    {
        auto rows = with_publisher(device, [](Publisher &p) { return transport::session_rows(p); });
        auto *values = new Tango::DevString[rows.size()];
        for(std::size_t i = 0; i < rows.size(); ++i)
            values[i] = Tango::string_dup(rows[i].c_str());
        a.set_value(values, long(rows.size()), 0, true);
    }
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
} // namespace

void install_bulk_commands(std::vector<Tango::Command *> &list, std::string_view prefix)
{
    list.push_back(new BulkCommand(std::string(prefix) + "Open", wire::k_open_marker));
    list.push_back(new BulkCommand(std::string(prefix) + "Close", wire::k_close_marker));
}
void install_bulk_attributes(std::vector<Tango::Attr *> &list)
{
    using U64 = Tango::DevULong64;
    using U32 = Tango::DevULong;
    add(list, new StreamAttr, "The bulk stream: a tango-bulk-stream JSON document");
    add(list, new SessionsAttr,
        "One row per session: ordinal|label|delivery|state|depth|room|sent|skipped|"
        "transfer_max_us|transport");
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
