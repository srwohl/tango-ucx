// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
// SPDX-License-Identifier: LGPL-3.0-or-later
#include <tango-bulk/tango.h>

#include <tango/tango.h>

#include <cstring>
#include <iostream>
#include <optional>
#include <thread>
#include <unistd.h>

namespace
{
using namespace TangoBulk;

// Publishes frames of 4,096 bytes, each filled with its frame index's low byte, and the index
// as its one application field. The bulk commands are installed as StreamOpen and StreamClose.
class Device : public TANGO_BASE_CLASS
{
  public:
    Device(Tango::DeviceClass *c, const std::string &name) : TANGO_BASE_CLASS(c, name.c_str())
    {
        Device::init_device();
    }
    ~Device() override
    {
        Device::delete_device();
    }
    void init_device() override
    {
        PublisherLimits limits;
        limits.budget = 16ull << 20;
        limits.max_sessions = 2;
        limits.allow_every = true;
        bulk.emplace(Description{Element::U8, {4096}, 8, "<u8"}, limits);
        next = 0;
        attach_publisher(*this, *bulk);
        set_state(Tango::ON);
    }
    void delete_device() override
    {
        detach_publisher(*this);
        bulk.reset();
    }
    Tango::DevLong publish(Tango::DevLong n)
    {
        Tango::DevLong done = 0;
        for(; done < n; ++done, ++next)
        {
            auto slot = bulk->acquire(std::chrono::steady_clock::now() + std::chrono::seconds(1));
            if(!slot)
                break;
            std::memset(slot->payload().data(), int(next & 0xff), slot->payload().size());
            std::memcpy(slot->fields().data(), &next, sizeof(next));
            std::move(*slot).publish();
        }
        return done;
    }
    // The loop grants every free slot ahead of the device, and grants issued before an every
    // session's Open never reach it. Cancelling them, which uses no frame index, lets the next
    // frame reach every open session.
    Tango::DevLong drain()
    {
        auto until = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        auto n = bulk->health().slots_held;
        for(; !n && std::chrono::steady_clock::now() < until; n = bulk->health().slots_held)
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        for(auto i = n; i; --i)
            bulk->acquire(std::chrono::steady_clock::now() + std::chrono::seconds(1));
        return Tango::DevLong(n);
    }
    std::optional<Publisher> bulk;
    std::uint64_t next = 0;
};

class Command : public Tango::Command
{
  public:
    using Run = CORBA::Any *(*)(Command &, Device &, const CORBA::Any &);
    Command(const char *name, Tango::CmdArgType in, Tango::CmdArgType out, Run run)
        : Tango::Command(name, in, out), run_(run)
    {
    }
    CORBA::Any *execute(Tango::DeviceImpl *d, const CORBA::Any &in) override
    {
        return run_(*this, *static_cast<Device *>(d), in);
    }
    Run run_;
};

class DeviceClass : public Tango::DeviceClass
{
  public:
    DeviceClass() : Tango::DeviceClass(std::string("BulkFixture")) {}
    void command_factory() override
    {
        command_list.push_back(new Command("Publish", Tango::DEV_LONG, Tango::DEV_LONG,
                                           [](Command &c, Device &d, const CORBA::Any &in)
                                           {
                                               Tango::DevLong n = 0;
                                               c.extract(in, n);
                                               return c.insert(d.publish(n));
                                           }));
        command_list.push_back(new Command("Drain", Tango::DEV_VOID, Tango::DEV_LONG,
                                           [](Command &c, Device &d, const CORBA::Any &)
                                           { return c.insert(d.drain()); }));
        command_list.push_back(new Command("Finish", Tango::DEV_VOID, Tango::DEV_VOID,
                                           [](Command &c, Device &d, const CORBA::Any &)
                                           {
                                               d.bulk->finish();
                                               return c.insert();
                                           }));
        // A new incarnation: detach, destroy the publisher, make and attach another.
        command_list.push_back(new Command("Restart", Tango::DEV_VOID, Tango::DEV_VOID,
                                           [](Command &c, Device &d, const CORBA::Any &)
                                           {
                                               d.delete_device();
                                               d.init_device();
                                               return c.insert();
                                           }));
        install_bulk_commands(command_list, "Stream");
    }
    void attribute_factory(std::vector<Tango::Attr *> &list) override
    {
        install_bulk_attributes(list);
    }
    void device_factory(const Tango::DevVarStringArray *names) override
    {
        for(CORBA::ULong i = 0; i < names->length(); ++i)
            device_list.push_back(new Device(this, std::string((*names)[i])));
        for(auto *d : device_list) // with -nodb the CORBA object key is how clients find it
            export_device(d, d->get_name().c_str());
    }
};

class Server : public Tango::DServer
{
  public:
    using Tango::DServer::DServer;

  private:
    void class_factory() override
    {
        add_class(new DeviceClass);
    }
};
} // namespace

int main(int argc, char *argv[])
{
    try
    {
        auto *util = Tango::Util::init(argc, argv);
        util->register_dserver_constructor(
            [](Tango::DeviceClass *c, const std::string &name, const std::string &description,
               Tango::DevState state, const std::string &status) -> Tango::DServer *
            { return new Server(c, name.c_str(), description.c_str(), state, status.c_str()); });
        // Bulk commands run concurrently with Restart, so detach_publisher's wait is exercised.
        util->set_serial_model(Tango::NO_SYNC);
        util->server_init();
        (void)!::write(3, "READY\n", 6);
        util->server_run();
        util->server_cleanup();
    }
    catch(const Tango::DevFailed &e)
    {
        Tango::Except::print_exception(e);
        return 1;
    }
    catch(const std::exception &e)
    {
        std::cerr << "fixture_device: " << e.what() << std::endl;
        return 1;
    }
}
