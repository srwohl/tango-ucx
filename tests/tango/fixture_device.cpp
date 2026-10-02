// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-ucx project
// SPDX-License-Identifier: LGPL-3.0-or-later
#include <tango-ucx/tango.h>

#include <tango/tango.h>

#include <cstdint>
#include <cstring>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <thread>
#include <unistd.h>

namespace
{
using namespace TangoUcx;

// Publishes frames of 4,096 bytes, each filled with its frame index's low byte, and the index
// as its one application field. The UCX commands are installed as StreamOpen and StreamClose.
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
        limits.max_sessions = sessions;
        limits.allow_every = true;
        publisher.emplace(Description{Element::U8, {4096}, 8, "<u8"}, limits);
        next = 0;
        attach_publisher(*this, *publisher);
        set_state(Tango::ON);
    }
    void delete_device() override
    {
        frames = {};
        detach_publisher(*this);
        publisher.reset();
    }
    void publish(Slot &slot)
    {
        std::memset(slot.payload().data(), int(next & 0xff), slot.payload().size());
        std::memcpy(slot.fields().data(), &next, sizeof(next));
        std::move(slot).publish();
        ++next;
    }
    Tango::DevLong publish(Tango::DevLong n)
    {
        publisher->ucx_start();
        Tango::DevLong done = 0;
        for(; done < n; ++done)
        {
            auto slot =
                publisher->acquire(std::chrono::steady_clock::now() + std::chrono::seconds(1));
            if(!slot)
                break;
            publish(*slot);
        }
        return done;
    }
    // Ends this incarnation and makes another with `rows` session rows. The subscribers of this
    // one get End, and five seconds to close their sessions.
    void prepare(Tango::DevLong rows)
    {
        publisher->finish();
        auto until = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        for(auto h = publisher->health();
            h.every + h.latest && std::chrono::steady_clock::now() < until; h = publisher->health())
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        delete_device();
        sessions = std::uint32_t(rows);
        init_device();
    }
    // Publishes n frames from a thread of the device, as fast as its sessions allow, then ends
    // the incarnation.
    void start(Tango::DevLong n)
    {
        publisher->ucx_start();
        frames = std::jthread(
            [this, n](std::stop_token stop)
            {
                for(Tango::DevLong i = 0; i < n && !stop.stop_requested();)
                    if(auto slot = publisher->acquire(std::chrono::steady_clock::now() +
                                                      std::chrono::milliseconds(100)))
                        publish(*slot), ++i;
                publisher->finish();
            });
    }
    std::optional<Publisher> publisher;
    std::uint64_t next = 0;
    std::uint32_t sessions = 2;
    std::jthread frames;
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
    DeviceClass() : Tango::DeviceClass(std::string("UcxFixture")) {}
    void command_factory() override
    {
        command_list.push_back(new Command("Publish", Tango::DEV_LONG, Tango::DEV_LONG,
                                           [](Command &c, Device &d, const CORBA::Any &in)
                                           {
                                               Tango::DevLong n = 0;
                                               c.extract(in, n);
                                               return c.insert(d.publish(n));
                                           }));
        command_list.push_back(new Command("Finish", Tango::DEV_VOID, Tango::DEV_VOID,
                                           [](Command &c, Device &d, const CORBA::Any &)
                                           {
                                               d.publisher->finish();
                                               return c.insert();
                                           }));
        command_list.push_back(new Command("Prepare", Tango::DEV_LONG, Tango::DEV_VOID,
                                           [](Command &c, Device &d, const CORBA::Any &in)
                                           {
                                               Tango::DevLong n = 0;
                                               c.extract(in, n);
                                               d.prepare(n);
                                               return c.insert();
                                           }));
        command_list.push_back(new Command("Start", Tango::DEV_LONG, Tango::DEV_VOID,
                                           [](Command &c, Device &d, const CORBA::Any &in)
                                           {
                                               Tango::DevLong n = 0;
                                               c.extract(in, n);
                                               d.start(n);
                                               return c.insert();
                                           }));
        install_ucx_commands(command_list, "Stream");
    }
    void attribute_factory(std::vector<Tango::Attr *> &list) override
    {
        install_ucx_attributes(list);
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
        // UCX commands run concurrently with Prepare, so detach_publisher's wait is exercised.
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
