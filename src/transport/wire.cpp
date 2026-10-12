// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-ucx project
// SPDX-License-Identifier: LGPL-3.0-or-later
#include "wire.h"
#include "transport.h"

#include <nlohmann/json.hpp>

#include <algorithm>
#include <charconv>
#include <cstring>
#include <limits>
#include <stdexcept>

namespace TangoUcx::wire
{
namespace
{
using json = nlohmann::ordered_json;
constexpr std::string_view k_open = "tango-ucx-open", k_close = "tango-ucx-close",
                           k_reply = "tango-ucx-reply", k_stream = "tango-ucx-stream",
                           k_sessions = "tango-ucx-sessions";
constexpr std::array<std::string_view, 11> k_element_names{
    "u8", "u16", "u32", "u64", "i8", "i16", "i32", "i64", "f32", "f64", "bytes"};
constexpr std::array<std::string_view, 13> k_status_names{
    "ok",         "malformed", "version",  "instance_id",     "every-refused",
    "no-session", "endpoint",  "finished", "unknown-session", "busy",
    "timeout",    "started",   "range"};

std::string interface(std::string_view name)
{
    return std::string(name) + "/" + std::to_string(k_major) + "." + std::to_string(k_minor);
}
std::string dump(const json &j)
{
    return j.dump(-1, ' ', false, json::error_handler_t::replace);
}
bool number(std::string_view s, unsigned &v)
{
    auto [end, ec] = std::from_chars(s.data(), s.data() + s.size(), v);
    return ec == std::errc{} && end == s.data() + s.size() && !s.empty();
}
// Any minor of our major is accepted: a minor only adds keys.
Status check_interface(const json &j, std::string_view &name)
{
    if(!j.is_object())
        return Malformed;
    auto it = j.find("interface");
    if(it == j.end() || !it->is_string())
        return Malformed;
    std::string_view v = it->get_ref<const std::string &>();
    auto slash = v.rfind('/');
    auto dot = v.find('.', slash == v.npos ? 0 : slash);
    unsigned major = 0, minor = 0;
    if(slash == v.npos || dot == v.npos || !number(v.substr(slash + 1, dot - slash - 1), major) ||
       !number(v.substr(dot + 1), minor))
        return Malformed;
    name = v.substr(0, slash);
    return major == k_major ? Ok : Version;
}
bool unsigned_key(const json &j, const char *key, std::uint64_t max, std::uint64_t &v)
{
    auto it = j.find(key);
    if(it == j.end() || !it->is_number_unsigned())
        return false;
    v = it->get<std::uint64_t>();
    return v <= max;
}
bool string_key(const json &j, const char *key, std::size_t max, std::string_view &v)
{
    auto it = j.find(key);
    if(it == j.end() || !it->is_string())
        return false;
    v = it->get_ref<const std::string &>();
    return v.size() <= max;
}
bool bool_key(const json &j, const char *key, bool &v)
{
    auto it = j.find(key);
    if(it == j.end() || !it->is_boolean())
        return false;
    v = it->get<bool>();
    return true;
}
std::string hex(std::span<const std::byte> b)
{
    constexpr char digits[] = "0123456789abcdef";
    std::string s(2 * b.size(), '0');
    for(std::size_t i = 0; i < b.size(); ++i)
    {
        auto v = std::to_integer<unsigned>(b[i]);
        s[2 * i] = digits[v >> 4];
        s[2 * i + 1] = digits[v & 15];
    }
    return s;
}
bool unhex(std::string_view s, std::span<std::byte> out, std::uint16_t &n)
{
    auto nibble = [](char c) -> int
    {
        if(c >= '0' && c <= '9')
            return c - '0';
        if(c >= 'a' && c <= 'f')
            return c - 'a' + 10;
        if(c >= 'A' && c <= 'F')
            return c - 'A' + 10;
        return -1;
    };
    if(s.size() % 2 || s.size() / 2 > out.size())
        return false;
    for(std::size_t i = 0; i < s.size() / 2; ++i)
    {
        int hi = nibble(s[2 * i]), lo = nibble(s[2 * i + 1]);
        if(hi < 0 || lo < 0)
            return false;
        out[i] = std::byte(hi << 4 | lo);
    }
    n = std::uint16_t(s.size() / 2);
    return true;
}
json parse_json(std::string_view doc)
{
    if(doc.size() > k_max_document)
        return json(json::value_t::discarded);
    return json::parse(doc, nullptr, false);
}
Status parse_open(const json &j, OpenMessage &m)
{
    constexpr auto max64 = std::numeric_limits<std::uint64_t>::max();
    std::uint64_t depth = 0, batch = 0, range = 0;
    std::string_view delivery, label, address;
    bool ok = unsigned_key(j, "session", max64, m.session) && m.session &&
              unsigned_key(j, "instance_id", max64, m.instance_id) &&
              unsigned_key(j, "depth", 65536, depth) && depth &&
              unsigned_key(j, "batch", depth, batch) && batch &&
              unsigned_key(j, "min_interval_ns", max64, m.min_interval_ns) &&
              string_key(j, "delivery", 6, delivery) &&
              (delivery == "every" || delivery == "latest" ||
               (delivery == "pull" && unsigned_key(j, "range", UINT32_MAX, range) && range)) &&
              bool_key(j, "gpu", m.gpu) && string_key(j, "label", k_max_label, label) &&
              string_key(j, "address", 2 * k_max_address, address) &&
              unhex(address, m.address, m.address_bytes) && m.address_bytes;
    if(!ok || (j.contains("ring_bytes") &&
               !(unsigned_key(j, "ring_bytes", max64, m.ring_bytes) && m.ring_bytes)))
        return Malformed;
    m.depth = std::uint32_t(depth);
    m.batch = std::uint32_t(batch);
    m.range = std::uint32_t(range);
    m.every = delivery == "every";
    m.label_bytes = std::uint8_t(label.size());
    std::memcpy(m.label.data(), label.data(), label.size());
    return Ok;
}
} // namespace

std::string_view status_name(Status s)
{
    return s < k_status_names.size() ? k_status_names[s] : "malformed";
}
std::string format(const OpenMessage &m)
{
    json j = {{"interface", interface(k_open)},
              {"session", m.session},
              {"instance_id", m.instance_id},
              {"depth", m.depth},
              {"batch", m.batch},
              {"min_interval_ns", m.min_interval_ns},
              {"delivery", m.range   ? "pull"
                           : m.every ? "every"
                                     : "latest"},
              {"gpu", m.gpu},
              {"label", std::string(m.label.data(), m.label_bytes)},
              {"address", hex({m.address.data(), m.address_bytes})}};
    if(m.ring_bytes)
        j["ring_bytes"] = m.ring_bytes;
    if(m.range)
        j["range"] = m.range;
    return dump(j);
}
std::string format(const CloseMessage &m)
{
    return dump({{"interface", interface(k_close)}, {"session", m.session}});
}
std::string format(const Reply &r)
{
    json j = {{"interface", interface(k_reply)}, {"status", std::string(status_name(r.status))}};
    if(r.status == Ok && r.address_bytes)
        j["address"] = hex({r.address.data(), r.address_bytes});
    if(r.detail_bytes)
        j["detail"] = std::string(r.detail.data(), r.detail_bytes);
    return dump(j);
}
std::string format(const StreamMessage &m)
{
    const auto &d = m.description;
    json j = {{"interface", interface(k_stream)},
              {"instance_id", m.instance_id},
              {"element", k_element_names.at(std::size_t(d.element))}};
    // An opaque stream has no shape, so a 3.0 subscriber, which needs one, refuses it before Open.
    if(d.max_payload_bytes)
        j["max_payload_bytes"] = d.max_payload_bytes;
    else
        j["shape"] = d.shape;
    j.update({{"field_bytes", d.field_bytes},
              {"field_dtype", d.field_dtype},
              {"allow_every", m.allow_every},
              {"max_sessions", m.max_sessions}});
    if(!d.application_text.empty())
        j["application_text"] = d.application_text;
    return dump(j);
}
bool valid_text(std::string_view text)
{
    // A byte that is not UTF-8 is replaced when the text is dumped, so it reads back changed.
    return text.size() <= k_max_text &&
           json::parse(dump(json(std::string(text)))).get<std::string>() == text;
}
std::string format(const SessionsMessage &m)
{
    auto sessions = json::array();
    for(const auto &r : m.sessions)
        sessions.push_back(json{{"ordinal", r.ordinal},
                                {"label", r.label},
                                {"delivery", r.pull    ? "pull"
                                             : r.every ? "every"
                                                       : "latest"},
                                {"state", r.retiring ? "retiring" : "ready"},
                                {"depth", r.depth},
                                {"room", r.room},
                                {"sent", r.sent},
                                {"skipped", r.skipped},
                                {"transfer_max_us", r.transfer_max_us},
                                {"transport", r.transport},
                                {"passes", r.passes}});
    return dump({{"interface", interface(k_sessions)},
                 {"instance_id", m.instance_id},
                 {"missing", m.missing},
                 {"sessions", std::move(sessions)}});
}
std::string format(const Topology &topology)
{
    auto devices = json::array();
    for(const auto &device : topology.network_devices)
        devices.push_back(json{{"name", device.name}, {"numa_node", device.numa_node}});
    return dump({{"cpu", topology.cpu},
                 {"numa_node", topology.numa_node},
                 {"ring_numa_node", topology.ring_numa_node},
                 {"allowed_cpus", topology.allowed_cpus},
                 {"sibling_cpus", topology.sibling_cpus},
                 {"network_devices", std::move(devices)}});
}
Status parse(std::string_view doc, StreamMessage &m)
{
    auto j = parse_json(doc);
    std::string_view name, element, dtype, text;
    if(auto st = check_interface(j, name); st != Ok)
        return st;
    m = {};
    std::uint64_t field_bytes = 0, sessions = 0;
    auto shape = j.find("shape");
    bool opaque = j.contains("max_payload_bytes");
    bool ok =
        name == k_stream &&
        unsigned_key(j, "instance_id", std::numeric_limits<std::uint64_t>::max(), m.instance_id) &&
        string_key(j, "element", 8, element) && unsigned_key(j, "field_bytes", 1024, field_bytes) &&
        string_key(j, "field_dtype", k_max_document, dtype) &&
        bool_key(j, "allow_every", m.allow_every) &&
        unsigned_key(j, "max_sessions", 64, sessions) && sessions &&
        (opaque ? shape == j.end() &&
                      unsigned_key(j, "max_payload_bytes", 1ull << 31,
                                   m.description.max_payload_bytes) &&
                      m.description.max_payload_bytes
                : shape != j.end() && shape->is_array() && !shape->empty() && shape->size() <= 8);
    auto e = std::find(k_element_names.begin(), k_element_names.end(), element);
    // A publisher without an application text leaves the key out.
    ok = ok &&
         (!j.contains("application_text") || string_key(j, "application_text", k_max_text, text));
    if(!ok || e == k_element_names.end())
        return Malformed;
    for(const auto &extent : opaque ? json::array() : *shape)
    {
        if(!extent.is_number_unsigned() || extent.get<std::uint64_t>() == 0)
            return Malformed;
        m.description.shape.push_back(extent.get<std::uint64_t>());
    }
    m.description.element = Element(e - k_element_names.begin());
    m.description.field_bytes = std::uint32_t(field_bytes);
    m.description.field_dtype = dtype;
    m.description.application_text = text;
    m.max_sessions = std::uint32_t(sessions);
    return Ok;
}
Status parse(std::string_view doc, Request &r)
{
    auto j = parse_json(doc);
    std::string_view name;
    if(auto st = check_interface(j, name); st != Ok)
        return st;
    r = {};
    if(name == k_close)
        return unsigned_key(j, "session", std::numeric_limits<std::uint64_t>::max(),
                            r.close.session) &&
                       r.close.session
                   ? Ok
                   : Malformed;
    if(name != k_open)
        return Malformed;
    return parse_open(j, r.open.emplace());
}
bool parse(std::string_view doc, Reply &r)
{
    auto j = parse_json(doc);
    std::string_view name, status, address, detail;
    if(check_interface(j, name) != Ok || name != k_reply || !string_key(j, "status", 32, status))
        return false;
    r = {};
    auto it = std::find(k_status_names.begin(), k_status_names.end(), status);
    if(it == k_status_names.end())
        return false;
    r.status = Status(it - k_status_names.begin());
    if(string_key(j, "detail", k_max_document, detail))
    {
        r.detail_bytes = std::uint8_t(std::min(detail.size(), r.detail.size()));
        std::memcpy(r.detail.data(), detail.data(), r.detail_bytes);
    }
    if(!j.contains("address"))
        return true;
    return string_key(j, "address", 2 * k_max_address, address) &&
           unhex(address, r.address, r.address_bytes);
}

std::uint64_t get64(const std::byte *p) noexcept
{
    std::uint64_t v = 0;
    for(int i = 0; i < 8; ++i)
        v |= std::uint64_t(std::to_integer<unsigned>(p[i])) << (8 * i);
    return v;
}
void put64(std::byte *p, std::uint64_t v) noexcept
{
    for(int i = 0; i < 8; ++i)
        p[i] = std::byte(v >> (8 * i));
}

std::array<std::byte, 8> encode(const SessionMessage &m)
{
    std::array<std::byte, 8> b{};
    put64(b.data(), m.session);
    return b;
}
bool decode(std::span<const std::byte> b, SessionMessage &m)
{
    if(b.size() != 8)
        return false;
    m.session = get64(b.data());
    return true;
}
std::size_t encode_into(std::span<std::byte> b, const FrameMessage &m)
{
    std::size_t fixed = m.packed ? 48 : 40;
    auto n = fixed + m.record_bytes;
    if(m.record_bytes > m.record.size() || b.size() < n)
        return 0;
    put64(b.data(), m.session);
    put64(b.data() + 8, m.sequence);
    put64(b.data() + 16, m.payload_bytes);
    put64(b.data() + 24, m.index);
    put64(b.data() + 32, m.timestamp_ns);
    if(m.packed)
        put64(b.data() + 40, m.position);
    std::memcpy(b.data() + fixed, m.record.data(), m.record_bytes);
    return n;
}
// The length says which frame it is: a packed ring's has its position, 8 bytes more.
bool decode(std::span<const std::byte> b, std::size_t record_bytes, FrameMessage &m)
{
    if(record_bytes > m.record.size() ||
       (b.size() != 40 + record_bytes && b.size() != 48 + record_bytes))
        return false;
    m.session = get64(b.data());
    m.sequence = get64(b.data() + 8);
    m.payload_bytes = get64(b.data() + 16);
    m.index = get64(b.data() + 24);
    m.timestamp_ns = get64(b.data() + 32);
    m.packed = b.size() == 48 + record_bytes;
    m.position = m.packed ? get64(b.data() + 40) : 0;
    m.record_bytes = std::uint16_t(record_bytes);
    std::memcpy(m.record.data(), b.data() + b.size() - record_bytes, record_bytes);
    return true;
}
std::array<std::byte, 24> encode(const CreditMessage &m)
{
    std::array<std::byte, 24> b{};
    put64(b.data(), m.session);
    put64(b.data() + 8, m.credit);
    put64(b.data() + 16, m.bytes);
    return b;
}
bool decode(std::span<const std::byte> b, CreditMessage &m)
{
    if(b.size() != 16 && b.size() != 24)
        return false;
    m = {get64(b.data()), get64(b.data() + 8), b.size() == 24 ? get64(b.data() + 16) : 0};
    return true;
}
std::array<std::byte, 32> encode(const EndMessage &m)
{
    std::array<std::byte, 32> b{};
    put64(b.data(), m.session);
    put64(b.data() + 8, m.sequences);
    put64(b.data() + 16, m.frames);
    put64(b.data() + 24, m.reason | m.missing << 8);
    return b;
}
bool decode(std::span<const std::byte> b, EndMessage &m)
{
    if(b.size() != 32 || std::to_integer<unsigned>(b[24]) > Closing)
        return false;
    m = {get64(b.data()), get64(b.data() + 8), get64(b.data() + 16),
         Reason(std::to_integer<unsigned>(b[24])), get64(b.data() + 24) >> 8};
    return true;
}
} // namespace TangoUcx::wire

namespace TangoUcx::transport
{
int marker_major(std::string_view description)
{
    constexpr std::string_view prefix = "tango-ucx/";
    if(!description.starts_with(prefix))
        return -1;
    description.remove_prefix(prefix.size());
    auto space = description.find(' ');
    unsigned major = 0;
    if(space == description.npos || !wire::number(description.substr(0, space), major) ||
       major > 255)
        return -1;
    auto role = description.substr(space + 1);
    return role == "open" || role == "close" ? int(major) : -1;
}
Offer parse_ucx_stream(std::string_view document)
{
    wire::StreamMessage m;
    auto status = wire::parse(document, m);
    if(status != wire::Ok)
        throw std::runtime_error(status == wire::Version
                                     ? "UcxStream speaks another tango-ucx major version"
                                     : "UcxStream is not a valid tango-ucx-stream document");
    Offer offer{std::move(m.description), {}, m.instance_id};
    offer.limits.allow_every = m.allow_every;
    offer.limits.max_sessions = m.max_sessions;
    return offer;
}
} // namespace TangoUcx::transport
