// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
// SPDX-License-Identifier: LGPL-3.0-or-later
#include "wire.h"

#include <nlohmann/json.hpp>

#include <algorithm>
#include <charconv>
#include <cstring>
#include <limits>

namespace TangoBulk::wire
{
namespace
{
using json = nlohmann::ordered_json;
constexpr std::string_view k_open = "tango-bulk-open", k_close = "tango-bulk-close",
                           k_reply = "tango-bulk-reply";
constexpr std::array<std::string_view, 11> k_status_names{
    "ok",       "malformed", "version",         "incarnation", "every-refused", "no-session",
    "endpoint", "finished",  "unknown-session", "busy",        "timeout"};

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
    std::uint64_t depth = 0, batch = 0;
    std::string_view delivery, label, address;
    bool ok =
        unsigned_key(j, "session", max64, m.session) && m.session &&
        unsigned_key(j, "incarnation", max64, m.incarnation) &&
        unsigned_key(j, "depth", 65536, depth) && depth && unsigned_key(j, "batch", depth, batch) &&
        batch && unsigned_key(j, "min_interval_ns", max64, m.min_interval_ns) &&
        string_key(j, "delivery", 6, delivery) && (delivery == "every" || delivery == "latest") &&
        bool_key(j, "gpu", m.gpu) && string_key(j, "label", k_max_label, label) &&
        string_key(j, "address", 2 * k_max_address, address) &&
        unhex(address, m.address, m.address_bytes) && m.address_bytes;
    if(!ok)
        return Malformed;
    m.depth = std::uint32_t(depth);
    m.batch = std::uint32_t(batch);
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
    return dump({{"interface", interface(k_open)},
                 {"session", m.session},
                 {"incarnation", m.incarnation},
                 {"depth", m.depth},
                 {"batch", m.batch},
                 {"min_interval_ns", m.min_interval_ns},
                 {"delivery", m.every ? "every" : "latest"},
                 {"gpu", m.gpu},
                 {"label", std::string(m.label.data(), m.label_bytes)},
                 {"address", hex({m.address.data(), m.address_bytes})}});
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
    return dump(j);
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
    std::string_view name, status, address;
    if(check_interface(j, name) != Ok || name != k_reply || !string_key(j, "status", 32, status))
        return false;
    r = {};
    auto it = std::find(k_status_names.begin(), k_status_names.end(), status);
    if(it == k_status_names.end())
        return false;
    r.status = Status(it - k_status_names.begin());
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
    auto n = 40 + std::size_t(m.record_bytes);
    if(m.record_bytes > m.record.size() || b.size() < n)
        return 0;
    put64(b.data(), m.session);
    put64(b.data() + 8, m.sequence);
    put64(b.data() + 16, m.payload_bytes);
    put64(b.data() + 24, m.index);
    put64(b.data() + 32, m.timestamp_ns);
    std::memcpy(b.data() + 40, m.record.data(), m.record_bytes);
    return n;
}
bool decode(std::span<const std::byte> b, std::size_t record_bytes, FrameMessage &m)
{
    if(record_bytes > m.record.size() || b.size() != 40 + record_bytes)
        return false;
    m.session = get64(b.data());
    m.sequence = get64(b.data() + 8);
    m.payload_bytes = get64(b.data() + 16);
    m.index = get64(b.data() + 24);
    m.timestamp_ns = get64(b.data() + 32);
    m.record_bytes = std::uint16_t(record_bytes);
    std::memcpy(m.record.data(), b.data() + 40, record_bytes);
    return true;
}
std::array<std::byte, 16> encode(const CreditMessage &m)
{
    std::array<std::byte, 16> b{};
    put64(b.data(), m.session);
    put64(b.data() + 8, m.credit);
    return b;
}
bool decode(std::span<const std::byte> b, CreditMessage &m)
{
    if(b.size() != 16)
        return false;
    m = {get64(b.data()), get64(b.data() + 8)};
    return true;
}
std::array<std::byte, 32> encode(const EndMessage &m)
{
    std::array<std::byte, 32> b{};
    put64(b.data(), m.session);
    put64(b.data() + 8, m.sequences);
    put64(b.data() + 16, m.frames);
    b[24] = std::byte(m.reason);
    return b;
}
bool decode(std::span<const std::byte> b, EndMessage &m)
{
    if(b.size() != 32 || std::to_integer<unsigned>(b[24]) > Closing)
        return false;
    for(int i = 25; i < 32; ++i)
        if(b[i] != std::byte{})
            return false;
    m = {get64(b.data()), get64(b.data() + 8), get64(b.data() + 16),
         Reason(std::to_integer<unsigned>(b[24]))};
    return true;
}
} // namespace TangoBulk::wire
