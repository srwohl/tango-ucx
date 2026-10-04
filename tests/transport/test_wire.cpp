// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-ucx project
// SPDX-License-Identifier: LGPL-3.0-or-later
#include <catch2/catch_test_macros.hpp>
#include <transport.h>
#include <wire.h>

#include <nlohmann/json.hpp>

#include <algorithm>
#include <cstring>
#include <functional>
#include <utility>
#include <vector>

using namespace TangoUcx;

namespace
{
using Bytes = std::vector<std::byte>;
using Round = std::function<std::optional<Bytes>(std::span<const std::byte>)>;

wire::OpenMessage golden_open()
{
    wire::OpenMessage m{};
    m.session = 7340032917504811;
    m.instance_id = 12648430;
    m.depth = 256;
    m.batch = 16;
    m.every = true;
    m.label_bytes = 8;
    std::memcpy(m.label.data(), "archiver", 8);
    m.address_bytes = 3;
    m.address[0] = std::byte{0x0a};
    m.address[1] = std::byte{0x3f};
    m.address[2] = std::byte{0xff};
    return m;
}
std::string replace(std::string s, std::string_view from, std::string_view to)
{
    auto at = s.find(from);
    REQUIRE(at != s.npos);
    return s.replace(at, from.size(), to);
}
std::string without(std::string s, std::string_view key)
{
    auto at = s.find(",\"" + std::string(key) + "\":");
    REQUIRE(at != s.npos);
    return s.erase(at, s.find_first_of(",}", at + 1) - at);
}
wire::Status status_of(const std::string &doc)
{
    wire::Request r;
    return wire::parse(doc, r);
}
Bytes u64s(std::initializer_list<std::uint64_t> values)
{
    Bytes b(8 * values.size());
    auto *p = b.data();
    for(auto v : values)
        wire::put64(std::exchange(p, p + 8), v);
    return b;
}
template <std::size_t N> Bytes bytes(const std::array<std::byte, N> &a)
{
    return {a.begin(), a.end()};
}
// Only the exact length decodes, and a flipped bit is either refused or encodes back unchanged.
void sweep(const Bytes &good, const Round &round)
{
    auto longer = good;
    longer.push_back(std::byte{});
    for(std::size_t n = 0; n <= longer.size(); ++n)
        CHECK(round({longer.data(), n}).has_value() == (n == good.size()));
    for(std::size_t bit = 0; bit < 8 * good.size(); ++bit)
    {
        auto flipped = good;
        flipped[bit / 8] ^= std::byte(1u << bit % 8);
        auto back = round(flipped);
        CHECK((!back || *back == flipped));
    }
}
} // namespace

TEST_CASE("control documents match their golden JSON and round-trip")
{
    auto open = wire::format(golden_open());
    CHECK(open == R"({"interface":"tango-ucx-open/3.2","session":7340032917504811,)"
                  R"("instance_id":12648430,"depth":256,"batch":16,"min_interval_ns":0,)"
                  R"("delivery":"every","gpu":false,"label":"archiver","address":"0a3fff"})");
    wire::Request r;
    REQUIRE(wire::parse(open, r) == wire::Ok);
    REQUIRE(r.open);
    CHECK(wire::format(*r.open) == open);

    auto close = wire::format(wire::CloseMessage{42});
    CHECK(close == R"({"interface":"tango-ucx-close/3.2","session":42})");
    REQUIRE(wire::parse(close, r) == wire::Ok);
    CHECK_FALSE(r.open);
    CHECK(r.close.session == 42);

    wire::Reply accepted{wire::Ok};
    accepted.address_bytes = 2;
    accepted.address[0] = std::byte{0x7c};
    accepted.address[1] = std::byte{0x01};
    CHECK(wire::format(accepted) ==
          R"({"interface":"tango-ucx-reply/3.2","status":"ok","address":"7c01"})");
    CHECK(wire::format(wire::Reply{wire::EveryRefused}) ==
          R"({"interface":"tango-ucx-reply/3.2","status":"every-refused"})");
    wire::Reply back;
    REQUIRE(wire::parse(wire::format(accepted), back));
    CHECK(back.status == wire::Ok);
    CHECK(back.address_bytes == 2);
    CHECK(back.address[0] == std::byte{0x7c});
    for(int s = wire::Ok; s <= wire::Range; ++s)
    {
        REQUIRE(wire::parse(wire::format(wire::Reply{wire::Status(s)}), back));
        CHECK(back.status == s);
    }
}

TEST_CASE("readers accept a newer minor and unknown keys, and refuse another major")
{
    auto open = wire::format(golden_open());
    CHECK(status_of(replace(open, "open/3.2", "open/3.7")) == wire::Ok);
    CHECK(status_of(replace(open, "open/3.2", "open/3.0")) == wire::Ok);
    CHECK(status_of(replace(open, R"("gpu":false)", R"("gpu":false,"later":{"x":[1,2]})")) ==
          wire::Ok);
    CHECK(status_of(replace(open, "open/3.2", "open/4.0")) == wire::Version);
    CHECK(status_of(replace(open, "open/3.2", "open/2.9")) == wire::Version);
    wire::Reply reply;
    CHECK_FALSE(wire::parse(R"({"interface":"tango-ucx-reply/4.0","status":"ok"})", reply));
    REQUIRE(wire::parse(R"({"interface":"tango-ucx-reply/3.9","status":"busy","hint":1})", reply));
    CHECK(reply.status == wire::Busy);
}

TEST_CASE("malformed control documents are refused")
{
    auto open = wire::format(golden_open());
    for(auto key : {"session", "instance_id", "depth", "batch", "min_interval_ns", "delivery",
                    "gpu", "label", "address"})
        CHECK(status_of(without(open, key)) == wire::Malformed);
    for(const auto &bad : {std::string("{"),
                           std::string("[]"),
                           std::string("null"),
                           std::string(R"({"session":1})"),
                           replace(open, "open/3.2", "open/3"),
                           replace(open, "open/3.2", "open/x.0"),
                           replace(open, "tango-ucx-open", "tango-ucx-other"),
                           replace(open, "7340032917504811", "0"),
                           replace(open, "7340032917504811", "-1"),
                           replace(open, "7340032917504811", "1.5"),
                           replace(open, "7340032917504811", "\"1\""),
                           replace(open, R"("depth":256)", R"("depth":0)"),
                           replace(open, R"("depth":256)", R"("depth":65537)"),
                           replace(open, R"("batch":16)", R"("batch":257)"),
                           replace(open, R"("delivery":"every")", R"("delivery":"all")"),
                           replace(open, R"("gpu":false)", R"("gpu":0)"),
                           replace(open, "archiver", std::string(65, 'x')),
                           replace(open, "0a3fff", "0a3ff"),
                           replace(open, "0a3fff", "0a3fzz"),
                           replace(open, "0a3fff", ""),
                           replace(open, "0a3fff", std::string(8194, 'a')),
                           open + std::string(wire::k_max_document, ' '),
                           wire::format(wire::CloseMessage{0})})
        CHECK(status_of(bad) == wire::Malformed);
    wire::Reply reply;
    CHECK_FALSE(wire::parse(R"({"interface":"tango-ucx-reply/3.2","status":"fine"})", reply));
    CHECK_FALSE(
        wire::parse(R"({"interface":"tango-ucx-reply/3.2","status":"ok","address":"7"})", reply));
    CHECK_FALSE(wire::parse(R"({"interface":"tango-ucx-open/3.2","status":"ok"})", reply));
}

TEST_CASE("the UcxStream document matches its golden JSON and refuses what it cannot describe")
{
    wire::StreamMessage m{12648430, {Element::U16, {2048, 1024}, 16, "[('t','<f8')]"}, true, 8};
    auto doc = wire::format(m);
    CHECK(doc == R"({"interface":"tango-ucx-stream/3.2","instance_id":12648430,"element":"u16",)"
                 R"("shape":[2048,1024],"field_bytes":16,"field_dtype":"[('t','<f8')]",)"
                 R"("allow_every":true,"max_sessions":8})");
    auto offer = transport::parse_ucx_stream(replace(doc, "3.2", "3.3"));
    CHECK(offer.instance_id == 12648430);
    CHECK(offer.description.element == Element::U16);
    CHECK(offer.description.shape == std::vector<std::uint64_t>{2048, 1024});
    CHECK(offer.description.field_bytes == 16);
    CHECK(offer.description.field_dtype == "[('t','<f8')]");
    CHECK((offer.limits.allow_every && offer.limits.max_sessions == 8));
    auto status = [](const std::string &d)
    {
        wire::StreamMessage back;
        return wire::parse(d, back);
    };
    CHECK(status(replace(doc, "3.2", "4.0")) == wire::Version);
    CHECK_THROWS(transport::parse_ucx_stream(replace(doc, "3.2", "4.0")));
    for(auto key :
        {"instance_id", "element", "field_bytes", "field_dtype", "allow_every", "max_sessions"})
        CHECK(status(without(doc, key)) == wire::Malformed);
    for(auto [from, to] : std::initializer_list<std::pair<const char *, const char *>>{
            {"u16", "u12"},
            {R"("shape":[2048,1024],)", ""},
            {"[2048,1024]", "[]"},
            {"[2048,1024]", "[2048,0]"},
            {"[2048,1024]", "[1,1,1,1,1,1,1,1,1]"},
            {"[2048,1024]", "2048"},
            {R"("field_bytes":16)", R"("field_bytes":1025)"},
            {R"("max_sessions":8)", R"("max_sessions":0)"},
            {R"("max_sessions":8)", R"("max_sessions":65)"},
            {"tango-ucx-stream", "tango-ucx-open"}})
        CHECK(status(replace(doc, from, to)) == wire::Malformed);
}

// A 3.0 subscriber needs a shape, so it refuses an opaque stream before it sends an Open.
TEST_CASE("an opaque stream's UcxStream has a maximum and no shape, and its Open asks for a ring")
{
    wire::StreamMessage m{12648430, {Element::Bytes, {}, 8, "<u8", 9ull << 20}, true, 8};
    auto doc = wire::format(m);
    CHECK(doc == R"({"interface":"tango-ucx-stream/3.2","instance_id":12648430,"element":"bytes",)"
                 R"("max_payload_bytes":9437184,"field_bytes":8,"field_dtype":"<u8",)"
                 R"("allow_every":true,"max_sessions":8})");
    auto offer = transport::parse_ucx_stream(doc);
    CHECK((offer.description.max_payload_bytes == 9ull << 20 && offer.description.shape.empty()));
    wire::StreamMessage back;
    for(auto [from, to] : std::initializer_list<std::pair<const char *, const char *>>{
            {"9437184", "0"},
            {"9437184", "2147483649"},
            {"9437184", "[9437184]"},
            {R"("field_bytes")", R"("shape":[4],"field_bytes")"}})
        CHECK(wire::parse(replace(doc, from, to), back) == wire::Malformed);
    auto open = golden_open();
    open.ring_bytes = 264241152;
    auto text = wire::format(open);
    CHECK(text.ends_with(R"("address":"0a3fff","ring_bytes":264241152})"));
    wire::Request r;
    REQUIRE(wire::parse(text, r) == wire::Ok);
    CHECK(r.open->ring_bytes == 264241152);
    CHECK(status_of(replace(text, "264241152", "0")) == wire::Malformed);
    CHECK(status_of(replace(text, "264241152", "\"1\"")) == wire::Malformed);
}

TEST_CASE("a puller's Open states its range, and its delivery is pull")
{
    auto open = golden_open();
    open.every = false;
    open.range = 64;
    auto text = wire::format(open);
    CHECK(text.find(R"("delivery":"pull")") != text.npos);
    CHECK(text.ends_with(R"("address":"0a3fff","range":64})"));
    wire::Request r;
    REQUIRE(wire::parse(text, r) == wire::Ok);
    CHECK((r.open->range == 64 && !r.open->every && wire::format(*r.open) == text));
    for(auto bad : {R"("range":0)", R"("range":"64")", R"("range":4294967296)"})
        CHECK(status_of(replace(text, R"("range":64)", bad)) == wire::Malformed);
    CHECK(status_of(without(text, "range")) == wire::Malformed);
    // A range that is no multiple of the batch, or more than the ring holds, parses: the
    // publisher answers it with the status `range`.
    CHECK(status_of(replace(text, R"("range":64)", R"("range":300)")) == wire::Ok);
    CHECK(wire::format(wire::Reply{wire::Range}).find(R"("status":"range")") != std::string::npos);
}

TEST_CASE("the UcxSessions document matches its golden JSON and is never capped")
{
    wire::SessionsMessage m{12648430, {}};
    CHECK(wire::format(m) ==
          R"({"interface":"tango-ucx-sessions/3.2","instance_id":12648430,"missing":0,)"
          R"("sessions":[]})");
    m.sessions = {{1, "archiver", true, false, 256, 250, 6, 0, 12, "sysv/memory"},
                  {2, "a\"b", false, true, 8, 8, 0, 3, 0, ""},
                  {3, "writer", false, false, 64, 8, 56, 0, 0, "", true, 2}}; // a puller
    m.missing = 5;
    CHECK(wire::format(m) ==
          R"({"interface":"tango-ucx-sessions/3.2","instance_id":12648430,"missing":5,"sessions":[)"
          R"({"ordinal":1,"label":"archiver","delivery":"every","state":"ready","depth":256,)"
          R"("room":250,"sent":6,"skipped":0,"transfer_max_us":12,"transport":"sysv/memory",)"
          R"("passes":0},)"
          R"({"ordinal":2,"label":"a\"b","delivery":"latest","state":"retiring","depth":8,)"
          R"("room":8,"sent":0,"skipped":3,"transfer_max_us":0,"transport":"","passes":0},)"
          R"({"ordinal":3,"label":"writer","delivery":"pull","state":"ready","depth":64,)"
          R"("room":8,"sent":56,"skipped":0,"transfer_max_us":0,"transport":"","passes":2}]})");
    // The most a publisher can list is more than the 16 KiB a reader of a command accepts.
    m.sessions.assign(64, {64, std::string(64, 'l'), true, true, 65536, 65536, ~0ull, ~0ull, ~0ull,
                           std::string(64, 't')});
    auto doc = wire::format(m);
    CHECK(doc.size() > wire::k_max_document);
    auto back = nlohmann::json::parse(doc);
    REQUIRE(back["sessions"].size() == 64);
    CHECK(back["sessions"][63]["label"] == std::string(64, 'l'));
}

TEST_CASE("a UCX command marker names its major version")
{
    CHECK(transport::marker_major(wire::k_open_marker) == wire::k_major);
    CHECK(transport::marker_major(wire::k_close_marker) == wire::k_major);
    CHECK(transport::marker_major("tango-ucx/12 open") == 12);
    for(auto other : {"tango-ucx/3 renew", "tango-ucx/x open", "tango-ucx/3", "tango-ucx/ open",
                      "tango-ucx/300 open", "DevString", ""})
        CHECK(transport::marker_major(other) == -1);
}

TEST_CASE("data-plane structs match their golden bytes")
{
    CHECK(bytes(wire::encode(wire::SessionMessage{0x0102030405060708})) ==
          Bytes{std::byte{8}, std::byte{7}, std::byte{6}, std::byte{5}, std::byte{4}, std::byte{3},
                std::byte{2}, std::byte{1}});
    CHECK(bytes(wire::encode(wire::CreditMessage{1, 0x200, 0x300})) == u64s({1, 0x200, 0x300}));
    auto end = u64s({1, 2, 3, 0});
    end[24] = std::byte{wire::Evicted};
    CHECK(bytes(wire::encode(wire::EndMessage{1, 2, 3, wire::Evicted})) == end);
    wire::EndMessage lost; // a puller's End: what the set lost, after the reason
    REQUIRE(wire::decode(u64s({1, 2, 3, 7 << 8 | wire::Closing}), lost));
    CHECK((lost.reason == wire::Closing && lost.missing == 7));
    wire::FrameMessage f{1, 2, 3, 4, 5};
    f.record_bytes = 8;
    std::fill_n(f.record.begin(), 8, std::byte{0x21});
    auto frame = u64s({1, 2, 3, 4, 5, 0x2121212121212121});
    Bytes out(48);
    REQUIRE(wire::encode_into(out, f) == 48);
    CHECK(out == frame);
    CHECK(wire::encode_into({out.data(), 47}, f) == 0);
    f.packed = true; // a packed ring's frame: its position, before the record
    f.position = 6;
    Bytes packed(56);
    REQUIRE(wire::encode_into(packed, f) == 56);
    CHECK(packed == u64s({1, 2, 3, 4, 5, 6, 0x2121212121212121}));
    CHECK(wire::encode_into({packed.data(), 55}, f) == 0);
    wire::FrameMessage back;
    REQUIRE(wire::decode(packed, 8, back));
    CHECK((back.packed && back.position == 6 && back.record[7] == std::byte{0x21}));
    REQUIRE(wire::decode(frame, 8, back));
    CHECK((!back.packed && back.position == 0));
}

TEST_CASE("data-plane structs refuse every other length and re-encode every accepted bit flip")
{
    sweep(bytes(wire::encode(wire::SessionMessage{0x0102030405060708})),
          [](std::span<const std::byte> b) -> std::optional<Bytes>
          {
              wire::SessionMessage m;
              return wire::decode(b, m) ? std::optional(bytes(wire::encode(m))) : std::nullopt;
          });
    sweep(u64s({7, 9}),
          [](std::span<const std::byte> b) -> std::optional<Bytes>
          {
              wire::CreditMessage m;
              if(!wire::decode(b, m))
                  return std::nullopt;
              auto out = bytes(wire::encode(m));
              out.resize(b.size());
              return out;
          });
    wire::CreditMessage ring; // a packed ring's Credit has 8 bytes more, and no other length
    auto credit = u64s({7, 9, 11, 13});
    for(std::size_t n = 17; n <= credit.size(); ++n)
        CHECK(wire::decode({credit.data(), n}, ring) == (n == 24));
    CHECK((ring.credit == 9 && ring.bytes == 11));
    sweep(bytes(wire::encode(wire::EndMessage{7, 9, 11, wire::Protocol})),
          [](std::span<const std::byte> b) -> std::optional<Bytes>
          {
              wire::EndMessage m;
              return wire::decode(b, m) ? std::optional(bytes(wire::encode(m))) : std::nullopt;
          });
    sweep(u64s({1, 2, 3, 4, 5, 0x2121212121212121}),
          [](std::span<const std::byte> b) -> std::optional<Bytes>
          {
              wire::FrameMessage m;
              Bytes out(48);
              if(!wire::decode(b, 8, m) || wire::encode_into(out, m) != 48)
                  return std::nullopt;
              return out;
          });
    sweep(u64s({1, 2, 3, 4, 5, 6, 0x2121212121212121}),
          [](std::span<const std::byte> b) -> std::optional<Bytes>
          {
              wire::FrameMessage m;
              Bytes out(56);
              if(!wire::decode(b, 8, m) || !m.packed || wire::encode_into(out, m) != 56)
                  return std::nullopt;
              return out;
          });
}
