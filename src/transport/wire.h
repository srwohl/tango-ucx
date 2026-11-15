// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-ucx project
// SPDX-License-Identifier: LGPL-3.0-or-later
#pragma once

#include <tango-ucx/ucx.h>

#include <array>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace TangoUcx::wire
{
// 0.1 is a pre-release, to be continued until 1.0: until then any change may break the wire, so
// a document is accepted only at exactly this version. From 1.0, a minor only adds keys and any
// minor of the same major is accepted.
inline constexpr std::uint8_t k_major = 0;
inline constexpr std::uint8_t k_minor = 1;
inline constexpr std::size_t k_max_document = 16 * 1024;
// An application text of this many bytes fits a document even when every byte is escaped.
inline constexpr std::size_t k_max_text = 2048;
inline constexpr std::size_t k_max_address = 4096;
inline constexpr std::size_t k_max_label = 64;
inline constexpr std::size_t k_max_detail = 128;
// The UCX commands' type descriptions; a client finds the commands by them.
inline constexpr const char *k_open_marker = "tango-ucx/0 open";
inline constexpr const char *k_close_marker = "tango-ucx/0 close";
// Data-plane messages; the value is the UCX active-message id.
enum Type : std::uint8_t
{
    Ready,
    Frame,
    Credit,
    End
};
enum Status : std::uint16_t
{
    Ok,
    Malformed,
    Version,
    InstanceId,
    EveryRefused,
    NoSession,
    Endpoint,
    Finished,
    UnknownSession,
    Busy,
    Timeout,
    Started,
    Range
};
// Why a session ended, in End. 0 is no reason, so a zeroed End never reads as finished. A
// value this build does not know is kept as it came, and means the session failed.
enum Reason : std::uint8_t
{
    Unspecified,
    FinishedReason,
    Evicted,
    Protocol,
    Closing
};

// Control plane: JSON documents carried by the UCX commands.
struct OpenMessage
{
    std::uint64_t session = 0, instance_id = 0;
    std::uint32_t depth = 0, batch = 0, range = 0;     // range: a puller's, the frames of a range
    std::uint64_t min_interval_ns = 0, ring_bytes = 0; // ring_bytes: an opaque stream's packed ring
    bool every = false, gpu = false;
    std::array<char, k_max_label> label{};
    std::uint8_t label_bytes = 0;
    std::array<std::byte, k_max_address> address{};
    std::uint16_t address_bytes = 0;
};
struct CloseMessage
{
    std::uint64_t session = 0;
};
struct Request
{
    std::optional<OpenMessage> open;
    CloseMessage close;
};
struct Reply
{
    Status status = Ok;
    std::array<std::byte, k_max_address> address{};
    std::uint16_t address_bytes = 0;
    // Why the request was refused, in the publisher's own words. Most replies have none.
    std::array<char, k_max_detail> detail{};
    std::uint8_t detail_bytes = 0;
};
// The UcxStream attribute: what every frame of this publisher instance is.
struct StreamMessage
{
    std::uint64_t instance_id = 0;
    Description description;
    bool allow_every = false;
    std::uint32_t max_sessions = 0;
};
// The UcxSessions attribute: the sessions of one Health sample.
struct SessionRow
{
    std::uint32_t ordinal = 0;
    std::string label;
    bool every = false, retiring = false;
    std::uint32_t depth = 0, room = 0;
    std::uint64_t sent = 0, skipped = 0, transfer_max_us = 0;
    std::string transport;
    bool pull = false;
    std::uint64_t passes = 0; // a puller's: the turns it was passed over
};
struct SessionsMessage
{
    std::uint64_t instance_id = 0;
    std::vector<SessionRow> sessions;
    std::uint64_t missing = 0; // the frames no puller of the set received
};

std::string format(const OpenMessage &);
std::string format(const CloseMessage &);
std::string format(const Reply &);
std::string format(const StreamMessage &);
std::string format(const SessionsMessage &);
std::string format(const Topology &);
Status parse(std::string_view, Request &);
bool parse(std::string_view, Reply &);
Status parse(std::string_view, StreamMessage &);
// Whether UcxStream carries a text unchanged. It must be UTF-8 and no longer than k_max_text.
bool valid_text(std::string_view);
std::string_view status_name(Status);

// Data plane: fixed little-endian structs; the active-message id is the type. A field keeps its
// offset for good, and new fields go at the end: a decoder accepts a message longer than it knows
// and ignores the rest.
struct SessionMessage
{
    std::uint64_t session = 0;
};
struct FrameMessage
{
    std::uint64_t session = 0, sequence = 0, payload_bytes = 0, index = 0, timestamp_ns = 0;
    std::array<std::byte, 1024> record{};
    std::uint16_t record_bytes = 0;
    // A packed ring's frame states where it is placed, counted from the session's first byte:
    // 8 bytes more, before the record. Its subscriber takes nothing from the order of arrival.
    bool packed = false;
    std::uint64_t position = 0;
};
// `bytes` is where the last credited frame ends in a packed ring, counted from the session's
// first byte; an array stream's Credit is the first 16 bytes.
struct CreditMessage
{
    std::uint64_t session = 0, credit = 0, bytes = 0;
};
// The reason is byte 24, and the seven bytes after it are reserved: zero when sent, ignored when
// read. `missing` is a set's, the frames no puller received: 0 in every End but a puller's.
struct EndMessage
{
    std::uint64_t session = 0, sequences = 0, frames = 0;
    Reason reason = FinishedReason;
    std::uint64_t missing = 0;
};

std::array<std::byte, 8> encode(const SessionMessage &);
bool decode(std::span<const std::byte>, SessionMessage &);
std::size_t encode_into(std::span<std::byte>, const FrameMessage &);
// packed: the stream is opaque, so its frames have their position, 8 bytes more.
bool decode(std::span<const std::byte>, std::size_t record_bytes, bool packed, FrameMessage &);
std::array<std::byte, 24> encode(const CreditMessage &);
bool decode(std::span<const std::byte>, CreditMessage &);
std::array<std::byte, 40> encode(const EndMessage &);
bool decode(std::span<const std::byte>, EndMessage &);
std::uint64_t get64(const std::byte *) noexcept;
void put64(std::byte *, std::uint64_t) noexcept;
} // namespace TangoUcx::wire
