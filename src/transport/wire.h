// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
// SPDX-License-Identifier: LGPL-3.0-or-later
#pragma once

#include <tango-bulk/bulk.h>

#include <array>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace TangoBulk::wire
{
inline constexpr std::uint8_t k_major = 3;
inline constexpr std::uint8_t k_minor = 0;
inline constexpr std::size_t k_max_document = 16 * 1024;
inline constexpr std::size_t k_max_address = 4096;
inline constexpr std::size_t k_max_label = 64;
// The bulk commands' type descriptions; a client finds the commands by them.
inline constexpr const char *k_open_marker = "tango-bulk/3 open";
inline constexpr const char *k_close_marker = "tango-bulk/3 close";
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
    Incarnation,
    EveryRefused,
    NoSession,
    Endpoint,
    Finished,
    UnknownSession,
    Busy,
    Timeout
};
enum Reason : std::uint8_t
{
    FinishedReason,
    Evicted,
    Protocol,
    Closing
};

// Control plane: JSON documents carried by the bulk commands.
struct OpenMessage
{
    std::uint64_t session = 0, incarnation = 0;
    std::uint32_t depth = 0, batch = 0;
    std::uint64_t min_interval_ns = 0;
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
};
// The BulkStream attribute: what every frame of this incarnation is.
struct StreamMessage
{
    std::uint64_t incarnation = 0;
    Description description;
    bool allow_every = false;
    std::uint32_t max_sessions = 0;
};
// The BulkSessions attribute: the sessions of one Health sample.
struct SessionRow
{
    std::uint32_t ordinal = 0;
    std::string label;
    bool every = false, retiring = false;
    std::uint32_t depth = 0, room = 0;
    std::uint64_t sent = 0, skipped = 0, transfer_max_us = 0;
    std::string transport;
};
struct SessionsMessage
{
    std::uint64_t incarnation = 0;
    std::vector<SessionRow> sessions;
};

std::string format(const OpenMessage &);
std::string format(const CloseMessage &);
std::string format(const Reply &);
std::string format(const StreamMessage &);
std::string format(const SessionsMessage &);
Status parse(std::string_view, Request &);
bool parse(std::string_view, Reply &);
Status parse(std::string_view, StreamMessage &);
std::string_view status_name(Status);

// Data plane: fixed little-endian structs; the active-message id is the type.
struct SessionMessage
{
    std::uint64_t session = 0;
};
struct FrameMessage
{
    std::uint64_t session = 0, sequence = 0, payload_bytes = 0, index = 0, timestamp_ns = 0;
    std::array<std::byte, 1024> record{};
    std::uint16_t record_bytes = 0;
};
struct CreditMessage
{
    std::uint64_t session = 0, credit = 0;
};
struct EndMessage
{
    std::uint64_t session = 0, sequences = 0, frames = 0;
    Reason reason = FinishedReason;
};

std::array<std::byte, 8> encode(const SessionMessage &);
bool decode(std::span<const std::byte>, SessionMessage &);
std::size_t encode_into(std::span<std::byte>, const FrameMessage &);
bool decode(std::span<const std::byte>, std::size_t record_bytes, FrameMessage &);
std::array<std::byte, 16> encode(const CreditMessage &);
bool decode(std::span<const std::byte>, CreditMessage &);
std::array<std::byte, 32> encode(const EndMessage &);
bool decode(std::span<const std::byte>, EndMessage &);
std::uint64_t get64(const std::byte *) noexcept;
void put64(std::byte *, std::uint64_t) noexcept;
} // namespace TangoBulk::wire
