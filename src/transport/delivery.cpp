// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-ucx project
// SPDX-License-Identifier: LGPL-3.0-or-later
// The deliveries of an instance (ADR 0002): which session gets which frame, the room each
// holds for grants, and the rules that combine every, latest and pull. Only these functions
// read more than one delivery unit.
#include "publisher.h"

#include <algorithm>
#include <cassert>

namespace TangoUcx::transport
{
// The sessions that hold the device: every sessions and pullers that have not begun to retire.
// The device waits for their room, and UcxFailedEvery counts those that fail.
RowSet Pub::device_holders() const
{
    return every.members() | pull.members();
}
// A ready session joins the unit of its delivery.
void Pub::join_delivery(std::uint32_t row_index, const wire::OpenMessage &open_message)
{
    switch(open_message.delivery)
    {
        case wire::Delivery::Every:
            every.add(row_index);
            break;
        case wire::Delivery::Pull:
            pull.add(row_index, open_message.range);
            break;
        case wire::Delivery::Latest:
            latest.add(row_index);
            break;
    }
}
// A session that leaves gives up its place in its delivery. True if it held the device.
bool Pub::leave_delivery(const PubRow &row)
{
    bool held_device = device_holders().has(row.row);
    every.remove(row.row);
    latest.remove(row.row);
    pull.remove(row, phase.was_started());
    return held_device;
}
// Who a published frame goes to: the every sessions, the latest sessions that take it now, and
// the puller of its range. The other latest sessions skip it. Asked before the slot counts as
// routed, while the grants not yet routed still hold its frame index. Every sessions and the
// puller had room reserved when the slot was granted.
Recipients Pub::recipients(const SourceSlot &slot, std::int64_t now)
{
    auto *to_puller = pull.route(*this);
    auto takers = latest.takers(*this, slot, now);
    Recipients to{every.members() | takers, latest.members().without(takers)};
    if(to_puller && to_puller->live())
        to.send.add(to_puller->row);
    return to;
}
// Whether a row holds the pressure the device waits under: every every session without room, and
// of a set only the puller the device waits for. A puller that is passed over holds none.
bool Pub::blocks(std::uint32_t row_index, std::optional<std::uint32_t> pressure) const
{
    if(!pressure)
        return false;
    if(every.has(row_index))
        return !has_room(rows[row_index]);
    return pull.has(row_index) && *pressure == row_index;
}
// Room for the next grant, or the row the device waits for. The every sessions gate first, and
// the pull set is asked only if they have room, because it may give a range to a puller as it
// answers. Latest sessions never gate.
std::optional<std::uint32_t> Pub::reserve_grant()
{
    if(auto every_blocked = every.blocking(*this))
        return every_blocked;
    return pull.reserve(*this);
}
// Room for one more grant. An every session has reserved room for every grant not yet routed, a
// puller for those in the ranges it holds.
bool Pub::has_room(const PubRow &row) const
{
    auto reserved = every.has(row.row) ? outstanding() : pull.reserved(*this, row.row);
    return row.ring.has_room(reserved, plan.sizes.frame_bytes);
}
RowSet LatestSessions::takers(const Pub &pub, const SourceSlot &slot, std::int64_t now) const
{
    RowSet taken;
    for(auto row_index : members_)
    {
        const auto &row = pub.rows[row_index];
        if(row.in_flight == 0 && now - row.last_send_ns >= row.min_interval_ns &&
           row.ring.fits_now(slot.publication.payload_bytes))
            taken.add(row_index);
    }
    return taken;
}
std::optional<std::uint32_t> EverySessions::blocking(const Pub &pub) const
{
    for(auto row_index : members_)
        if(!pub.has_room(pub.rows[row_index]))
            return row_index;
    return {};
}
// The index says which range, and the table which puller: after a pass no rule gives it. None
// for a stream with no pull set, a range not yet taken, or one whose puller has left.
PubRow *PullSet::holder(Pub &pub, std::uint64_t frame_index) const
{
    if(!range_frames_ || untaken(frame_index))
        return nullptr;
    auto row_index = range_pullers_[entry(frame_index)];
    return has(row_index) ? &pub.rows[row_index] : nullptr;
}
// The frames of the range of a frame index that are granted and not yet routed.
std::uint64_t PullSet::granted_in_range(const Pub &pub, std::uint64_t frame_index) const
{
    auto range_start = frame_index - frame_index % range_frames_;
    auto first = std::max(pub.next_frame_index, range_start);
    auto last = std::min(pub.next_frame_index + pub.outstanding(), range_start + range_frames_);
    return last > first ? last - first : 0;
}
// Whether a puller has room to take a range: rows for all of an untaken range, or for the grants
// of a range left by its puller, and in a packed ring bytes for those grants. At least one of
// each, for the grant being made.
bool PullSet::can_take_range(const Pub &pub, std::uint32_t row, bool untaken,
                             std::uint64_t granted) const
{
    const auto &ring = pub.rows[row].ring;
    auto held = reserved(pub, row);
    auto grants = std::max<std::uint64_t>(granted, 1);
    return ring.rows_free(held, untaken ? range_frames_ : grants) &&
           ring.bytes_free(held, grants, pub.plan.sizes.frame_bytes);
}
// A range goes to the next puller after the last taker, in the fixed order of the rows, that can
// take it. The range's grants then count against that puller, and those passed over count a
// pass. With no puller able to take it, nothing; with no puller left, an untaken range is
// nobody's, and its frames are missing.
PubRow *PullSet::take_range(Pub &pub, std::uint64_t frame_index)
{
    if(!range_frames_)
        return nullptr;
    assert(!holder(pub, frame_index));
    bool fresh = untaken(frame_index);
    auto granted = granted_in_range(pub, frame_index);
    assert(!fresh || granted == 0); // a grant is made only into a taken range
    auto &rows = pub.rows;
    RowSet passed_over;
    for(std::uint32_t step = 1; step <= rows.size(); ++step)
    {
        auto row_index = (last_taker_ + step) % rows.size();
        if(!has(row_index))
            continue;
        if(!can_take_range(pub, row_index, fresh, granted))
        {
            passed_over.add(row_index);
            continue;
        }
        for(auto passed : passed_over)
            ++rows[passed].passes;
        ranges_taken_ += fresh;
        last_taker_ = std::uint8_t(row_index);
        range_pullers_[entry(frame_index)] = last_taker_;
        return &rows[row_index];
    }
    if(fresh && members_.empty())
    {
        range_pullers_[entry(frame_index)] = last_taker_;
        ++ranges_taken_;
    }
    return nullptr;
}
// A puller's reservation is not stored: the grants not yet routed hold the frame indices from
// next_frame_index on, and the range table says whose range each is in. A grant routed, taken
// back or cancelled leaves that window, and a puller that leaves holds no range, so nothing has
// to be released. The order of the calls still matters: a range taken, or a frame routed, at the
// wrong position is a wrong decision, but no count is left behind that disagrees with the grants.
std::uint64_t PullSet::reserved(const Pub &pub, std::uint32_t row) const
{
    if(!range_frames_ || !has(row))
        return 0;
    auto first = pub.next_frame_index;
    auto end = first + pub.outstanding();
    std::uint64_t held = 0;
    for(auto start = first - first % range_frames_; start < end; start += range_frames_)
        if(start / range_frames_ < ranges_taken_ && range_pullers_[entry(start)] == row)
            held += granted_in_range(pub, start);
    return held;
}
// The puller of the next grant's frame index, which is the next one if no slot ahead of it is
// cancelled, has room for the grant, and nothing is returned. Without room the device waits
// for that puller or, when none has room for the range, for the one longest without a frame, and
// that row is returned. With no puller left it does not wait: nothing.
std::optional<std::uint32_t> PullSet::reserve(Pub &pub)
{
    auto frame_index = pub.next_frame_index + pub.outstanding();
    auto *taker = holder(pub, frame_index);
    if(!taker)
        taker = take_range(pub, frame_index);
    if(taker && pub.has_room(*taker))
        return {};
    if(members_.empty())
        return {};
    auto *slow = taker;
    if(!slow)
        for(auto row_index : members_)
            if(!slow || pub.rows[row_index].last_send_ns < slow->last_send_ns)
                slow = &pub.rows[row_index];
    return slow->row;
}
// The puller the next routed slot goes to. A cancelled slot is not routed here: it uses no frame
// index, so every later grant moves down by one, and the newest granted index leaves the window
// of whoever held it. A frame whose range's puller is
// gone, when no other puller can take the range's grants, goes to any puller with room for one
// more frame. If none has room, it is missing.
// TODO(pull-loss): a puller should never lose frames while the set is healthy, as every does
// not. Two edges lose them today, both when a puller leaves:
//   1. grants it had reserved, when no other puller has room: wait (pressure) instead of
//      counting them missing, and only miss them when no puller is left;
//   2. frames in its ring it never released. Options: keep failing the set, as an every
//      subscriber's death fails it (today); let a puller that closes cleanly receive what it
//      holds before it goes, so only a crash loses frames; or redeliver, at least once, keeping
//      source slots until released (duplicates, non-consecutive ranges, earlier pressure).
// Not decided; update CONTEXT.md's Missing with the choice.
PubRow *PullSet::route(Pub &pub)
{
    auto *to_puller = holder(pub, pub.next_frame_index);
    if(!to_puller)
        to_puller = take_range(pub, pub.next_frame_index);
    if(to_puller)
        return to_puller;
    if(!range_frames_)
        return nullptr;
    for(auto row_index : members_)
        if(pub.has_room(pub.rows[row_index]))
            return &pub.rows[row_index];
    ++missing_;
    return nullptr;
}
// A puller's End reports the set's missing count, so while one puller is sending, the End of
// every puller waits.
RowSet PullSet::end_waits(const Pub &pub) const
{
    for(auto row_index : members_)
        if(pub.rows[row_index].in_flight)
            return members_;
    return {};
}
// Sized for the grants outstanding, which span fewer ranges than the slots plus two.
void PullSet::setup(std::uint32_t slot_count)
{
    range_pullers_.assign(slot_count + 2, 0);
}
// Why a puller's Open is refused, or Ok.
wire::Status PullSet::check_open(const wire::OpenMessage &open_message) const
{
    if(open_message.range % open_message.batch)
        return wire::Range; // a range must be whole batches
    if(open_message.range > open_message.depth)
        return wire::Range; // a range must fit the ring
    if(!members_.empty() && open_message.range != range_frames_)
        return wire::Range; // every puller of a set has the same range
    return wire::Ok;
}
// A puller left before it was sent End. Frames it received but did not release are counted as
// missing, because a frame is never sent twice, and the ranges it held are nobody's. With no
// pullers left before the start, the range is cleared: the stream then has no pull set.
void PullSet::remove(const PubRow &row, bool started)
{
    if(members_.has(row.row) && row.end != EndNotice::Sent)
        missing_ += row.ring.unreleased();
    members_.remove(row.row);
    if(members_.empty() && !started)
        range_frames_ = 0;
}
// Every and latest sessions are all ready sessions.
void Pub::check_members() const
{
    assert((every.members() | latest.members()).without(ready_rows).empty());
}
} // namespace TangoUcx::transport
