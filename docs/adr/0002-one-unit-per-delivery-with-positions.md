<!--
SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-ucx project

SPDX-License-Identifier: LGPL-3.0-or-later
-->

# One unit per delivery, with room as positions

Status: **accepted**

## Context

Every, latest and pull sessions can share one publisher instance. Before this decision the
three deliveries shared one set of counters (`ready_mask`, `every_mask`, `pull_mask`,
per-session `reserved`, the pull ranges) and were written out together inside `grant_pass`,
`route_pass` and `unissue_grants`. Changing one meant reading all three. Overlapping
instances, every Nth frame and partial subscriptions would all have edited the same two
functions.

The deliveries meet in two places only. **Admission:** a slot is granted only if every every
session has room and the puller of that frame has room; latest never blocks. **Dispatch:** a
frame goes to all every sessions, one puller, and the latest sessions that have room and are
due. Admission is an AND and dispatch a union.

## Decision

**Each delivery is one unit of the instance:** `EverySessions`, `LatestSessions` and
`PullSet`. Each answers for its own sessions: which of them belong to it, whether they have
room for a grant, which of them get a frame, the session the device waits for, and what End
reports. Units never call each other.

**The rules that combine the units are the instance's, and live in one place.** Admission asks
every first, then pull: the pull set assigns a range when it admits a grant, and must not do so
for a grant every refuses. Dispatch sends each frame to the union. Every sessions and pullers
are the sessions that hold the device; pressure and eviction name the first of them that
refuses.

**Room is a position, not a counter that goes up and down.** A session's sent and credited
sequences, and the instance's grants issued and slots routed, only increase. The grants not
yet routed are the grant window, and each reservation is derived from it, never stored. An
every session joins before the first grant and reserves one frame per grant, so its
reservation is the window's length. A puller's is the number of the window's frames that fall
in its ranges. Room is an inequality between the session's ring and its reservation.

Deriving removes stale counters, not ordering errors. A step taken in the wrong order still
gives a wrong decision, but it can no longer leave a second ledger out of step.

**The set is closed:** three concrete types held by the instance, not a virtual interface.
Dispatch runs per frame, and a new delivery is a design decision, not a plug-in.

**The pull set keeps its ranges on the frame index,** and with them the shift when a cancelled
slot uses no index. Keying ranges on grant position would remove the shift but deliver a short
range around a cancelled slot, against "a range is whole batches". Revisit with evidence.

**Sessions keep transport state and their own delivery parameters:** endpoint, sequences, ring
bytes and sends in flight; the delivery they opened with, which Health and End still report
after the session has left its unit; latest's minimum interval and last send; and later their
selection. Membership belongs to the units.

What later steps add: an arena region with room is one more admission input (overlapping
instances); every Nth frame is a dispatch rule; a selection belongs to the session. The start
fence counts the ready every sessions (`ucx_start(every, deadline)`); membership replaces the
count after 0.1.

## Consequences

- **Each delivery reads alone.** Only the instance's delivery rules
  (`src/transport/delivery.cpp`) read more than one unit; the rest of the instance asks one unit
  at a time.
- **No reservation is stored.** The old per-session `reserved` counter and its release and
  take-back paths are gone. A cancel needs no reservation bookkeeping: it shortens the window.
- **A puller's reservation is a walk** over the window's ranges each time it is asked, where
  the counter was one read. The per-frame benchmark found the every path no slower; the pull
  path has no benchmark yet, since the perftest opens only every sessions.
- **Behaviour and the wire did not change.** The change landed one unit at a time, every,
  latest, then pull, behind a test of the delivery contract over random scenarios, through the
  public API (`tests/transport/test_delivery.cpp`). This was planned as a test against a small
  reference model. A contract test was chosen instead, so that it survives a change in how the
  publisher keeps its room.

## Considered

- The LMAX Disruptor: gating sequences, event handlers and a worker pool. Adopted as the model:
  every gates like an event handler, the pull set like a worker pool, latest does not gate. Not
  its machinery: no wait strategies, no shared ring; our consumers are remote, and a position
  is known as fast as credits arrive.
- Aeron's flow-control strategies alone: they compute how far the sender may go, which is
  admission only. Dispatch would stay tangled, and pull has no counterpart.
- Akka Streams' Broadcast, Balance and overflow strategies: the vocabulary for explaining this
  ("a Broadcast with backpressure, a Broadcast that drops, a Balance"), not a runtime to adopt.
- A virtual interface open to new deliveries: easier to extend, but it puts a virtual call in
  dispatch and invites deliveries the glossary has not decided.
- A `Delivery` object owning the grant window and every unit's reservations. Two stateful
  objects, it and the instance, would need a protocol to stay in step; deriving from the window
  needs none.
- A stored reservation per puller beside the derived one. It was kept for one commit, with
  assertions that the two agreed at every read, and then removed.
