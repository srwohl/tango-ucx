<!--
SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-ucx project

SPDX-License-Identifier: LGPL-3.0-or-later
-->

# One unit per delivery, with room as positions

Status: **proposed**

## Context

Every, latest and pull sessions can share one publisher instance. Today the three deliveries
share one set of counters (`ready_mask`, `every_mask`, `pull_mask`, per-session `reserved`,
the pull ranges) and are written out together inside `grant_pass`, `route_pass` and
`unissue_grants`. Changing one means reading all three. Overlapping instances, every Nth frame
and partial subscriptions would all edit the same two functions.

The deliveries meet in two places only. **Admission:** a slot is granted only if every every
session has room and the puller of that frame has room; latest never blocks. **Dispatch:** a
frame goes to all every sessions, one puller, and the latest sessions that have room and are
due. Admission is an AND and dispatch a union.

## Decision

**Each delivery is one unit of the instance:** `EverySessions`, `PullSet` and
`LatestSessions`. Each answers for its own sessions: room for a grant, a grant made or taken
back, which of its sessions get a frame, a credit, a retired session, the session the device
waits for, and what End reports. The instance grants while a slot is free and every unit that
gates has room, and sends each frame to the union. Pressure and eviction name the first unit
that refuses and its waiting session. Units never call each other.

**Room is a position, not a counter that goes up and down.** A session's sent and credited
sequences, and the instance's grants issued and slots routed, only increase. Every session
reserves one frame per grant and joins before the first, so its reservation is the instance's
outstanding grants, one number (asserted since `d0b07ce`). Room becomes an inequality on
positions, which assertions and a model test can check.

**The set is closed:** three concrete types held by the instance, not a virtual interface.
Dispatch runs per frame, and a new delivery is a design decision, not a plug-in.

**The pull set keeps its ranges on the frame index,** and with them the shift when a cancelled
slot uses no index. Keying ranges on grant position would remove the shift but deliver a short
range around a cancelled slot, against "a range is whole batches". Revisit with evidence.

**Sessions keep transport state only:** endpoint, sequences, ring bytes, sends in flight, and
later their selection.

What later steps add: an arena region with room is one more admission input (overlapping
instances); every Nth frame is a dispatch rule; a selection belongs to the session; the start
fence counts ready every sessions by label, as Aeron's tagged flow control counts tagged
receivers.

The change lands behind a model-based test (random opens, grants, cancels, credits, retires
and finishes against a small reference model), one unit at a time: every, latest, then pull.
Behaviour and the wire do not change.

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
