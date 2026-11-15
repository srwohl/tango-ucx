<!--
SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-ucx project

SPDX-License-Identifier: LGPL-3.0-or-later
-->

# Keep UCX resources across publisher instances

Status: **accepted**

## Context

A device that makes a `Publisher` for each acquisition pays for a UCX context, a worker, a
progress thread and a registered source ring each time. Measured with the Basler plugin on
4096² 16-bit frames and a 512 MiB budget (the Basler + tango-ucx findings, sections 1 and 7),
a cold acquisition costs about 150 ms more than a warm one. Two thirds of that is page faults on
the device's grab thread during the first frame into each source slot (14.5–16.8 ms against
3.4 ms for later frames), not the 46 ms prepare. A real camera with ten grab buffers would
probably lose frames there. The first publisher of a process also pays about 2.2 s of UCX
start-up.

tango-bulk's cuts already settled the shape. Cut 2: a new description is a new instance, and
"keep one registered ring across subscriptions" is its remedy, to be measured first. Cut 3: the
device's own Start waits for the every sessions it needs. Cut 4: the publisher counts failed
every sessions and the device decides. A process-wide cache of idle contexts, workers and
regions proved the gain, but nothing owned the pinned memory, a slow Close or a Tango thread
still holding the old publisher made it miss silently, and it needed a `pthread_atfork` hook.

## Decision

**The long-lived object is the `Publisher`.** It lives as long as the device and owns the UCX
context, the worker, the progress thread and the arena. `Publisher(PublisherLimits)` makes one
with no instance. `begin(Description)` starts a **publisher instance**: a new instance ID, frame
index from 0, its own sessions, slots, records and grants. `ucx_start`, `acquire` and `finish`
act on the active instance, as they do now. No instance handle is public: the device only
writes to the active instance, and subscribers tell instances apart by the instance ID in
`UcxStream`. The limits are fixed for the publisher's life; `begin` takes a description only.

**`Publisher(Description, PublisherLimits)` stays,** as `Publisher(limits)` followed by
`begin(description)`. A device with one acquisition, and every existing test, keeps working
unchanged.

**One active instance; earlier instances may drain.** `begin` finishes the active instance and
waits until nothing of it reads the arena: every slot the device acquired published or
cancelled, every frame sent, End sent to every session. The instance then closes its sessions
in the background, while the new one is active, and is dropped once they are closed. Until
overlapping instances are switched on, `begin` waits for that point. Afterwards an instance's
arena region keeps its frames, and `begin` need not wait.

**The session table belongs to the instance.** Credits and Close are routed to the instance
that holds the session; Open is routed by instance ID to the active instance, and refused with
`InstanceId` otherwise. All routing goes through one lookup, so the table can move up to the
publisher if sessions ever span instances (tango-ucx-stream v4). The wire does not change.

**The arena is registered once and pre-faulted.** It holds the source rings and is registered
with the publisher's context. Its pages are faulted in when it is made, on the progress thread,
so no frame pays for them. It is made at the first `begin`, sized for that instance's ring, and
replaced only when a later description needs a larger ring (never more than the budget) or when
an instance is quarantined, because a send that never completed may still read it. Pinned
memory therefore has an owner, is bounded by the budget and is released with the publisher.
GPU source rings stay per instance for now.

**The progress thread blocks when idle.** With no instance that has work, it waits on the
worker's events and on a wake that control calls, `begin`, `ucx_start`, `finish` and published
slots signal.

**The Tango glue attaches the publisher once.** `UcxStream`, `UcxSessions` and the Health
attributes read the active instance, so a Tango thread never holds an instance alive.

## Considered

- A publisher per acquisition, pre-faulted at setup: removes the grab-thread faults, but every
  prepare then costs about 200 ms, and it leaves the subscriber side and overlap unsolved.
- One continuous stream for the device's life (tango-bulk's model): gives up per-acquisition
  completeness and makes every saver detect boundaries. Cut 3's start fence answers its one
  advantage, the join race.
- Sessions spanning instances (protocol v4): the ordered geometry-event design Cut 2 rejected.
  Not without new evidence.
- An implicit, process-wide registration cache (the prototype): the pattern MPI and UCX's rcache
  are known for, with memory-hook and fork trouble. Explicit ownership is what newer APIs choose.
