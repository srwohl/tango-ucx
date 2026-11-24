<!--
SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-ucx project

SPDX-License-Identifier: LGPL-3.0-or-later
-->

# A long-lived Subscriber that follows the device

Status: **proposed**

## Context

A saver must write every frame of every acquisition, across many acquisitions, without a restart.
Each acquisition is one publisher instance (ADR 0001): one `begin`, one start, one End. The next
acquisition should be able to prepare and start while the last one is still draining. tango-ucx
cannot do this today, for three reasons:

- **Nothing says that an instance began or ended.** No tango-ucx attribute pushes an event. An
  every subscription or a puller must join before the start. End reaches sessions on the data
  plane only, so a client without a session never learns it.
- **`begin` waits for the last instance, and then cuts it off.** It waits until nothing of N is
  being sent. After 2 s it retires N's sessions, and a saver still receiving N fails.
- **The receiving side starts cold each time.** Each `Subscription` makes its own UCX context,
  worker, progress thread and receive ring. The ring's pages fault on the first frames, and nothing
  bounds the memory of several subscriptions together.

**This ADR decides what tango-ucx provides. No device is required to adopt it.** Lima's
`CtControl::prepareAcq` refuses while an acquisition is running, so a Lima device cannot use the
publisher's overlap as it stands. That is outside this ADR.

Start and end of an acquisition are common practice: DECTRIS Stream V2's start and end messages,
areaDetector's Acquire and DetectorState, Lima's `acq_status`, Bluesky's stage and unstage, and
Jungfraujoch's start and end messages.

## Measured

Measured 2026-11-22 and 23 at `f8300a2` with scratch programs outside the repository: one 18-thread
host, UCX 1.22, `-O2`, no RDMA device.

**The next `begin` with N still in flight.** Two processes over shared memory, 15 frames of 2 MiB.
The saver was paused with `SIGSTOP` to hold the sends in flight. Two runs of each:

| Saver paused for | `begin` returned after | N at the saver |
|---|---|---|
| 0.5 s | 0.52 s | complete |
| 1.5 s | 1.52 s | complete |
| 3 s | 2.03 s | **failed: "the publisher closed"** |
| 6 s | 2.03 s | **failed: "the publisher is unreachable", 0 frames** |

A saver that is only slow to write does not cause this. A grant needs room in every every
session's ring, so published frames go straight to the saver's ring. With the saver reading nothing
for 4 s, `begin` returned in under 1 ms and N ended complete. **What drains from the publisher is
transfers in flight,** as on a slow or stalled link.

**A long-lived worker keeps its peers' shared-memory segments.** Over `sysv` or `posix`, the
publisher kept two UCX segments per finished session until the `Publisher` was destroyed: about
2.1 MiB resident per session with 2 MiB frames, 0.04 MiB with 8 KiB frames. It needs a session that
carried frames. TCP showed no growth, and RDMA is untested. Raw UCX reproduces it with no tango-ucx
code. The cause is not known.

**Opening a subscription.** Frames of 2048 × 2048 × 16 bits, 30 opens per ring size, publisher and
subscriber in one process, no Tango round trip:

| Step | p50 | Largest seen |
|---|---|---|
| Cold open, total, rings of 64 MiB to 1 GiB | 11.2–11.7 ms | 14.4 ms |
| of which context, worker, ring and tables | 9.4–9.8 ms | 12.0 ms |
| of which Open handled by the publisher | 0.7 ms | 1.0 ms |
| of which endpoint and Ready | 1.1 ms | 1.9 ms |
| A worker on a live context | 4.2 ms | 5.4 ms |
| Registering a ring (no RDMA device) | 0.02 ms | 0.04 ms |
| Faulting in a ring of 64 MiB, 256 MiB, 1 GiB | 13, 61, 259 ms | 15, 71, 317 ms |
| First context in a fresh process, then its worker | 120–133 ms, then 4–6 ms | |

A live Subscriber with a reused ring would pay for a worker, Open, the endpoint and Ready: about
6 ms, an estimate from the parts. **Open latency alone does not justify a Subscriber on this host:**
a cold open takes under 1% of a 2 s start fence. The gain is warm rings, one thread and core, and a
memory bound. ADR 0001 recorded about 2.2 s for the first publisher of a process. This host took
0.13 s for the first context, and the difference was not investigated.

**Sharing one worker between sessions is not safe.** UCX pairs the endpoints between two workers by
the order in which each side creates them (`ucp_ep_match` in `ucp_ep_create_api_to_worker_addr`).
tango-ucx creates them on two sides at different times. Two sessions were run between the same two
workers, over SysV shared memory and over TCP on loopback, and the first was closed by force. Each
case ran once per transport:

| Case | One worker shared | One worker per session |
|---|---|---|
| Same creation order on both sides | second session healthy | not run |
| An Open abandoned before the subscriber's endpoint | **subscriber's endpoint reset** | healthy |
| Replies to two Opens handled in the other order | **publisher's endpoint reset**, and the subscriber's on TCP | healthy |

With a graceful close, the crossed case on a shared worker also cancelled a rendezvous frame, on
both transports.

**One thread per session has room.** One puller over shared memory, with readers that release at
once, took 6.96 GiB/s of 8 MiB frames and 184,000 frames/s of 32 KiB frames. Four pullers took
7.42 GiB/s and 212,000 frames/s, so the receiving thread was not the main limit.

## Decision

**The work lands in three steps, each useful alone:** the two events; overlapping instances on the
publisher; and a long-lived `Subscriber`, once the measurements below show what it is worth. Until
the third step, a saver opens one independent `Subscription` per instance.

### Step 1: two Tango events per instance

**Each publisher instance pushes two plain Tango change events.** They are called **Begun** and
**Ended**, because the glossary's Start is `ucx_start`, which comes later, and its End is a
subscription's outcome. One `begin` gives one Begun and one Ended. A repeated `ucx_start` or
`finish` gives no other event, and individual frames are identified by their records.

| Event | Attribute | When | Value |
|---|---|---|---|
| Begun | `UcxStream` (exists) | the instance answers Open, before the device can start it | the `tango-ucx-stream` document, unchanged |
| Ended | `UcxLast` (new) | no session is still owed an End | a `tango-ucx-last` document |

```json
{"interface":"tango-ucx-last/0.1","instance_id":1234,"outcome":"end","frames":1000,"missing":0}
```

`UcxLast` is a scalar DevString like `UcxStream`. Before any instance has ended its `instance_id`
is 0, which no instance has, and it has no `outcome`. Its keys are wire, for `tools/wire.txt`:

```
key last interface string
key last instance_id u64
key last outcome enum:outcome              # absent while instance_id is 0
key last frames u64
key last missing u64
enum outcome end
enum outcome failed
enum outcome cancelled
```

**The device states the outcome.** The publisher cannot tell a last frame from a stop, because
both call `finish()`. So `finish` becomes `finish(Outcome = Outcome::End)`, and the first call wins,
as `InstancePhase::finish` already decides. A `begin` with no earlier finish gives `cancelled`. A
failed GPU source, or a pull set with missing frames, gives `failed`, whatever the device said.

**Ended's condition** is that the instance's phase is Ended, no session is Ready, none has an End
due and none has a send in flight, or that the instance is quarantined. A session that failed or
closed is owed no End.

**The progress loop reports; a device thread pushes.**

```cpp
struct InstanceEvent
{
    enum class Kind { Begun, Ended } kind;
    std::uint64_t instance_id;
    std::string document;
};
void Publisher::on_instance(std::function<void(const InstanceEvent &)>);
```

The callback runs on the publisher's thread. Like `publish_from`'s `done`, it must be short and must
not call the publisher. It only queues. It never touches Tango or the GIL.
`DeviceImpl::push_change_event` takes the device's monitor, waiting up to 3.2 s before it throws
`API_CommandTimedOut`, so a push on the loop could stall every send.

**A C++ device** gets this from the glue. `install_ucx_attributes` adds `UcxLast` and declares both
attributes with `set_change_event(true, false)`. `attach_publisher` registers the callback and
starts one pushing thread; `detach_publisher` stops and joins it. A push that fails is logged. The
attribute still answers a read.

**A PyTango device** with its publisher in C++ runs the same loop in a Python daemon thread: a
binding that waits for the next event with the GIL released, then `push_change_event`. **No C++
thread ever takes the GIL.** PyTango takes the monitor with the GIL released and takes the GIL again
afterwards. A device whose UCX commands answer only after its own preparation queues Begun once
they do.

### Step 2: overlapping instances on the publisher

**`PublisherLimits::instances` is the most instances that may hold a source ring at once.** It
defaults to 1, and then `begin` behaves exactly as it does today, so no device changes unless it
asks.

**With two or more, `begin` does not wait for the last instance's transfers.** It finishes N, takes
a free region of the arena, makes N+1 and returns. N stays in the draining list with its ring and
sessions, sends what is in flight, sends End, and closes its sessions only then. `begin` no longer
retires a session that is still receiving.

**The arena is divided into equal regions, one per instance,** as ADR 0001 foresaw. Each instance's
ring is planned from `budget / instances`, so `budget` stays the bound on source memory and an
instance's depth is known before it starts. A device that wants the same depth with overlap doubles
its budget. A description that needs a larger arena waits until no region is in use.

**When no region is free, `begin` waits and then refuses.** `begin(Description, Deadline)` throws
at the deadline, and `begin(Description)` uses 2 s. N stays finished and goes on draining. **A
refused `begin` never cuts a draining instance.**

**A draining session that makes no progress is bounded.** It is evicted after `pressure_timeout`,
as a session holding pressure is, or retired when UCX reports its peer failed. With no
`pressure_timeout` its region stays held until then, and the next `begin` refuses visibly.

**What stays as it is.** Open goes to the active instance, so an Open for a draining one is refused.
Credit and Close already find a draining instance's session. `acquire`, `ucx_start`, `finish` and
Health act on the active instance. A `Slot` the device still holds keeps its own instance, and
publishes to it. The wire does not change.

**A lent ring stays valid until its last `done`,** no longer until the next `begin`. A device that
overlaps must tell the buffers of two instances apart.

**A quarantined instance's region is never reused.** When no region is left, `begin` makes a new
arena, as ADR 0001 does. The old one stays until the process exits, and Health counts it.

### What a follower can rely on

1. **Begun is queued before the device can start the instance,** so no frame is sent before it is
   queued. It is not necessarily received. With a fence that counts the follower, no frame is sent
   until the follower has joined. Without a fence, nothing protects a slow follower.
2. **Ended is queued after every session owed an End has been sent it.** It travels apart from the
   frames and can arrive before a session's last frames. It does not mean the saver has them.
3. **Begun comes before Ended for one instance.** Across instances there is no order: with overlap,
   Begun(N+1) normally comes before Ended(N). A follower keys everything by `instance_id`.
4. **On subscribe, Tango reads the attribute** and delivers it as the first callback
   (`EventConsumer::get_fire_sync_event`). That value may name an instance that has ended, and may
   duplicate a push. Open's reply settles it: `instance_id`, `finished` or `started` means this
   follower cannot join.
5. **A lost event is visible.** cppTango reports a counter gap as an `API_MissedEvents` error event,
   and its heartbeat period is 10 s. On any error event a follower reads both attributes again.
   `UcxLast` holds only the latest instance to end, so an older Ended can be lost.

### How a saver follows

1. Subscribe to `UcxStream` and `UcxLast`, stateless, because `UcxStream` raises while a device has
   no instance. The event callback only queues.
2. On each `UcxStream` value with an unseen `instance_id`, open a subscription, with a deadline
   inside the device's fence.
3. If Open is refused, record the instance as missed. That is never fatal to the saver.
4. **Keep the backlog on the saver.** One thread copies each batch to bounded memory
   (`Batch::copy`), which releases the ring at once, and another writes to disk. The bound is in
   bytes. When it is reached the saver stops copying, and pressure reaches the device.
5. Close the file on the subscription's outcome.
6. **Call an acquisition complete only if the session's outcome is End and Ended says `end`.** A
   session's End means it received everything that was *published*. Through LimaCCDs, a device that
   faulted after 15 of 100 frames still gave its saver a session End. Ended also gives `frames`
   and `missing` to check against. If Ended has not come after a grace period, read `UcxLast`. If it
   names another instance, mark the acquisition unverified.
7. An Ended for an instance never opened is recorded as a gap, with its frame count.

**The Python side has no follower helper for now.** A Python saver uses PyTango events directly.

### Step 3: the Subscriber

**The long-lived object on the receiving side is the `Subscriber`.** It owns one UCX context, one
progress thread and a pool of receive rings. A `Subscription` stays what it is: one session of one
instance, with frame index from 0 and its own outcome. One `Subscriber` makes many `Subscription`s.

**Each Subscription keeps its own worker.** The context and the thread are shared; the worker is
not. A worker costs about 4 ms, removes the endpoint pairing above, and keeps each UCX handler's
argument as the `Sub` it serves, so the frame path gains no lookup by session.

**One thread progresses all of a Subscriber's workers,** which stay in single-thread mode. All UCX
calls stay on it, ring registration included. Its idle wait covers every worker's event fd and one
wake that released batches share. A session's close becomes steps of that loop, as the publisher's
`retire_pass` is, because `Sub::finish_close` blocks the thread it runs on today. An application
that needs more receive progress makes more Subscribers, such as one per puller.

**The wire does not change for a session.** Each session opens from its own worker's address, so
the publisher sees what it sees today: one worker per session.

**The API**, in C++:

```cpp
struct SubscriberLimits
{
    std::uint64_t budget = 1ull << 30; // every ring and table this Subscriber holds
    int progress_cpu = -1;
    std::string net_devices;
};
class Subscriber
{
  public:
    explicit Subscriber(SubscriberLimits = {});
    Subscriber(Subscriber &&) noexcept;
    Subscriber &operator=(Subscriber &&) noexcept;
    ~Subscriber();
};

// tango.h: join the instance that a UcxStream document names.
Subscription every(Subscriber &, Tango::DeviceProxy &, std::string_view stream, Deadline,
                   SubscriptionOptions = {});
Subscription latest(Subscriber &, Tango::DeviceProxy &, std::string_view stream, Deadline,
                    SubscriptionOptions = {});
Subscription pull(Subscriber &, Tango::DeviceProxy &, std::string_view stream, std::uint32_t range,
                  Deadline, SubscriptionOptions = {});

std::uint64_t Subscription::instance_id() const; // new, and needed from step 1
```

In Python:

```python
subscriber = tango_ucx.Subscriber(budget="2GiB")
subscription = tango_ucx.every(device, subscriber=subscriber, stream=document, timeout=2.0,
                               budget="256MiB", batch=16)
subscription.instance_id
```

With a Subscriber, `progress_cpu` and `net_devices` in the options are refused: they are the
Subscriber's. **The one-shot calls stay.** `every(proxy, options)`, `latest` and `pull` make a
private Subscriber, as `Publisher(Description, limits)` stays beside `begin` in ADR 0001. A
Subscription keeps its Subscriber's loop alive, so they may be destroyed in either order.

**A ring is reused once its batches are released, and another is allocated otherwise.** A ring is
free when its session is closed, no receive is in flight and its last batch is released. To open a
subscription, before Open is sent, the Subscriber:

1. takes the smallest free host ring that holds the planned payload;
2. otherwise gives back free rings that are too small until a new ring fits the budget, then
   allocates the ring and faults it in on the calling thread;
3. otherwise waits for a release until the deadline, then throws, naming the bytes held and the
   bytes needed.

**The bound:** rings in use, free rings, tables and whatever this Subscriber has quarantined never
exceed `SubscriberLimits::budget`. Following a device through one overlap takes twice a
subscription's budget per session. **When the budget cannot cover both,** the new subscription is
refused before its Open, so the publisher never sees it. A ring is never shrunk, and no batch is
evicted or copied behind the application's back.

**What is not pooled.** Records, batch rows and operation tables are per Subscription. GPU and
pinned rings stay per Subscription for now, and count against the budget. A quarantined ring is
never reused. It keeps its worker, and the context, until the process exits.

### Required measurements

On the target host and network, through the real UCX commands:

- **Before step 2 is accepted:** N+1 starting while N's transfers are held in flight, with N and
  N+1 both complete, including a stalled session and a refused `begin`.
- **Before multi-hour runs:** the cause of the shared-memory segments a long-lived publisher keeps,
  and whether RDMA has the same growth.
- **Before step 3 is built:** ring registration at 256 MiB and 1 GiB on RDMA, the first frames into
  a warm ring against a cold one, and per-thread CPU at the target rate.
- **Before step 3 is accepted:** cold open against an open on a live Subscriber, from Begun to
  Ready. The p99 must be under a quarter of the start fence's deadline. Per-frame cost with one
  thread and two workers while N drains and N+1 receives.

## Consequences

- **A saver can follow a device from step 1,** with one independent subscription per instance.
- **A device can overlap from step 2 by asking for it.** Devices that do not ask see no change in
  `begin`, including its 2 s limit.
- **With overlap each instance has a share of the budget,** so its depth is smaller unless the
  budget grows.
- **Ended is required for a complete acquisition,** because a session's End cannot see a device
  that stopped early.
- **The wire gains one document and one enum.** No existing key or data-plane message changes.
- **A C++ device must not wait in the start fence inside a command of the same device.** Under
  `BY_DEVICE` that command holds the monitor UcxOpen and the push need. UcxOpen already has this
  limit.
- **Step 3 splits `Sub`** into a loop and its sessions. Making close a set of loop steps is the
  largest change. Opening N+1 briefly stalls N's receives by the cost of making a worker.
- **ADR 0004's parts attach without a lifecycle change.** Series parts are sent on the session
  after Begun and before its first frame. End parts are sent before its End, so Ended already
  follows them.
- **ADR 0003's memory provider** would be asked only when a pool has no ring.

## Considered

- **Reopening a full Subscription per instance, with no Subscriber.** At 12 ms it fits the fence on
  this host. It keeps cold rings, needs a thread per session and has no shared memory bound. **It is
  what steps 1 and 2 use,** until the measurements show what the Subscriber is worth.
- **The roadmap's member endpoint (section 7).** A saver keeps an endpoint across instances, the
  fence waits for members instead of a count, and the same endpoint can announce an instance. It
  removes the count, but it is a new protocol, it reverses part of Cut 3, and it tells only savers.
  Tango events reach any client. It can come later, and the Subscriber is where it would live.
- **One worker for all sessions.** It fails as measured above. Making it safe needs one kept
  endpoint pair per publisher and teardown that no longer follows the session, which changes the
  publisher.
- **Sessions spanning instances (protocol v4).** ADR 0001 rejected them, and there is no new
  evidence.
- **Giving N+1 whatever N leaves of the arena, or a wrapping allocator.** Uses memory better, but
  an instance's depth then depends on how slow the last one is, and regions fragment.
- **Keeping the backlog in the publisher's ring.** The source ring may be a camera's own buffers,
  and holding them starves the camera. The backlog belongs to the saver.
- **Pushing on `begin`'s thread.** A device may call `begin` holding the GIL. A direct C++ push
  would wait for the monitor while a UCX command holding the monitor waits for the GIL, until the
  monitor times out.
- **Calling into Python from the progress thread.** Waiting for the GIL there stalls every send.
- **One attribute for both events.** One ordered stream, but it must carry or point to the
  description, and `UcxStream` already is the description.
- **A "Started" event once the fence passes.** Not until a consumer asks.
- **A smaller receive ring when memory is short.** It changes a subscription's depth and a pull
  set's ranges without saying so.

Prior art: the split of a long-lived connection from per-run streams is DECTRIS SIMPLON's series
and ADIOS2 SST's steps. Separate workers per stream on one context is UCX's own model of a context
that owns memory registrations and workers that own communication.

## Open

1. ADR 0004 says "How parts are sent and received is ADR 0005". That line should say "a later ADR".
   This ADR does not make the change.
2. Names, and with them this ADR's title and file name. Proposed: `Receiver` for the `Subscriber`
   class, since the glossary's **Subscriber** is a process; `UcxEnded` for `UcxLast`; `Follower`
   for the logic that follows a device; `FrameSaver` for a saving device. **Begun**, **Ended**, an
   instance's outcome and `PublisherLimits::instances` need glossary entries.
3. An `instance_number` that counts up per `Publisher`, in `UcxStream` and `UcxLast`. Instance IDs
   are random, so a follower cannot order them or see that it missed one.
4. What happens to a saver's failure by default: the device stops, or the saver is evicted and its
   file marked incomplete.
5. Whether `begin` with `instances = 1` should keep cutting off a session after 2 s.
6. Health for draining instances: `UcxSessions` and Health show only the active one.
7. Whether `UcxLast` should keep a few instances, now that overlap can overwrite an Ended.
8. A Subscriber's Health: rings held, free and waited for, and the saver's backlog.
9. A follower helper in `tango_ucx`, and a Python helper for PyTango devices that publish.
