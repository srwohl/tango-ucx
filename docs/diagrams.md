# Diagrams

How the library is put together, what passes between a device and its subscribers, and the
states each part moves through. Terms are those of [CONTEXT.md](../CONTEXT.md). Names in
`code font` are the ones in the source.

1. [Structure](#1-structure)
2. [What Tango carries and what UCX carries](#2-what-tango-carries-and-what-ucx-carries)
3. [A subscription from Open to Close](#3-a-subscription-from-open-to-close)
4. [The path of a frame](#4-the-path-of-a-frame)
5. [Publisher instance states](#5-publisher-instance-states)
6. [Session states](#6-session-states)
7. [Subscription states](#7-subscription-states)
8. [Source slot states](#8-source-slot-states)
9. [Receive slot states](#9-receive-slot-states)
10. [Public types](#10-public-types)
11. [What owns what](#11-what-owns-what)

## 1. Structure

The parts of each process and the source file each one is in. The two processes meet in two
places only: Tango, and UCX.

```mermaid
flowchart LR
    subgraph DP["Device process"]
        DC["<b>Device code</b><br/>acquires a slot, fills it, publishes"]
        subgraph DTA["Tango adapter"]
            CMD["<b>UCX commands</b> Open, Close<br/><b>Attributes</b> UcxStream, UcxSessions,<br/>UcxPublished, UcxSkipped,<br/>UcxPressure, UcxFailedEvery<br/><code>src/tango/device.cpp</code>"]
        end
        subgraph DTR["Transport"]
            PUB["<b>Publisher</b><br/>loop thread, control table,<br/>session rows, grants<br/><code>publisher.cpp</code>"]
            SRC[("Source ring<br/>and records")]
            PLOW["<b>UCX worker and endpoints</b><br/><code>ucp.cpp, memory.cpp, wire.cpp</code>"]
            PGPU["<b>GPU runtime</b>, optional<br/><code>src/cuda/runtime.cpp</code>"]
        end
    end

    subgraph SP["Subscriber process"]
        subgraph STR["Transport"]
            SLOW["<b>UCX worker and endpoint</b><br/><code>ucp.cpp, memory.cpp, wire.cpp</code>"]
            RCV[("Receive ring<br/>and records")]
            SUB["<b>Subscription</b><br/>loop thread, batch rows,<br/>delivery queue<br/><code>subscription.cpp, batch.cpp</code>"]
            GPU["<b>GPU runtime</b>, optional<br/><code>src/cuda/runtime.cpp</code>"]
        end
        subgraph STA["Tango adapter"]
            SUBSCRIBE["<b>every, latest, pull</b><br/>finds the UCX commands<br/>by their markers<br/><code>src/tango/subscribe.cpp</code>"]
        end
        APP["<b>Application</b><br/>C++, or Python<br/>through <code>tango_ucx</code>"]
    end

    DC -- "acquire, publish,<br/>ucx_start, finish" --> PUB
    CMD -- "JSON documents" --> PUB
    PUB --- SRC
    PUB --- PLOW
    PGPU --- PUB
    PLOW == "UCX: Ready, Frame, End" ==> SLOW
    SLOW == "UCX: Credit" ==> PLOW
    SLOW --- SUB
    RCV --- SUB
    GPU --- SUB
    SUB -- "read: Batch" --> APP
    SUBSCRIBE -- "prepare, start" --> SUB
    APP -- "every, latest, pull" --> SUBSCRIBE
    CMD <== "Tango: discovery and control" ==> SUBSCRIBE
```

## 2. What Tango carries and what UCX carries

Tango carries what happens once for a subscription, as JSON documents. UCX carries what happens
for each frame, as active messages between the two loop threads. No Tango call is made on a
loop thread, and no frame passes through Tango.

```mermaid
flowchart LR
    subgraph DP["Device process"]
        DAPP["<b>Device code</b>"]
        DTANGO["<b>Tango threads</b><br/>UCX commands,<br/>attributes"]
        DLOOP["<b>Publisher<br/>loop thread</b>"]
    end
    subgraph SP["Subscriber process"]
        SLOOP["<b>Subscription<br/>loop thread</b>"]
        SAPP["<b>Application thread</b><br/>every, latest, pull,<br/>read, close"]
    end

    DAPP -- "slots" --> DLOOP
    DTANGO -- "control table" --> DLOOP
    DLOOP -. "Ready, Frame, End" .-> SLOOP
    SLOOP -. "Credit" .-> DLOOP
    SLOOP -- "batches" --> SAPP
    DTANGO <-- "markers, UcxStream, Open, Close" --> SAPP
```

Solid lines are Tango or calls within a process. Dotted lines are UCX.

| Carried by | Message | From | When |
|---|---|---|---|
| Tango | `command_list_query`, for the markers | subscriber | once, to find the UCX commands |
| Tango | read `UcxStream` | subscriber | once, for the instance ID and description |
| Tango | Open | subscriber | once, with the session, ring depth, batch size, delivery and UCX address |
| UCX | Ready | publisher | once, when the session is granted |
| UCX | Frame | publisher | for each frame, with its sequence, record and payload |
| UCX | Credit | subscriber | as receive slots are released; it is cumulative |
| UCX | End | publisher | once, with the sequences and frames sent and the reason |
| Tango | Close | subscriber | once, with the session |

## 3. A subscription from Open to Close

One every subscription, from finding the device to closing the session. The arrows with an
open head are UCX active messages. Every other message between the two processes is a Tango
call.

```mermaid
sequenceDiagram
    autonumber
    box Device process
        participant D as Device code
        participant P as Publisher loop
    end
    participant T as Tango
    box Subscriber process
        participant S as Subscription loop
        participant A as Application
    end

    A->>T: command_list_query
    T-->>A: the UCX commands, found by their markers
    A->>T: read UcxStream
    T-->>A: instance ID, description, allow_every
    A->>S: prepare
    Note over S: ring depth from the budget,<br/>receive ring allocated, loop started
    A->>T: Open (session, instance ID, ring depth,<br/>batch size, delivery, UCX address)
    T->>P: control table
    Note over P: checks the request, takes a session row,<br/>makes the endpoint
    P-)S: Ready
    P-->>T: ok, UCX address
    T-->>A: reply
    A->>S: start
    Note over S: makes the endpoint, waits for Ready
    S-->>A: Subscription

    D->>P: ucx_start
    loop each frame
        P-->>D: acquire returns a slot,<br/>once every session has room
        D->>P: fill payload and fields, publish
        Note over P: gives the frame its index and timestamp
        P-)S: Frame (sequence, frame index, timestamp,<br/>application fields, payload)
        Note over S: payload lands in its receive slot,<br/>frames are assembled into a batch
        S-->>A: read returns a Batch
        A->>S: last view gone: the batch is released
        S-)P: Credit (cumulative)
    end

    D->>P: finish
    P-)S: End (sequences, frames, finished)
    S-->>A: outcome is End, after the last batch
    A->>T: Close (session)
    T->>P: control table
    Note over P: flushes and closes the endpoint,<br/>frees the session row
    P-->>T: ok
    T-->>A: reply
    Note over S: flushes and closes the endpoint,<br/>frees the receive ring
```

## 4. The path of a frame

What decides whether a frame is sent, skipped or held back, on each side. The grant is where
pressure is applied: the device gets no slot until every session that must receive the frame
has room for it. Credit from the subscriber is what gives a session room again.

```mermaid
flowchart TB
    subgraph SUBSIDE["Subscriber process"]
        direction TB
        S1(["Frame header arrives<br/>from the publisher"])
        S2{"Valid? session, sequence<br/>within the ring, receive<br/>slot free, size"}
        S3(["Outcome: failed"])
        S4{"Payload came<br/>with the header?"}
        S5["Copy it into<br/>the receive slot"]
        S6["Receive it straight<br/>into the receive slot"]
        S7["Landed"]
        S8{"Batch size reached,<br/>max_wait passed,<br/>ring full, or End?"}
        S9["Deliver the batch to the queue.<br/>Latest: it replaces batches<br/>not yet read"]
        S10["Application:<br/>read returns the Batch"]
        S11["Released: last view gone, and<br/>a GPU view's event completed"]
        S12{"Every earlier<br/>sequence released?"}
        S13(["Send Credit, cumulative,<br/>to the publisher"])

        S1 --> S2
        S2 -- no --> S3
        S2 -- yes --> S4
        S4 -- yes --> S5
        S4 -- no --> S6
        S5 --> S7
        S6 --> S7
        S7 --> S8
        S8 -- "no: wait<br/>for more" --> S7
        S8 -- yes --> S9
        S9 --> S10
        S10 --> S11
        S11 --> S12
        S12 -- "no: wait<br/>for them" --> S11
        S12 -- yes --> S13
    end

    subgraph PUBSIDE["Device process"]
        direction TB
        P0(["Device: acquire(deadline)"])
        P1{"Started, not finished,<br/>and a source slot free?"}
        P2{"Every every session<br/>and the range's puller<br/>have room?"}
        P3["Pressure: wait for Credit"]
        P4{"Device waiting longer<br/>than pressure_timeout?"}
        P5["Evict the session:<br/>End (evicted)"]
        P6(["No slot: the device<br/>waits, buffers or stops"])
        P7["Slot granted: the device fills<br/>payload and fields"]
        P8{"publish?"}
        P9(["Cancelled:<br/>no frame index used"])
        P10["Route in grant order: frame index<br/>and timestamp go in the record"]
        P11{"For each session:<br/>its delivery?"}
        P12{"Room, nothing in flight,<br/>max_rate interval passed?"}
        P13(["Skip, counted"])
        P14["Send Frame to the session"]
        P15(["Every send completed:<br/>source slot free"])

        P0 --> P1
        P1 -- "no, until<br/>the deadline" --> P6
        P1 -- yes --> P2
        P2 -- no --> P3
        P3 --> P4
        P4 -- yes --> P5
        P5 --> P2
        P4 -- "no, until<br/>the deadline" --> P6
        P2 -- yes --> P7
        P7 --> P8
        P8 -- "no: Slot<br/>destroyed" --> P9
        P8 -- yes --> P10
        P10 --> P11
        P11 -- "every, or the<br/>range's puller" --> P14
        P11 -- latest --> P12
        P12 -- yes --> P14
        P12 -- no --> P13
        P14 --> P15
    end
```

## 5. Publisher instance states

One publisher instance, from construction to finish. Each new instance has a new instance ID
and restarts the frame index.

```mermaid
stateDiagram-v2
    Before: Before the start
    Before: Every, latest and pull join
    Before: No slot is granted, so acquire waits
    Started: Started
    Started: Slots are granted while the sessions have room
    Started: Latest joins. Every and pull are refused
    Finishing: Finishing
    Finishing: No more grants. Grants not yet acquired are taken back
    Finishing: Open is refused
    Ended: Ended
    Ended: End is sent to each session once its sends complete
    Ended: Sessions stay until they are closed

    [*] --> Before: Publisher constructed
    Before --> Started: ucx_start
    Before --> Finishing: finish
    Started --> Finishing: finish
    Finishing --> Ended: every slot the device holds is routed
    Ended --> [*]: Publisher destroyed
```

A publisher destroyed in any state sends End (closing) to the sessions it still has and closes
them. What is not closed three seconds later goes to quarantine.

## 6. Session states

One row of the publisher's session table.

```mermaid
stateDiagram-v2
    Free: Free
    Ready: Ready
    Ready: Frames are sent and Credit is received
    Pressure: Pressure
    Pressure: No room for another frame, every and pull only
    Pressure: The device gets no slot
    Ended: Ended
    Ended: End (finished) was sent
    Ended: The row stays until the session is closed
    Retiring: Retiring
    Retiring: End is sent if it is due
    Retiring: The endpoint is flushed, then closed
    Retiring: After 2 s it is closed by force
    Quarantine: Quarantine
    Quarantine: The publisher's memory is kept until the process exits

    [*] --> Free
    Free --> Ready: Open accepted, Ready sent
    Ready --> Pressure: no room
    Pressure --> Ready: Credit
    Pressure --> Retiring: device waiting longer than pressure_timeout
    Ready --> Ended: publisher finished, sends complete
    Ready --> Retiring: Close command
    Ready --> Retiring: failure, or Publisher destroyed
    Ended --> Retiring: Close command
    Retiring --> Free: endpoint closed, Close answered
    Retiring --> Quarantine: endpoint not closed after 3 s
```

Why a session retires decides whether End is due:

| Cause | End |
|---|---|
| Close command | none |
| UCX reports the endpoint failed, or a send failed | none |
| Eviction | End (evicted) |
| Credit for more than was sent, or a send the subscriber rejected | End (protocol) |
| Publisher destroyed | End (closing) |

An Open that is not accepted leaves the row free and answers with a status: `instance_id`,
`finished`, `every-refused`, `started`, `malformed`, `range`, `no-session`, `endpoint`, `busy`
or `timeout`.

## 7. Subscription states

One subscription, as the subscriber sees it. End, Failed and Cancelled are the three outcomes.
Batches delivered before the outcome can still be read, and the outcome is reported once the
last of them has been.

```mermaid
stateDiagram-v2
    Prepared: Prepared
    Prepared: Receive ring allocated, loop thread running
    Opening: Opening
    Opening: Endpoint made, waiting for Ready
    Receiving: Receiving
    Receiving: Batches are delivered and Credit is sent
    OEnd: End
    OEnd: The publisher finished and everything was delivered
    OFailed: Failed
    OFailed: The session ended without the subscriber asking
    OCancelled: Cancelled
    OCancelled: The subscriber closed the subscription
    Closing: Closing
    Closing: Close command sent, receives cancelled
    Closing: Answered ok, the endpoint is flushed, then closed
    Closing: Otherwise it is closed by force
    Closed: Closed
    Closed: Receive ring freed once its last view is gone
    Quarantine: Quarantine
    Quarantine: Memory is kept until the process exits

    [*] --> Prepared
    Prepared --> Opening: Open accepted
    Prepared --> Closed: Open refused
    Opening --> Receiving: Ready
    Opening --> Closing: no Ready in 5 s, or no endpoint
    Receiving --> OEnd: End (finished), every sequence delivered
    Receiving --> OFailed: failure
    Receiving --> OCancelled: close
    OEnd --> Closing: close
    OFailed --> Closing: close
    OCancelled --> Closing
    Closing --> Closed: endpoint closed
    Closing --> Quarantine: not closed 1 s after the deadline
    Closed --> [*]
```

A subscription fails when:

- End arrives with the reason evicted, protocol or closing;
- UCX reports the publisher unreachable;
- a frame is invalid, or its transfer fails;
- a sequence below End has not arrived two seconds after the last frame;
- an every subscription reaches End with a frame index missing;
- a puller reaches End and its set has missing frames;
- Credit cannot be sent;
- a GPU event fails.

## 8. Source slot states

One slot of the publisher's source ring.

```mermaid
stateDiagram-v2
    direction LR
    state "Free" as Free
    state "Granted" as Granted
    state "Held by the device" as Held
    state "Posted" as Posted
    state "Cancelled" as Cancelled
    state "Sending" as Sending

    [*] --> Free
    Free --> Granted: started, and the sessions have room
    Granted --> Held: acquire
    Granted --> Free: finish
    Held --> Posted: publish
    Held --> Cancelled: Slot destroyed
    Posted --> Sending: routed, frame index given
    Posted --> Free: routed, no session takes it
    Cancelled --> Free: routed, no frame index used
    Sending --> Free: every send completed
```

Slots are routed in the order they were granted, so a slot posted early waits for the ones
granted before it.

A GPU source slot is written through `gpu_payload` on one GPU stream. Publishing or cancelling
it records an event on that stream, and the slot is not routed until the event completes. An
event that fails finishes the publisher and ends its sessions with End (protocol).

## 9. Receive slot states

One slot of a subscription's receive ring. The sequence picks the slot.

```mermaid
stateDiagram-v2
    direction LR
    state "Free" as Free
    state "Receiving" as Receiving
    state "Landed" as Landed
    state "In a batch" as InBatch
    state "Waiting for its GPU event" as EventWait
    state "Released" as Released

    [*] --> Free
    Free --> Receiving: Frame header, payload to follow
    Free --> Landed: Frame header with its payload
    Receiving --> Landed: transfer completed
    Landed --> InBatch: assembled and queued
    InBatch --> Released: last view gone
    InBatch --> Released: latest, replaced before it was read
    InBatch --> EventWait: GPU view ended, or release_after
    EventWait --> Released: event completed
    Released --> Free: every earlier sequence released, so Credit covers it
```

`release_after` is for pinned host receive: the batch's slots wait for the uploads queued on
the GPU stream. A GPU event that fails leaves its slots held: they are never credited, the
subscription fails, and its receive ring goes to quarantine.

## 10. Public types

The types of `tango-ucx/ucx.h` and the functions of `tango-ucx/tango.h`.

```mermaid
classDiagram
    direction LR
    class Publisher {
        Publisher(Description, PublisherLimits)
        ucx_start()
        acquire(Deadline) optional~Slot~
        finish()
        health() Health
        description() Description
    }
    class Slot {
        payload() span~byte~
        gpu_payload(GpuStream) pointer
        payload_bytes() size_t
        fields() span~byte~
        publish(timestamp_ns)
        publish_bytes(bytes, timestamp_ns)
    }
    class Subscription {
        read(Deadline) optional~Batch~
        outcome() optional~Outcome~
        fd() int
        description() Description
        close(Deadline)
        health() Health
    }
    class Batch {
        frames() size_t
        index(frame) uint64
        payload(frame) span~byte~
        payload_bytes(frame) size_t
        payload() pointer
        records() pointer
        gpu() bool
        copy() Batch
        gpu_view(GpuStream) GpuView
        release_after(GpuStream)
    }
    class GpuView {
        payload() pointer
        payload(frame) pointer
        payload_bytes(frame) size_t
        records() pointer
        frames() size_t
    }
    class GpuStream {
        pointer handle
    }
    class Description {
        Element element
        vector~uint64~ shape
        uint32 field_bytes
        string field_dtype
        uint64 max_payload_bytes
        string application_text
    }
    class PublisherLimits {
        uint64 budget
        uint32 max_sessions
        bool allow_every
        milliseconds pressure_timeout
        int gpu
    }
    class SubscriptionOptions {
        uint64 budget
        uint32 batch
        microseconds max_wait
        double max_rate
        int gpu
        bool allow_gpu_over_tcp
        string label
        int pinned
    }
    class Health {
        frames, skipped
        every, latest, failed_every
        pressure, pressure_from
        slots_free, slots_held
        last_index
        transfer_max_ns, transfer_mean_ns
        quarantined_bytes
        memory_hooks, started
        transport, failure
    }
    class Outcome {
        <<enumeration>>
        End
        Failed
        Cancelled
    }
    class tango_h["tango.h"] {
        <<functions>>
        install_ucx_commands(commands, prefix)
        install_ucx_attributes(attributes)
        attach_publisher(DeviceImpl, Publisher)
        detach_publisher(DeviceImpl)
        every(DeviceProxy, SubscriptionOptions) Subscription
        latest(DeviceProxy, SubscriptionOptions) Subscription
        pull(DeviceProxy, range, SubscriptionOptions) Subscription
    }

    Publisher *-- Description
    Publisher ..> PublisherLimits
    Publisher ..> Slot : acquire
    Publisher ..> Health
    Subscription ..> Batch : read
    Subscription ..> Outcome
    Subscription ..> Health
    Subscription --> Description : from UcxStream
    Batch ..> GpuView : gpu_view
    Slot ..> GpuStream : gpu_payload
    Batch ..> GpuStream : gpu_view, release_after
    tango_h ..> Publisher : attach
    tango_h ..> Subscription : every, latest, pull
    tango_h ..> SubscriptionOptions
```

## 11. What owns what

The fixed tables behind the public types. Everything here is allocated once, at setup, from
the budget.

```mermaid
classDiagram
    direction TB
    class Publisher
    class Slot
    class Subscription
    class Batch

    class Pub {
        loop thread
        instance_id
        ready, every, pull, retiring masks
        next_frame_index
        grants
    }
    class SourceSlot {
        posted, cancel, held
        payload_bytes, timestamp
        sends
        GPU stream, GPU event
    }
    class PubRow {
        session
        sent, credited, reserved
        ring_depth, in_flight
        ready, closing, end_sent
    }
    class PublisherOperations {
        one send per slot and row
        Ready, End, Flush, Close per row
    }
    class ControlTable {
        call(Request, Deadline) Reply
    }
    class ControlEntry {
        Free, Filling, Posted
        Taken, Replied, Abandoned
    }
    class Region {
        registered with UCX
    }
    class Sub {
        loop thread
        session
        assembled, credited
        outcome
        queue
    }
    class Rings {
        payload
        records
        placements
        busy, landed, returned, event_wait
        released
    }
    class BatchRow {
        first_sequence, frames
        viewed, recorded
        GPU event
    }
    class Worker["ucp::Worker"] {
        endpoint, progress
    }
    class Owner
    class Quarantine {
        keep(Owner, bytes)
    }
    class GpuRuntime {
        <<interface>>
        allocate, allocate_pinned, release
        make_event, destroy_event
        record, query
        copy_to_host
    }

    Publisher *-- Pub
    Slot o-- Pub : shared
    Pub *-- "slot_count" SourceSlot
    Pub *-- "max_sessions" PubRow : session rows
    Pub *-- PublisherOperations
    Pub *-- ControlTable
    ControlTable *-- "8" ControlEntry
    Pub *-- Region : source ring
    Pub *-- Worker
    Pub ..> GpuRuntime : GPU source ring

    Subscription *-- Sub
    Sub o-- Rings : shared
    Batch o-- Rings : shared
    Rings *-- "ring_depth" BatchRow
    Sub *-- Region : receive ring
    Sub *-- Worker
    Rings ..> GpuRuntime : GPU or pinned ring

    Owner <|-- Pub
    Owner <|-- Sub
    Quarantine o-- Owner
```
