# tango-ucx

A bulk data plane for Tango devices. Tango carries discovery and control. UCX carries
frames and reports a peer that dies, so the publisher can reclaim everything a dead
subscriber held.
This is the clean-slate vocabulary; it becomes the root `CONTEXT.md` of the new tree.

## Language

### The stream and its frames

**Device**:
A Tango device. Never a GPU.
_Avoid_: device (for a GPU), device memory, device server (for the device itself)

**UCX stream**:
The high-rate data a device publishes alongside its attributes, one per device.
_Avoid_: bulk stream, stream (alone), channel, feed, topic

**Frame**:
One published payload together with its record. Interpreting a frame never needs a Tango
round trip.
_Avoid_: image, sample

**Opaque stream**:
A UCX stream whose payloads are bytes the library carries unchanged and does not interpret,
such as a detector's compressed frames. Each frame has its own length, up to a maximum.
Every other UCX stream is an array stream: all its frames have the one size its shape gives.
_Avoid_: raw stream, compressed stream, variable stream, blob stream

**Record**:
The fixed-size metadata published with one frame: its frame index, its acquisition
timestamp and its application fields.
_Avoid_: metadata, stamp, frame metadata

**Application fields**:
The part of a record the device defines: a fixed number of bytes, described by a NumPy
dtype string that the library carries without interpreting.
_Avoid_: extension, user data, custom metadata

**Frame index**:
A frame's position in its incarnation's publication order, starting at 0. Every
subscription sees the same index for the same frame.
_Avoid_: frame number, frame id, event counter

**Description**:
What every frame of an incarnation is: element type, shape, and the size and dtype of the
application fields. An opaque stream's has no shape, and the most bytes a frame may have.
_Avoid_: geometry, layout, schema, array terms

**Incarnation**:
One publisher's life, from construction to finish, with one description. A new
incarnation restarts the frame index.
_Avoid_: epoch, generation, acquisition

**Batch**:
One or more frames of one subscription, delivered together. Their payloads form one packed
array and their records form another. An opaque batch's payloads are one run of bytes, each
frame directly after the one before it, and it has no array.
_Avoid_: chunk, frame list

**Batch size**:
The most frames a subscription puts in one batch. A batch may be shorter.
_Avoid_: chunk size

### Publishing

**Publisher**:
The part of a device that owns the UCX stream's source memory, grants sessions and
publishes frames.
_Avoid_: producer, server

**Start**:
The point at which a publisher begins to grant slots, set by the device with `ucx_start`.
No frame is published before it. An every subscription joins before it and is refused after
it; a latest subscription joins at any time.
_Avoid_: arm, go

**Slot**:
One frame-sized division of a ring. The device holds a source slot exclusively from
acquiring it until it publishes it as a frame or cancels it. An opaque stream's source slot
has the size of the largest frame, and the device says each frame's length when it publishes.
_Avoid_: slot handle, publication, cell

**Pressure**:
The condition in which an every subscription has no room for another frame, so the
publisher refuses new slots and the device must wait, buffer or stop.
_Avoid_: backpressure, stall, throttle

**Skip**:
A frame that a latest subscription does not receive because it had no room. Skipped
frames are counted and are never an error.
_Avoid_: drop, loss, discard

### Subscribing

**Subscriber**:
A process that receives a UCX stream.
_Avoid_: client, consumer, sink

**Subscription**:
What a subscriber holds to receive one UCX stream: one session, its receive ring, and the
batches delivered from it. It ends when its session ends.
_Avoid_: stream (alone), reader, attachment, connection

**Delivery**:
Which frames a subscription receives: every, latest or pull. It is chosen when subscribing
and fixed for the subscription.
_Avoid_: flow policy, flow, ordered, lossless, lossy

**Every**:
Delivery of every frame of the incarnation, in order, from frame index 0. The subscription
joins before the start, and applies pressure when it has no room.
_Avoid_: ordered, lossless, reliable

**Latest**:
Delivery of the newest frames the subscription has room for, skipping the rest. An
undelivered frame may be replaced by a newer one; a delivered frame never is.
_Avoid_: lossy, preview, freshest, conflate

**Pull**:
Delivery shared by a set of pullers: each frame goes to exactly one puller, in contiguous
ranges, and together they receive every frame. Named after ZeroMQ's PUSH/PULL. Planned,
not yet built.
_Avoid_: group, round-robin fan-out, load balancing

**Fan-out**:
Every subscription receiving its own copy of the frames its delivery entitles it to. This
is the relationship between subscriptions unless they pull.
_Avoid_: broadcast, multicast

### Sessions and memory

**Session**:
The publisher's grant to one subscription, with an identity. It lasts until the subscriber
closes it, either side dies, or the publisher evicts it. There is exactly one session per
subscription.
_Avoid_: connection, peer, seat, lease

**Sequence**:
A frame's position among one session's transfers, which picks its receive slot. It differs
from the frame index whenever frames are skipped or pulled.
_Avoid_: index (for this)

**Budget**:
The most memory one publisher or subscription may use for its ring and records. Ring
depth is derived from it, never asked for.
_Avoid_: pinned budget, quota, memory limit

**Ring depth**:
The number of slots in a ring. For a packed ring, the most frames it can hold.
_Avoid_: credit window, queue depth

**Packed ring**:
The receive ring of a subscription to an opaque stream. It has no slots: each frame is
placed directly after the one before it, and one that would cross the ring's end starts at
its front. Its size in bytes is what is left of the budget after the tables. The publisher
places each frame and states its position with the frame; the subscriber checks the position
and takes nothing from the order in which frames arrive.
_Avoid_: byte ring, variable ring, heap

**Borrow**:
How every batch is delivered: its views refer to the receive ring directly, so holding
them holds receive slots.
_Avoid_: zero copy, borrowed frame

**Copy**:
A batch whose payload and records were copied into application-owned host memory. It
holds no receive slots and never affects credit.
_Avoid_: clone, detached batch, copied frame

**View**:
Anything that refers to a batch's memory: the batch itself, a NumPy array, a record array,
a DLPack tensor.
_Avoid_: alias, retention, borrow token

**Release**:
The point at which a batch's receive slots may be reused: its last view is gone, and every
event recorded by its GPU views has completed.
_Avoid_: unpin, return

**Credit**:
A subscriber's permission for the publisher to reuse receive slots, given once those slots
are released. It is cumulative. In a packed ring it counts bytes as well as frames.
_Avoid_: ack, token, permit

**Quarantine**:
Memory and transport resources that are never reused, because nothing proved they are
safe. They are kept until the process exits and are counted.
_Avoid_: poisoned, zombie

### How a subscription ends

**Outcome**:
How a subscription ended, reported once after its last batch: end, failed or cancelled.
_Avoid_: end reason, terminal state, last error

**End**:
The outcome when the publisher finished its incarnation and the subscription received
everything its delivery entitles it to.
_Avoid_: eof

**Failed**:
The outcome when the session ended without the subscriber asking: the publisher died, the
transport broke, or the subscription was evicted.
_Avoid_: aborted

**Cancelled**:
The outcome when the subscriber closed the subscription.
_Avoid_: interrupted

**Eviction**:
The publisher ending an every subscription that has held pressure longer than the
publisher allows.
_Avoid_: kick

**Health**:
A sampled snapshot of a publisher's or subscription's counts and current pressure. It
never drives a decision.
_Avoid_: stats, metrics, counters

### GPU

**GPU batch**:
A batch whose payload is in GPU memory. Its records stay in host memory.
_Avoid_: device batch, CUDA batch

**GPU stream**:
A CUDA or ROCm stream on which the application orders GPU work.
_Avoid_: stream (alone), device stream

**GPU view**:
A scoped view of a GPU batch's payload for one GPU stream. When the scope ends, an event is
recorded on that stream, and the batch is not released until the event completes.
_Avoid_: device view, DLPack view

### Tango

**UCX commands**:
The Open and Close Tango commands of a device with a UCX stream. A subscriber finds
them by their markers, whatever the device names them.
_Avoid_: coordination channel, control channel, RPC

**Marker**:
The `tango-ucx/<major>` tag in a UCX command's type description.
_Avoid_: signature, magic
