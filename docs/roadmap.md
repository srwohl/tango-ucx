# Roadmap

What the first release, 0.1, holds, the frame model it needs, and the order of the work. Terms
are those of [CONTEXT.md](../CONTEXT.md). Status: being worked on; 0.1 is not released.

1. [The 0.1 release](#1-the-01-release)
2. [Frames made of parts](#2-frames-made-of-parts)
3. [Selections and the grant](#3-selections-and-the-grant)
4. [How detectors and cameras map](#4-how-detectors-and-cameras-map)
5. [Choices still open](#5-choices-still-open)
6. [Order of the work](#6-order-of-the-work)
7. [After 0.1](#7-after-01)
8. [Known edges](#8-known-edges)

## 1. The 0.1 release

0.1 is released when a subscriber can ask for part of each frame: some parts, some layers, a
band of rows, a rectangle, or the records alone. That is what makes adoption obvious: one
device serves a saver taking everything, a viewer taking one band, and analysis taking a
region of interest, each moving only its own bytes.

Until the release the wire stays 0.1 and may still change (`tools/wire.txt`). The rules that
keep it compatible across versions apply from 1.0.

## 2. Frames made of parts

A description is the device's own form: it states the shape, axis order and element as the
device produces them. A selection never asks the device to reshape.

A frame is a list of named **parts**, with one record shared by all of them. A part is either:

- an **array**: an element type and named axes, each with a size and optional labels, laid out
  in axis order. Its first axis may vary per frame up to a maximum, and its element may be a
  structure of named fields, as a record is; that describes decoded events and peak lists. A
  pair of axes may carry a mosaic pattern, such as RGGB on `(y, x)`; or
- **bytes** up to a maximum, with the name of their encoding (such as `bslz4`, `mono12p` or
  `tpx3`) and, when there is one, the array they decode to.

Today's array stream is a frame with one array part, and today's opaque stream a frame with one
bytes part. Record fields may belong to a part or follow an axis (`field_axes`), as a
detector's threshold energy, flat field and pixel mask belong to each threshold.

Each part has a fixed place in the source slot; a bytes part, or an array whose first axis
varies, takes its maximum, as an opaque slot does today. A Frame message states each part's
length. All parts of a frame arrive together.

## 3. Selections and the grant

A subscriber selects in Open:

- **parts**, by name; all of them when it names none;
- in each array part, for each axis, all of it, a set of indices or labels, or a range
  `[start, stop)` with an optional step; for a mosaic, its colours by name; for a structured
  element, its fields by name. An axis that varies per frame is taken whole;
- or **records only**, with no payload.

A bytes part can be chosen but not cut into. The publisher refuses what it cannot send, with a
status that names the reason, and otherwise states the **grant** in OpenReply: the parts and the
shapes it will send. The subscriber's description is the grant. The offer in `UcxStream` is an
upper bound, and OpenReply is the authority.

```python
select={"parts": ["threshold_2"]}                               # a compressed threshold
select={"band": ["green"], "y": [100, 612], "x": [200, 712]}  # a band and its interior rectangle
```

A subscriber gets each granted part as its own packed array, `(frames, ...)`; a stream of one
part keeps `batch.array`. Every, latest and pull each take a selection; the pullers of a set
share one. Sessions with and without a selection share an instance.

The publisher prepares the selection when Open is accepted, expressing it as runs of contiguous
bytes relative to the source slot. For a fixed ROI, reuse those offsets and lengths for every
frame. Each send supplies the current source address.

- A whole contiguous part or a band of full-width rows in a planar layout uses one run and
  the existing contiguous send path.
- An interior rectangle in a row-major image uses one run per selected row. Compare UCX
  scatter/gather IOV, the generic datatype and explicit packing for throughput and source-buffer
  hold time.
- A band of an interleaved layout, a mosaic colour, or fields of a structure uses an element
  gather through the generic datatype's pack callback.

`UcxStream` describes what can be selected: the parts, their axes and labels, the kinds of
selection and their limits, and which record fields follow which axis. `UcxSessions` shows each
session's selection and bytes per frame.

### Fixed ROIs and memory traffic

For a fixed-size array, a rectangle at the same position has the same row offsets, row lengths,
output shape and byte count in every frame. Prepare these from the granted selection and
source layout once per session. Reuse descriptor storage for each in-flight send, keeping its
IOV list and source pixels valid until completion. A ring slot with a stable address can reuse
its row pointers as well. Prepare a new plan when the source layout or selection changes.
The receiver gets the granted packed array, with its original frame index and timestamp.
The UCX API defines an IOV as a list of buffers and requires the list to remain valid until
the transfer completes. [UCX datatype documentation](https://openucx.github.io/ucx/api/latest/html/group___u_c_p___d_a_t_a_t_y_p_e.html)

UCX's `ucp_client_server.c` provides an active-message IOV example. `fill_request_param`
selects the IOV datatype and passes the number of segments as the count. `send_recv_am`
sends that list with `ucp_am_send_nbx` and handles rendezvous reception. The `-c am -v N`
options select this path with N buffers. It allocates buffers per iteration. Our adaptation
points those segments at image rows and reuses the layout across frames, with descriptor
storage held per in-flight operation or stable source slot.
[UCX client/server example](https://openucx.github.io/ucx/api/latest/html/ucp_client_server_8c-example.html)

The upstream active-message tests also exercise combinations of contiguous, IOV and generic
send and receive datatypes, including rendezvous. An IOV sender and a contiguous receiver
provide the mechanism for assembling selected source rows into one packed output array.
The fixed ROI and source-ring reuse policy are application decisions built on that mechanism.
[UCX active-message datatype tests](https://github.com/openucx/ucx/blob/v1.21.x/test/gtest/ucp/test_ucp_am.cc#L1891-L1894)

This makes IOV attractive for fixed rectangles. Preparing the list once removes repeated
allocation and geometry calculations. If UCX transfers directly from the rows, it also avoids
writing a temporary crop and reading that crop again for sending. The selected UCX protocol,
transport and memory type determine whether this direct path is available. Check it with the
project's UCX version and active-message send API in the benchmark.

Pixel traffic remains on every frame. A tall, narrow rectangle has many short segments and may
use only a few bytes from each fetched cache line. A wide rectangle of the same area may be
much cheaper to read. Each subscriber also adds reads, subject to cache reuse, while sharing
memory bandwidth with acquisition and the full-frame saver. Increasing worker counts helps
only while those resources have capacity.

The generic datatype reuses the same prepared geometry for packing. Its callback writes a
packed representation into UCX-provided storage, so include internal packing traffic in the
comparison. Explicit packing into reusable registered storage adds source reads, staging
writes and subsequent send reads. With several identical subscriptions, packing once per frame
and sharing the result could offset those costs. That is a benchmark candidate. Such a buffer
would remain held until all sends using it complete.

Direct IOV sends also extend the lifetime of the source frame. Today `slot.sends` counts all
sends using a source slot, and the slot becomes reusable only when that count reaches zero.
A slow ROI transfer can therefore retain a full camera buffer after the saver's send has
finished. Once the source ring has no free slots, acquisition cannot obtain another buffer.
An every subscription also gates new source grants through its receive credits. Latest
currently limits each session to one outstanding transfer, bounding how many source slots
that session can retain. Several sessions and shared progress, memory and transport resources
can still affect acquisition and the saver.

For optional ROI consumers, compare direct sends with a bounded registered staging ring.
Packing into that ring releases the ROI's claim on the source once extraction completes;
the network transfer then holds the staging buffer. If its staging capacity is exhausted,
skip an optional latest delivery. Keep extraction work bounded so it also leaves progress
time for required sends. This design trades additional memory traffic for shorter source
retention. Measure it before choosing the rectangle path. Required every subscriptions retain
their completeness and backpressure semantics. Separate workers could reduce progress
contention, while memory bandwidth and the network link would still be shared.

Source lifetime and receive credit have separate completion points. `Pub::on_send` releases a
send's source hold at UCX completion. An application may keep processing its received copy
after that. Its receive slots remain occupied until it releases the Batch, after which the
subscriber advances its cumulative credit. Returning receive credit at transfer completion
would allow those slots to be overwritten during application access. An every subscriber
whose receive ring is full can gate future source grants even when source slots are free.
For latest, exhausted receive capacity causes skipped deliveries. ROI processing time belongs
to this receive-credit lifetime; source retention covers the outstanding transfer.

Lima2's masked ROI benchmark motivates measuring memory traffic and traversal order. Its
follow-up combines static masks ahead of processing and traverses wide profiles in row order
with column accumulators. Those processing results inform our experiment. Our UCX transfer
paths need their own measurements. [Lima2 ROI benchmark discussion](https://gitlab.esrf.fr/limagroup/lima2/-/issues/452#note_578812)

## 4. How detectors and cameras map

| Source | Parts | A selection | Runs |
|---|---|---|---|
| Threshold detector, compressed (as DECTRIS Stream V2) | `threshold_1`, `threshold_2`, `difference`: bytes | whole parts | one per part |
| The same, uncompressed | three array parts `(y, x)` | part and rectangle | one per row |
| Many counters in one array (as Medipix3 spectroscopic mode, 4–8 counters) | one array `(counter, y, x)` | counters and rectangle | one per row |
| Storage cells in a train (as AGIPD, JUNGFRAU at XFELs) | one array `(cell, 2, y, x)`, data and gain; cell and pulse IDs follow the cell axis | cells, gain stage, rectangle | one per row |
| Spectroscopic pixel detector, histogrammed (as HEXITEC) | one array `(y, x, energy)` | energy window and rectangle | packed |
| Multi-channel analyser for fluorescence (as Xspress3) | one array `(channel, energy)` | channels and energy window | one per channel |
| 4D-STEM, ptychography | one array `(ky, kx)`; the scan position in the record | detector region | one per row |
| Colour camera, Bayer | one array `(y, x)`, pattern RGGB | colour and rectangle | packed |
| Colour camera, RGB | one array `(y, x, colour)` | colours and rectangle | packed |
| Packed pixel format (as Mono12p) | bytes `mono12p`, decoding to `u16 (y, x)` | whole part | one |
| Event detector, raw (as Timepix3 behind a Cheetah T3) | bytes `tpx3`, one part per chip | chips | one per part |
| Event detector, decoded | one array `events (n ≤ max)` of `{x, y, toa, tot}` | fields | packed |
| Several sensors behind a prism | one array part per sensor, each its own shape and type | part and rectangle | one per row |
| Filter wheel | one array part; the filter in the record | frames by a record field | after 0.1 |

[research/detectors.md](research/detectors.md) checks these against sources and lists the gaps:
data sent once per series (flat fields, pixel masks, energies per threshold), sub-byte depths
(1, 4, 6, 12 bits) as bytes encodings, encodings with parameters, and module geometry.

Multispectral cameras use the same three orders, under the names BSQ `(band, y, x)`, BIL
`(y, band, x)` and BIP `(y, x, band)`.

A difference image is computed by the detector: it is one more part the device publishes.
DECTRIS's Stream V2 keys channel data, threshold energy, flat field and pixel mask by channel
name, and carries all channels of an exposure in one message
([stream_v2](https://github.com/dectris/documentation/blob/main/stream_v2/README.md)); parts
keep that convention and add selection by each subscriber.

An event detector has no frames. Its device cuts the stream into chunks, each ending on a
packet boundary so it decodes alone, and states in the record the chunk's time range, its hit
and TDC counts. Packets within a chunk are not in time order. A saver takes every chunk, and
pullers share the clustering. Images integrated from the events are a second stream, not a
part, since they come at their own rate: `install_ucx_commands` takes a prefix, so one device
can offer `RawOpen` and `ImageOpen`. A region of interest over raw events is a filter on their
content, not on memory; it comes after 0.1, and until then a subscriber takes a chip or the
image stream.

## 5. Choices still open

1. Bytes parts in 0.1, or array parts first. Recommended: in 0.1, since compressed thresholds,
   packed pixel formats and raw events all need them.
2. Interleaved and mosaic layouts in 0.1 through the pack callback, or planar layouts only at
   first. Recommended: planar is the zero-copy path, packing covers the rest.
3. The send mechanism for rows. Compare reusable IOV layouts, the generic datatype and
   explicit packing, following [Fixed ROIs and memory traffic](#fixed-rois-and-memory-traffic).
   Measure wide and narrow rectangles of equal area, segment counts and subscriber counts,
   including identical and overlapping selections. Keep a full-frame saver active and use
   source rings large enough to expose memory pressure. Compare full-frame delivery followed
   by a consumer crop. Record setup cost separately from steady-state throughput, CPU use and
   memory traffic where measurable. Verify the received bytes, the actual UCX protocol and
   copying behaviour, GPU support, source-buffer hold times and acquisition and saver
   throughput before choosing. Include a slow or stalled optional ROI consumer, check the
   number of source slots it retains, and compare bounded staging with direct sends.
4. Arrays whose first axis varies, of structured elements, in 0.1. Recommended: in 0.1; it lets
   decoded events be described rather than sent as bytes.
5. Pullers that never lose frames (`TODO(pull-loss)`) in 0.1. Recommended: in 0.1; event data
   is processed by pullers, and a lost chunk is lost hits.

## 6. Order of the work

| | Work | Estimate |
|---|---|---|
| 1 | Adapt the Basler plugin (markers, `begin`) and rerun its end-to-end test | ½ day |
| 2 | Start fence: the count of ready every sessions; Basler's `startAcq` waits on it | ½–1 day |
| 2b | Lent ring: Basler grabs into registered memory through Pylon's buffer factory, with no copy for subscribers; video and live mode publish the camera's own pixel format | done |
| 3 | Benchmark reusable IOV layouts, generic packing and explicit packing under a full-frame saver load (choice 3) | 1 day |
| 4 | Parts in the description, the slot and Frame; discovery in `UcxStream` | 2–3 days |
| 5 | Varying first axis and structured elements (choice 4) | 1 day |
| 6 | Selections and the grant: whole parts, bands, rectangles, colours, fields, records only | 3–4 days |
| 7 | Pullers that never lose frames (choice 5) | 1–2 days |
| 8 | Multi-layer and multi-part frames from the Basler plugin; a synthetic beam and a synthetic event source | 2–3 days |
| 9 | Release: a stored-document wire test, the compatibility policy, an `experimental` area | 1 day |

About 13–18 working days. A test over random selections checks the contract: the bytes received
are the selection of what was published.

## 7. After 0.1

- Emulator test mode in the Basler plugin; its failed-buffer branch.
- A beam pipeline demonstration checked against truth.
- A long-lived `Subscriber` with one receive arena.
- A saver that follows the device across acquisitions, with its backlog in Health.
- Overlapping instances: arena regions, quarantine per region, a budget policy, health per
  instance.
- Alternating saver groups.
- Membership for the start fence, so no device holds a count: a saver keeps a member endpoint
  across instances, and the fence waits for every member. Liveness comes from UCX's peer
  errors, as a ZooKeeper ephemeral node lives with its session, so there is no lease. The same
  endpoint can tell a saver that an instance began, and carry its group. It reverses part of
  Cut 3 and needs an ADR. `ucx_start(every, deadline)` then gains a form without the count.
- Frames selected by a record field, such as a time range or a scan region, and every Nth
  frame.
- Filters on content for events: a region of interest over raw hits, decoded by the publisher.
- Meaning of bits within an element, such as a gain stage in the top bits of a pixel.

## 8. Known edges

- **Pullers can lose frames when one leaves** (`TODO(pull-loss)` in `src/transport/publisher.cpp`);
  choice 5.
- **The arena's NUMA node** is wherever its helper thread runs (`TODO(numa)` in
  `src/transport/engine.cpp`).
- **Event detectors at their full rate** have not been measured through the packed ring.
- **A lent ring on a real camera** has only run on Pylon's emulator: not Pylon's GigE and USB
  drivers grabbing into up to a thousand registered buffers, Basler's blank frames for missed
  block IDs (the emulator has none), or a packed or YUV format as opaque bytes.
- **Pressure with a lent ring** holds Pylon's buffers. Once its queue is empty the camera's
  frames are lost, and Basler publishes them as blank frames, where a copy into the ring would
  have absorbed the burst.
- **Colour in video mode** is a plain array for now: Bayer as `(y, x)` and RGB as `(y, x, 3)`,
  with the pixel format in the application text. The mosaic and colour axes wait for parts.
