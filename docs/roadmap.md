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

A frame is a list of named **parts**, with one record shared by all of them. A part is either:

- an **array**: an element type and named axes, each with a size and optional labels, laid out
  in axis order; or
- **bytes** up to a maximum, with the name of their encoding (such as `bslz4`) and the array
  they decode to.

Today's array stream is a frame with one array part, and today's opaque stream a frame with one
bytes part. Record fields may belong to a part or follow an axis (`field_axes`), as a
detector's threshold energy, flat field and pixel mask belong to each threshold.

Each part has a fixed place in the source slot; a bytes part takes its maximum, as an opaque
slot does today. A Frame message states each part's length. All parts of a frame arrive
together.

## 3. Selections and the grant

A subscriber selects in Open:

- **parts**, by name; all of them when it names none;
- in each array part, for each axis, all of it, a set of indices or labels, or a range
  `[start, stop)` with an optional step;
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

The publisher turns a selection into the runs of contiguous bytes it sends from the slot:

- one run for a whole part, or a band of rows in a planar layout: sent as today, zero-copy;
- one run per row for a rectangle: UCX IOV, or the generic datatype;
- an element gather, for a band of an interleaved or mosaic layout: the generic datatype's pack
  callback, one copy.

`UcxStream` describes what can be selected: the parts, their axes and labels, the kinds of
selection and their limits, and which record fields follow which axis. `UcxSessions` shows each
session's selection and bytes per frame.

## 4. How detectors and cameras map

| Source | Parts | Bands or thresholds are | A selection | Runs |
|---|---|---|---|---|
| Photon-counting detector with thresholds, compressed (as DECTRIS Stream V2) | `threshold_1`, `threshold_2`, `difference`: bytes | part names | whole parts | one per part |
| The same, uncompressed | three array parts `(y, x)` | part names | part and rectangle | one per row |
| Stacked layers, band sequential (BSQ) `(band, y, x)` | one array part | axis labels | band and rectangle | one per row |
| Band interleaved by line (BIL) `(y, band, x)`, pushbroom | one array part | axis labels | band and rectangle | one per row |
| Band interleaved by pixel (BIP) `(y, x, band)`, RGB | one array part | axis labels | band and rectangle | packed |
| Mosaic filter, e.g. 4 × 4 | one array part declared `(y_cell, y_phase, x_cell, x_phase)` over the same memory | labels mapping onto phase pairs | fixed phases and cell ranges | packed |
| Several sensors behind a prism | one array part per sensor, each its own shape and type | part names | part and rectangle | one per row |
| Filter wheel | one array part | a record field | frames by a record field | after 0.1 |

A difference image is computed by the detector: it is one more part the device publishes.
DECTRIS's Stream V2 keys channel data, threshold energy, flat field and pixel mask by channel
name, and carries all channels of an exposure in one message
([stream_v2](https://github.com/dectris/documentation/blob/main/stream_v2/README.md)); parts
keep that convention and add selection by each subscriber.

## 5. Choices still open

1. Bytes parts in 0.1, or array parts first. Recommended: in 0.1, since detectors with
   compressed thresholds are the case that shows the value.
2. Interleaved and mosaic layouts in 0.1 through the pack callback, or planar layouts only at
   first. Recommended: planar is the zero-copy path, packing covers the rest.
3. The send mechanism for rows: IOV or the generic datatype. A measurement at a few rectangle
   sizes on this UCX decides it, and whether rendezvous stays zero-copy with IOV.

## 6. Order of the work

| | Work | Estimate |
|---|---|---|
| 1 | Adapt the Basler plugin (markers, `begin`) and rerun its end-to-end test | ½ day |
| 2 | Start fence: the count of ready every sessions; Basler's `startAcq` waits on it | ½–1 day |
| 3 | Measure IOV against the generic datatype for rows and gathers | 1 day |
| 4 | Parts in the description, the slot and Frame; discovery in `UcxStream` | 2–3 days |
| 5 | Selections and the grant: whole parts, bands, rectangles, records only | 3–4 days |
| 6 | Multi-layer and multi-part frames from the Basler plugin and the synthetic beam | 1–2 days |
| 7 | Release: a stored-document wire test, the compatibility policy, an `experimental` area | 1 day |

About 11–15 working days. A test over random selections checks the contract: the bytes received
are the selection of what was published.

## 7. After 0.1

- Emulator test mode in the Basler plugin; its failed-buffer branch.
- A beam pipeline demonstration checked against truth.
- A long-lived `Subscriber` with one receive arena.
- A saver that follows the device across acquisitions, with its backlog in Health.
- Overlapping instances: arena regions, quarantine per region, a budget policy, health per
  instance.
- Alternating saver groups.
- Frames selected by a record field, and every Nth frame.

## 8. Known edges

- **Pullers can lose frames when one leaves** (`TODO(pull-loss)` in `src/transport/publisher.cpp`).
- **The arena's NUMA node** is wherever its helper thread runs (`TODO(numa)` in
  `src/transport/engine.cpp`).
