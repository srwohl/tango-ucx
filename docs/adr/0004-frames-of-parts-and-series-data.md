<!--
SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-ucx project

SPDX-License-Identifier: LGPL-3.0-or-later
-->

# Frames of parts, with series data

Status: **proposed**

## Context

A description today is one element type and one shape, or a maximum number of bytes, with
fixed-size application fields and untyped application text. A detector that produces more than
one image per exposure, data that holds for a whole series, or results that come at the end
cannot say so. It can concatenate everything into one opaque frame, publish one stream per
image, or put what is left in application text. Each loses something a consumer needs: part
selection, frames that arrive together, or data a program can read without the device's
documentation.

DECTRIS's EIGER2 and PILATUS4 are the first case, and the hardest of the common ones. Each
exposure has up to two (EIGER2) or four thresholds and a difference image, each compressed
alone. Each threshold has its own energy, flat field and pixel mask, and the difference
channel's are of another shape: two energies, a mask that is the OR of both, no flat field.
The sources (`docs/research/threshold-detectors.md`, `docs/research/detectors.md`) show four
things the state of the art does not do:

- **Series data reaches one consumer.** Stream V2's start message goes through ZeroMQ PUSH,
  which round-robins it like any image, so with two consumers one of them never sees the flat
  field or mask. Jungfraujoch, Lima2 and Diamond's EigerFan each work around it differently.
- **Consumers drop series data.** ADEiger and the MAX IV receiver keep only the energies; Lima2
  logs the flat field and mask and keeps neither.
- **Nothing says what a pixel value means.** A masked pixel is 2^bits − 1, or 0 with
  `mask_to_zero`; whether the flat field was already applied is a separate setting. A consumer
  that has not read the manual cannot tell a masked pixel from a count.
- **One encoding name means two byte formats.** DECTRIS's `bslz4` has a 12-byte HDF5 header,
  areaDetector's has none.

The same patterns come from other detectors: JUNGFRAU and AGIPD calibration per storage cell
and gain stage, Jungfraujoch's ROI sums and azimuthal profiles with the maps that define them,
and run results at the end of a series.

The description is frozen at 1.0. A pattern it cannot express then needs a break; a delivery
feature added later does not. So this ADR decides what a description can express, all of it
from 0.1, and which parts the library delivers in 0.1. How parts are sent and received is
ADR 0005.

## The patterns

| Pattern | Example | Expressed as |
|---|---|---|
| Several images per exposure | EIGER2 `threshold_1`, `threshold_2`, `difference` | frame parts |
| An image compressed alone | Stream V2 `bslz4`, per channel | a bytes part with an encoding |
| Uncompressed images, stacked or not | `(threshold, y, x)`, or one part per threshold | an array part, or several |
| Data for the whole series, per image | energy, flat field, mask per threshold | series parts and series fields, each linked to its frame part |
| Series data of another shape for one image | the difference channel's two energies and OR-ed mask | each part has its own series data |
| Series data along an axis | JUNGFRAU pedestal and gain per storage cell | a series part linked to an axis |
| Per-frame results with a legend | ROI sums with `roi_map`, azimuthal profile with `az_int_map` | a frame part with a linked series part |
| What a pixel value means | no-data, saturation, flat field applied, mask applied | named series fields |
| Results at the end of a series | Jungfraujoch run results; HDF5 data written at close | end parts |
| Series data that changes mid-series | NXmx's per-frame mask `(nP, i, j)`; Jungfraujoch calibration messages | revisions |
| Variable-length structured results | spots, reflections | an array part with a varying first axis of structured elements |
| Per-frame data of any size | Stream V2 `image_appendix` | a bytes part with encoding `json` or `cbor` |
| Per-series data of any size | Stream V2 `header_appendix` | a series bytes part; application text when it is text |
| Sub-byte and packed depths | Medipix3 1/6/12 bits, Mono12p, SELUN 8-bit floats | a bytes part whose encoding decodes to an array |
| The detector's own numbering | `image_id`, `original_image_id`, `series_id` | record fields; the frame index stays dense |
| Rational times on a series time base | Stream V2 `start_time`, `real_time` | record fields, the time base a series field |
| Bit depth chosen per series | EIGER2 8, 16 or 32 bits | a new instance |
| Event streams | Timepix3 chunks | bytes parts, one per chip; images from them are another stream |

## Decision

**A frame is a list of named parts with one record.** A part is an **array** (element, named
axes with sizes and optional labels, a first axis that may vary per frame up to a maximum, an
element that may be a structure of named fields, an optional mosaic pattern on a pair of axes)
or **bytes** up to a maximum, with an encoding. Today's array stream is a frame with one array
part, today's opaque stream a frame with one bytes part. This is roadmap section 2, kept.

**Each part has a cadence:**

- **frame**: published with every frame, in its slot;
- **series**: published once by the device before `ucx_start`, held by the publisher outside
  the source ring for the instance's life, and sent to each session that selected it before
  that session's first frame, whenever the session joins;
- **end**: published by the device at finish, and sent to each session that selected it before
  its End.

Every delivery gets series and end parts per session: each puller of a set, each every
session, each latest session. A late joiner gets the current series data. Series and end
parts take no room in a receive ring for frames.

**A part may describe another part, or one axis of it.** `flatfield_threshold_2` describes
`threshold_2`; a pedestal describes `module_data` along its `cell` axis; `roi_map` describes
`roi_sums`. A part says what it describes, and the description lists it under that part, so a
subscriber finds a threshold's mask by rule, not by name. **Selecting a part selects what
describes it,** unless the subscriber leaves those out by name. A consumer that silently lacks
the mask is the failure the sources keep showing; a viewer that does not want a flat field
says so.

**Series fields are typed, per part.** Beside series parts, which are arrays or bytes, each
part may carry series fields: a structure with a NumPy dtype, as application fields have, sent
with the series data. A difference part's `threshold_energy` is `(2,) f8` where a threshold's
is `f8`. A few names are the library's, so every consumer can rely on them:

| Field | Meaning |
|---|---|
| `no_data_value` | the value of a pixel with no data: masked, in a gap, or defective |
| `saturation_value` | the first value that means saturated |
| `flatfield_applied` | the detector applied the flat field |
| `mask_applied` | masked pixels carry `no_data_value` |

The rest are the device's. A pixel mask uses NXmx's bit meanings when its part says so.

**Encodings name their framing.** An encoding is a name and its parameters (such as
`element_size` for bitshuffle): `bslz4-h5` and `lz4-h5` are the HDF5-filter forms DECTRIS sends,
`bslz4` and `lz4` the bare ones. An encoding may name the array it decodes to. The names live
in a registry checked as the wire is (`tools/wire.txt`).

**Series data has revisions.** All series data of an instance has one revision number, 0 when
the instance starts; each frame's record states the revision it was produced under. When the
library supports updates:

- an update takes effect from the next slot the device acquires; frames already acquired keep
  the revision they had;
- a session receives the new revision of what it selected before the first frame that uses it;
- a subscriber keeps a revision until no unreleased batch refers to it, and the publisher until
  its last send has completed;
- a latest session never receives a frame whose revision it has not received.

**Record fields may follow an axis or belong to a part** (`field_axes`, roadmap section 2), as
cell and pulse IDs follow a storage-cell axis.

**What 0.1 delivers.** The description and the wire express all of the above. The library
delivers in 0.1: parts of every cadence, whole-part selection with the parts that describe
them, records only, series fields and the library's names, encodings, and the revision in every
record, always 0. Updates mid-series come after 0.1 and need no wire change. Axis selections
and rectangles follow roadmap section 3.

**What stays out.**

- A schema for geometry and modules: a series part can carry it, with no meaning the library
  knows.
- A subscriber reporting errors back to the device, such as a full disk: a protocol, not a
  frame.
- Frames selected by a record field, and every Nth frame: after 0.1, roadmap section 7.

## Consequences

- **A DECTRIS device publishes one stream** with its thresholds, difference, energies, flat
  fields and masks. Every consumer of it gets the series data it selected, however many
  consumers there are and whenever they join.
- **A Stream V2 bridge is a mapping**, not a reinterpretation: a channel is a frame part, a
  channel's start-message entries are its series parts and fields, `header_appendix` and
  `image_appendix` are a series and a frame bytes part. `header_detail` becomes each
  subscriber's choice.
- **A frame's description grows** from an element and a shape to a list of parts, each with
  axes, a cadence, links and fields. `UcxStream` shows all of it, and its JSON is wire.
- **The record gains the revision,** four bytes per frame.
- **The publisher holds series data** for the instance's life: for EIGER2 16M with two
  thresholds (4148 × 4362 pixels), a float32 flat field and a uint32 mask per threshold plus the
  difference's mask come to about 345 MiB of registered memory beside the arena. It counts
  against the publisher's budget, and each subscriber holds what it selected, against its own.
- **Devices must declare** what a value means and which data belongs to which image. A device
  that does not still works: no links and no fields are valid.

## Considered

- **One stream per threshold.** Partial subscription for free, but three instances, rings and
  start fences; nothing makes a frame's thresholds arrive together, and a saver joins them by
  frame index.
- **Series data through Tango attributes.** Read once, so speed does not matter, but nothing
  ties a value to the instance, and every device and consumer pairs them by hand.
- **Series data in every frame.** Correct for late joiners, but a flat field and mask per
  threshold in each slot costs several times the frame's size in ring memory.
- **A self-describing message per frame, as Stream V2's CBOR.** No description to freeze, but
  every frame is parsed, rings cannot be planned from the description, and series data has no
  place but the start message, whose delivery is the problem.
- **NXmx as the model.** Its vocabulary for masks and detector channels is used where it fits;
  its model is a file's, with no cadence, no selection and no delivery.
- **Untyped series metadata, a key-value map.** Easy to add, but it is application text again:
  a consumer still needs the device's manual to find the no-data value.
- **A subscriber-chosen cadence** ("only the first frame of this part"). A late joiner misses the
  first frame, and only the device knows which data holds for the series.

## Open

1. Glossary: CONTEXT.md lists "generation" as an _Avoid_ term for the instance ID, so this ADR
   says **revision**. **Part**, **cadence**, **series part**, **end part**, **series field** and
   **describes** need entries.
2. One revision for all series data, or one per part. One is simpler to check per frame; per
   part avoids resending a flat field when only the mask changes.
3. Whether a series part can be a frame part's legend and also describe an axis of another
   part, or one link per part only.
4. The difference channel's two energies in Stream V2 (`threshold_energy` is a map from name to
   a float), and whether it has a flat field: not settled by the sources.
