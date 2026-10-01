# Next: detector-native opaque payloads

Status: **planned, 2026-10-01; no code**. This is the next piece of work after phases 1 to 3.
It records what a scoping study found and what was decided. Terms are those of
[CONTEXT.md](CONTEXT.md).

The starting plan is section 11.1 of the specification,
`../tango-bulk/prototypes/final_shape/fixed_tables_v2.md`. This note departs from it in one
main way: the receive ring is packed, where section 11.1 keeps every slot at the maximum size.

## What it is

A detector may deliver frames that are already compressed, with a different length for each
frame. The library must carry those bytes unchanged, with each frame's record, and must not
treat the payload as a dense array. Today every frame of a bulk stream has one fixed size,
taken from the Description's element and shape.

## Before starting: close out phase 3

1. Commit the work in the tree: `bulk_start` and `Health::started`, the `max_wait` unit fix
   in the Python binding, and Python GPU receive.
2. Make `pixi run count` pass: `budget.toml` still says `active_phase = 2`, the GPU test files
   have no allowance, and several per-file allowances are exceeded.
3. Restore the test "an every subscription gone before its first frame fails that frame's
   send" in `tests/transport/test_failure.cpp`, or record why it was removed. It passes on the
   current code.
4. Bring the specification up to date: `bulk_start` and the `started` status replace
   `first_grant`; there are 76 C++ names; a GPU view exports DLPack 1.0 only.
5. Run the ASan build once on the current code.

## What the study found

- **The transport already carries a length for each frame.** The Frame message has
  `payload_bytes`, and the subscriber learns it before it chooses where the payload goes, for
  both eager and rendezvous frames (`on_frame` in `src/transport/subscription.cpp`).
- **The fixed size is a convention in a few places:** `sizes_of` in `memory.cpp`, the
  `k * frame_bytes` offsets in `subscription.cpp` and `batch.cpp`, the equality check on
  `payload_bytes` in `on_frame`, and `send_frame` in `publisher.cpp`.
- **With slots at the maximum size, compression gives no depth.** For a 2068 x 2162 16-bit
  frame compressed to about 300 KiB, with a safe maximum of 9 MiB, a 256 MiB subscription has
  a depth of 28 frames, against 30 for the same frames uncompressed. Packed, the same budget
  holds about 870. These figures are computed from the planning formulas, not measured.
- **The pieces do not interact with the start, with every and latest delivery, or with
  cumulative Credit** as long as Credit counts frames. A packed ring changes that last part.
- **There is no DECTRIS reference frame and no decoder** in this repository or in
  `../tango-bulk`.

## Decisions taken

| Decision | Choice | Why |
|---|---|---|
| Receive ring | Packed: each frame is placed directly after the one before | A depth of 28 is too small for an archiver, and a packed batch is already the byte blob an archiver writes |
| Order of work | Maximum-size slots first, then the packed ring | The first step proves unequal lengths end to end; the second replaces only placement and Credit |
| Line ceiling | Not the constraint for this work | The owner's decision. Count after the first step all the same |
| Source ring | Slots stay at the maximum size | The device fills a slot before it knows the length, and the source ring covers only frames in flight |
| How a stream is marked opaque | A nonzero `Description::max_payload_bytes`, with an empty shape | The shape keeps meaning array extents; no extra enumeration |
| Publishing a length | A separate `Slot::publish_bytes(n)` | An overload of `publish` would take today's `publish(timestamp)` calls and send that many bytes |
| Reading a frame | `Batch::frame(i)` gives one frame's bytes; the batch stays the view that holds the memory | No per-frame owning type is needed |
| `Batch::payload()` and Python `Batch.array` on an opaque batch | Refused | There is no packed array of equal frames to return |
| GPU subscription to an opaque stream | Refused when the subscription is planned | DLPack's array export has no meaning for it; no silent use of host memory |
| A frame of zero bytes, or over the maximum | Refused at `publish_bytes`; the slot stays held | Section 11.1 |

## The packed receive ring

The subscriber keeps one byte ring. When a frame's header arrives it places the payload at
the ring's write position and advances by the frame's length.

- **An offset and length for each frame**, in a table beside the records. Frame `k` is found
  through the table, not by multiplying.
- **Credit counts bytes as well as frames.** The publisher grants a slot before the device
  knows the length, so it reserves the maximum at the grant and returns the difference when
  the slot is published. Pressure is then pessimistic only by the grants outstanding.
- **A wrap rule.** A frame never straddles the end of the ring. One that does not fit starts
  at the front, and both sides account for the bytes skipped at the end.
- **A batch is one run of bytes** with its offsets, lengths and records, when frames are
  placed in frame order.

This is new logic in the code that keeps a publisher from overwriting memory a subscriber
still holds. It needs the Credit simulation and the failure tests the existing rules have.

## Open questions

1. **Do frame headers arrive in send order when eager and rendezvous frames mix on one
   endpoint?** If yes, a batch is packed in frame order. If not, frames are placed in arrival
   order and only the table gives their order. Answer this with a small UCX test before the
   packed ring is designed in detail.
2. **Wire version.** Section 11.1 asks for a new major version, which refuses every
   subscriber of the old version, on array streams too. A minor version that leaves `shape`
   out of the BulkStream document for an opaque stream is also safe: an old subscriber is
   refused before Open. Which one depends on whether subscribers of version 3 are deployed.
3. **A frame over the maximum.** What the device should do with it is a detector question.
4. **DECTRIS evidence.** A frame generated in the test with a `bitshuffle` package proves the
   record fields are enough to decode. A captured SIMPLON frame has to be supplied.
5. **Does registration pin the unused part of a slot?** Not measured for any transport. It
   decides what the first step costs in physical memory.
6. **Where a frame's length lives.** Beside the records, or as a field of the record so that
   Python sees a column of lengths. The second changes the record stride for opaque streams.

## Build order

1. **Unequal lengths with slots at the maximum size**, host memory, C++ only:
   `max_payload_bytes`, `publish_bytes`, the length in `send_frame` and `on_frame`,
   `Batch::frame`, the BulkStream keys. One test publishes frames of 1 byte, an eager size, a
   rendezvous size and the maximum, and compares every byte and record. Estimated at 80
   library lines. **Count here** and compare with the estimate before going on.
2. **The header-order test** of open question 1.
3. **The packed receive ring**: the table, byte Credit with the reservation at the grant, the
   wrap rule. Tests: a held batch while frames of other lengths arrive, pressure, a latest
   subscription skipping, End, close with a held batch, a publisher that sends more bytes than
   were credited.
4. **Wire**: the version decision, the golden BulkStream document, the refusals.
5. **Python**: `Batch.frame`, the refusal of `array`, the description key.
6. **The DECTRIS check**, in Python.
7. **Archive batches**: an example that writes batches of 10 and 100 frames as one byte blob
   with offsets, lengths and records, and a benchmark of the write.
8. `Batch::copy()` for an opaque batch, the CONTEXT.md terms, the name lists, a phase 4 in
   `budget.toml` and `tools/count.py`.

## Acceptance

The five criteria of section 11.1 stand. Add one for the packed ring: with a 256 MiB budget
and frames of about 300 KiB under a 9 MiB maximum, the subscription holds several hundred
frames, and no frame is overwritten while a batch that contains it is held.

## Estimates

All estimated by the scoping study at the density of the current code, each within about 40%.

| Part | Library | Tests |
|---|---:|---:|
| Step 1, host C++ | 80 | 160 |
| Full feature with slots at the maximum size, with Python and the DECTRIS check | 116 | 370 |
| The packed receive ring, in addition | 200 to 250, not scoped | not scoped |

The full feature needs at least three new C++ names and one new Python name. Both name lists
are at their limits (76 and 31).
