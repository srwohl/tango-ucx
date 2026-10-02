# Next: detector-native opaque payloads

Status: **built on 2026-10-02, as phase 4; in the working tree, not committed**. The plan
below is kept as it was written on 2026-10-01. What was built, what was decided on the way and
what is still open are in "As built". Terms are those of [CONTEXT.md](CONTEXT.md).

The starting plan is section 11.1 of the specification,
`../tango-bulk/prototypes/final_shape/fixed_tables_v2.md`. This note departs from it in one
main way: the receive ring is packed, where section 11.1 keeps every slot at the maximum size.

## What it is

A detector may deliver frames that are already compressed, with a different length for each
frame. The library must carry those bytes unchanged, with each frame's record, and must not
treat the payload as a dense array. Today every frame of a UCX stream has one fixed size,
taken from the Description's element and shape.

## Before starting: close out phase 3

Done on 2026-10-02. `budget.toml` is at phase 4 with an 11,000-line ceiling: the phase 3
allowances were raised to the measured counts, and the phase 4 allowances are this note's
estimates split by file.

1. Commit the work in the tree: `ucx_start` and `Health::started`, the `max_wait` unit fix
   in the Python binding, and Python GPU receive.
2. Make `pixi run count` pass: `budget.toml` still says `active_phase = 2`, the GPU test files
   have no allowance, and several per-file allowances are exceeded.
3. Restore the test "an every subscription gone before its first frame fails that frame's
   send" in `tests/transport/test_failure.cpp`, or record why it was removed. It passes on the
   current code.
4. Bring the specification up to date: `ucx_start` and the `started` status replace
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
| Reading a frame | `Batch::payload(i)` gives one frame's bytes; the batch stays the view that holds the memory | No per-frame owning type is needed |
| `Batch::payload()` and Python `Batch.array` on an opaque batch | Refused | There is no packed array of equal frames to return |
| GPU subscription to an opaque stream | Refused when the subscription is planned | DLPack's array export has no meaning for it; no silent use of host memory |
| A frame of zero bytes, or over the maximum | Refused at `publish_bytes`; the slot stays held | Section 11.1 |

## As built

Every step of the build order is done, in the order planned. On 2026-10-02, after the change
described in "Header order", the default run (81 tests), the ASan run (81), the TSan run (80)
and the GPU run (12) passed, each built from the tree as it is now. The ASan build also reports
undefined behaviour without failing a test: run with `UBSAN_OPTIONS=halt_on_error=1`, it fails
`test_archive`, where `examples/archive.h` reads a chunk's table at an address that is not
8-byte aligned. That is not fixed.

### Decisions taken while building

| Decision | Choice | Why |
|---|---|---|
| Header order (open question 1) | The publisher places each frame and states its position in the Frame header. The subscriber checks the position against the frames it holds and takes nothing from the order of arrival | Headers do not always arrive in send order: see "Header order" below |
| Wire version (open question 2) | Minor: 3.1 | An array stream stays readable by a 3.0 subscriber. An opaque UcxStream has `max_payload_bytes` and no `shape`, so a 3.0 subscriber refuses it before Open. Change `k_major` instead if no 3.0 subscriber is deployed and a clean break is wanted |
| Where a length lives (open question 6) | In a table beside the records | The record stays what the device published, at the same stride. Python reads `len(batch.payload(i))` |
| Open | An optional `ring_bytes` key, sent for an opaque stream only | The publisher needs the ring's size for byte Credit. An opaque publisher refuses an Open whose ring is under two frames of the maximum |
| Credit | 24 bytes for a packed ring: session, frames, bytes. An array stream's stays 16 | `bytes` is where the last credited frame ends, counted from the session's first byte |
| Room for a grant | The maximum for the grant, for each grant outstanding, and once more | A frame that wraps skips less than one maximum, and the frames a ring holds wrap at most once. The simulation test checks it against random lengths and releases |
| Ring size | The budget less the tables. The tables hold `ring / (max / 256)` frames, within an eighth of the budget and 65,536 | A table row costs about 450 bytes, so frames far smaller than 1/256 of the maximum are bounded by rows, not bytes |
| Smallest ring | Three frames of the maximum | One held, one arriving, and the wrap |
| `Slot::publish()` on an opaque stream, `publish_bytes` on an array stream | Refused; the slot stays held | `publish()` would send the whole slot |
| `Batch::payload(i)` on an array batch | Allowed; refused on a GPU batch | The same archiver code reads both; a span is host memory |
| A batch of a packed ring | Cut where the next frame starts at the ring's front | It keeps a batch one run of bytes, so the archive example writes it with one `writev` and no gather |

### Header order

Changed on 2026-10-02, after a review. As first built, the subscriber placed each frame where
the one before it ended, in the order headers arrived, and failed the subscription on a frame
that was not the next sequence.

- **UCX does not keep the order.** In UCX 1.22 (`src/ucp/core/ucp_am.c`) eager messages of
  several fragments are delivered in order among themselves, but a message of one packet, or a
  rendezvous request, is delivered when it arrives, also while an earlier message is still in
  fragments. The order holds only while all fragments travel on one lane. `ucp.h` promises
  nothing.
- **Reproduced on this host.** With `UCX_TLS=tcp UCX_MAX_EAGER_RAILS=2` and two network devices,
  an every subscription to an opaque stream failed within 4 to 33 frames of mixed sizes. An
  array stream delivered 60,000: its slot comes from the sequence in the header. The first
  header-order test did not see it: it ran on one lane and filled each frame byte by byte.
- **The Frame header of a packed ring is 8 bytes longer**: the frame's position, counted from
  the session's first byte, before the record. An array stream's header is unchanged.
- **What the subscriber checks** (`Sub::fits`): the frame is within the bytes Credit returned
  and whole before the ring's end; it lies after every frame of a lower sequence that has
  arrived and before every one of a higher, leaving at least a byte for each frame between
  them that has not; next to a neighbour it is exactly where the placement rule puts it. A run
  of more than 1,024 frames not yet arrived is refused.
- **Measured after the change**: 200,000 frames of 8 bytes to 1 MiB on two eager rails, every
  index, length, record field and first payload bytes as sent.

### Open questions, as they stand

1. Answered: no. See "Header order".
2. Decided as a minor version. The owner knows whether 3.0 subscribers exist.
3. **Still open.** The library refuses a frame over the maximum and leaves the slot with the
   device. What the detector should then do is not decided.
4. **Half done.** A frame generated with `bitshuffle` (`bs32-lz4<` and `bs16-lz4<`, with the
   12-byte header) decodes from its record fields alone, in `python/tests/test_opaque.py`. A
   captured SIMPLON frame still has to be supplied.
5. Measured over shared memory and TCP: registration neither pins nor touches a slot. Resident
   memory grows only with the bytes written: a publisher with 512 MiB of 9 MiB slots had
   about 28 MiB resident before its first frame. **Not measured over RDMA**, where
   registration pins.
6. Decided: beside the records.

### Not done

- **A byte target for a batch.** Section 11.1 asks for batches bounded by bytes, frames and
  delay. A batch is bounded by frames, by `max_wait` and by the ring; a byte target would be a
  new subscription option and a new public name.
- **The benchmark on the target file system.** `test_archive "[.benchmark]"` ran here on ext4
  over NVMe, mostly into the page cache: about 1.0 to 1.7 GiB/s for the write and 2.7 to
  3.5 GiB/s for the gather copy. Set `TANGO_UCX_ARCHIVE_DIR` to run it on GPFS.
- **The specification.** Section 11.1 still says a new major version, a `PayloadKind` tag, a
  `FrameView` and slots of the maximum on both sides.
- **The example is not built by this tree.** `examples/` is a separate consumer of the
  installed package, and the package's CMake config is not installed yet. `archive.cpp` was
  checked for syntax only; `archive.h` is built and tested through `test_archive`.

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
   out of the UcxStream document for an opaque stream is also safe: an old subscriber is
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
   `Batch::payload(i)`, the UcxStream keys. One test publishes frames of 1 byte, an eager size, a
   rendezvous size and the maximum, and compares every byte and record. Estimated at 80
   library lines. **Count here** and compare with the estimate before going on.
2. **The header-order test** of open question 1.
3. **The packed receive ring**: the table, byte Credit with the reservation at the grant, the
   wrap rule. Tests: a held batch while frames of other lengths arrive, pressure, a latest
   subscription skipping, End, close with a held batch, a publisher that sends more bytes than
   were credited.
4. **Wire**: the version decision, the golden UcxStream document, the refusals.
5. **Python**: `Batch.payload`, the refusal of `array`, the description key.
6. **The DECTRIS check**, in Python.
7. **Archive batches**: an example that writes batches of 10 and 100 frames as one byte blob
   with offsets, lengths and records, and a benchmark of the write.
8. `Batch::copy()` for an opaque batch, the CONTEXT.md terms, the name lists, a phase 4 in
   `budget.toml` and `tools/count.py`.

## Acceptance

The five criteria of section 11.1 stand. Add one for the packed ring: with a 256 MiB budget
and frames of about 300 KiB under a 9 MiB maximum, the subscription holds several hundred
frames, and no frame is overwritten while a batch that contains it is held.

As built: criteria 1, 3 and 5 are met. Criterion 2 is met for a generated frame, a frame at
the maximum and a frame over it, and waits for DECTRIS reference data. Criterion 4 is met for
batches of 10 and 100 frames under a frame count and a delay, with no byte target, and the
copy and the write are measured apart. The packed ring's criterion is met: a 256 MiB budget
gives a ring of 252.9 MiB that held 804 frames, each checked byte for byte while held.

## Estimates

All estimated by the scoping study at the density of the current code, each within about 40%.

| Part | Library | Tests | Measured library | Measured tests |
|---|---:|---:|---:|---:|
| Step 1, host C++ | 80 | 160 | 75 | 130 |
| Full feature with slots at the maximum size, with Python and the DECTRIS check | 116 | 370 | | |
| The packed receive ring, in addition | 200 to 250, not scoped | not scoped | | |
| All of phase 4 | 316 to 366 | 370 and more | 237 | 974 |

The library is 5,783 lines and the tests 5,114, 10,897 together under the 11,000 ceiling. The
position in the Frame header and its checks are 53 of the library lines and 75 of the tests. The
packed ring cost less library than estimated because it replaced the placement of step 1
rather than adding to it. The tests cost more: the packed ring's were not scoped.

The full feature needs at least three new C++ names and one new Python name. Both name lists
were at their limits (76 and 31) and are now 78 and 32: `Description::max_payload_bytes`,
`Slot::publish_bytes`, the `Batch::payload(i)` overload (no new name), and Python's `Batch.payload`.
