<!--
SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-ucx project

SPDX-License-Identifier: LGPL-3.0-or-later
-->

# Multi-threshold detectors, series data and their transports

Notes for ADR 0004: frames made of named parts, subscribers selecting whole parts, and series
data sent once per session, with versioned updates. The target is DECTRIS multi-threshold
detectors (EIGER2, PILATUS4). Each claim cites a primary source from the list at the end. A
claim not found there is marked *not stated*. Written 2026-11-22.

## Summary

- DECTRIS Stream V2 sends one start message per series, holding per-channel series data. The
  threshold energy, the flat field (f32) and the 32-bit pixel mask are each keyed by channel
  name. One count-rate table is not keyed by channel. Each image message then holds every
  channel [Stream V2]. The default `header_detail` is `basic`, which leaves out the flat
  fields, masks and table, so a default stream has no per-pixel series data [Stream V2, SIMPLON].
- ZeroMQ PUSH round-robins every message, start and end included. With two consumers, the
  series data reaches one of them [ZMQ, Stream V2]. Every multi-consumer system read works
  around this. Jungfraujoch uses one socket per writer and sends start and end on each
  [JFJ stream]. Lima2 re-publishes the start message to its other receivers [Lima2 dectris].
  Diamond's EigerFan sends the header to every consumer [EigerFan]. Our per-session series
  parts solve this by construction.
- No primary source has an in-series update of a flat field or mask. DECTRIS loads the
  configuration at `arm` [SIMPLON]. NXmx lets the mask carry a frame axis `(nP, i, j)`
  [NXmx, NXdetector_channel]. That is the only precedent for series data that varies within a
  series. Jungfraujoch sends pedestals as separate calibration messages after start, on one
  socket only, without acknowledgement [JFJ stream, JFJ CBOR].
- The consumers read throw away most series data. ADEiger and the MAX IV receiver keep only
  the threshold energies [ADEiger stream2, MAX IV SR]. Lima2 logs flat field and mask and
  drops them [Lima2 dectris]. Jungfraujoch keeps the mask and drops the flat field, and it
  refuses multi-channel images [JFJ deser]. Delivering these arrays in a usable form is open
  ground, not established practice.
- The most concrete misses in our design are these. A series part needs a declared link to
  the frame parts it describes. The difference channel's series data has a different shape.
  Frames need a "masked/no-data" value. End-of-series data has no home. The same encoding
  name, `bslz4`, means two different byte framings in DECTRIS and in areaDetector.

## Checks of our existing claims

| Claim (where) | Verdict |
|---|---|
| Stream V2 keys channel data, threshold energy, flat field and pixel mask by channel name, and sends all channels of an exposure in one message (roadmap §4) | **Confirmed.** The image `data` map follows the order of the start message's `channels` [Stream V2]. The count-rate table is one `TypedArray` and not keyed by channel [Stream V2]. |
| Parts `threshold_1`, `threshold_2`, `difference` (roadmap §4) | **Corrected.** These names are DECTRIS's own [FileWriter, NXdetector_channel]. Stream V2 takes names from `channels` and does not fix them [Stream V2]. SIMPLON has `threshold/n/...` for any n [SIMPLON]. ADEiger handles up to 4 thresholds, "Pilatus4 has 4" (`eigerDetector.cpp:44–45`) [ADEiger]. The DECTRIS sources gathered give no count for PILATUS4. |
| A difference image is one more part the device publishes (roadmap §4) | **Confirmed, with detail.** It is computed by subtracting the upper threshold's counts from the lower's. It needs all thresholds enabled [SIMPLON]. Its series data has a different shape: `threshold_energy` is `float[2]`, the mask is the OR of both masks, and there is no flat field [FileWriter, NXdetector_channel]. |
| Start message carries per-channel energy, flat field and 32-bit mask plus count-rate tables (detectors.md gap 1) | **Corrected.** Flat fields, masks and count-rate tables are sent only with `header_detail = all`. The default is `basic` [Stream V2, SIMPLON]. |
| Tag 56500 with `bslz4` and an element-size modifier (detectors.md) | **Confirmed, with detail.** A second algorithm is `lz4`, with modifier 0. Both use the HDF5 filter framing [tag]. Jungfraujoch adds `bszstd` and `gzip` [JFJ CBOR]. |
| Rational times, time base fixed per series (detectors.md gap 6) | **Confirmed.** `start_time` and `stop_time` are relative to the image's `series_date` [Stream V2]. Jungfraujoch names the stop time `end_time` [JFJ CBOR]. |
| Pull is "named after ZeroMQ's PUSH/PULL" (CONTEXT.md) | **Confirmed, with a difference.** PUSH round-robins each message to one peer, and blocks rather than discards in the mute state [ZMQ]. Series messages are not special: they are round-robined too. Ours are sent per session. |

## DECTRIS Stream V2

| Capability | Detail | Source |
|---|---|---|
| Transport | One CBOR map per ZeroMQ message, PUSH on TCP 31001. Clients use PULL. Several may connect, and each message goes to one client, round-robin. | [Stream V2] |
| Messages | `start` at arm, `image` per exposure, `end` at disarm. | [Stream V2] |
| Series identity | `series_id` (integer) and `series_unique_id` (text), on start, image and end. | [Stream V2] |
| Channels | `channels`: the active channel names, which also give the order of the image data. | [Stream V2] |
| Per-channel series data | `threshold_energy` Map(name → eV). `flatfield` Map(name → MultiDimArray), f32 in the parser. `pixel_mask` Map(name → 32-bit MultiDimArray). | [Stream V2, examples] |
| Series-wide arrays | `countrate_correction_lookup_table` (TypedArray), one per series. | [Stream V2] |
| "Applied" flags | `flatfield_enabled`, `pixel_mask_enabled`, `countrate_correction_enabled`, `virtual_pixel_interpolation_enabled`. | [Stream V2] |
| Scalar series data | Beam centre, count/frame time, detector description, serial and translation, incident energy and wavelength, `number_of_images`, pixel size, `saturation_value`, sensor material and thickness, `arm_date`. | [Stream V2] |
| Element type | `image_dtype` `uint8`/`uint16`/`uint32`, with `image_size_x`/`_y`. Data are RFC 8746 typed arrays. The parser handles u8, u16le, u32le and f32le, row-major (tag 40). | [Stream V2, examples] |
| Goniometer | `goniometer`: Map(axis → {start, increment}). The C parser fixes the axes as chi, kappa, omega, phi, two_theta. | [Stream V2, examples] |
| Per-frame data | `image_id`, `start_time`, `stop_time`, `real_time` (rationals over a fixed time base), `series_date`, `data` Map(name → MultiDimArray). | [Stream V2] |
| User data | `user_data` (Any) on start and image, from SIMPLON `header_appendix` and `image_appendix`. | [Stream V2] |
| Compression | Tag 56500 `[algorithm, modifier, bytes]`, equal to the decompressed byte string. It may appear wherever a byte string may. `bslz4` takes the element size (1, 2, 4) and `lz4` an unused 0. Both use the HDF5 framing, and the parser reads the original size from the first 8 bytes, big-endian. | [tag, examples, bitshuffle] |
| End message | Only `type`, `series_id`, `series_unique_id`. | [Stream V2] |
| Late or second consumer | *Not stated* by DECTRIS. Under ZeroMQ, the start goes to one PULL peer and later images are round-robined [ZMQ]. The DCU's `stream/status/dropped` counts "images that got dropped as not requested", reset at arm [SIMPLON]. | [ZMQ, SIMPLON] |

## DECTRIS Stream V1 (prior art)

| Capability | Detail | Source |
|---|---|---|
| Transport | ZeroMQ PUSH/PULL on 9999, multipart JSON plus binary parts. | [SIMPLON §5.4.3] |
| Series header | `dheader-1.0` with `header_detail`. `basic` and `all` add the detector configuration as JSON. `all` also adds the flat field (`float32`), the pixel mask (`uint32`), the count-rate table (`float32`, shape e.g. `[2,1000]`) and `header_appendix`. One mask and one flat field: no channels. | [SIMPLON §5.4.3] |
| Image | `dimage-1.0` (series, frame, MD5 of the data part), `dimage_d-1.0` (shape, type, encoding `[bs<BIT>][[-]lz4][<\|>]`, size), the data blob, `dconfig-1.0` (start, stop and real time in ns), and an optional `image_appendix`. Its lz4 is raw, "without any additional data like block size". | [SIMPLON §5.4.3] |
| End | `dseries_end-1.0` after disarm, cancel or abort. | [SIMPLON §5.4.3] |

## DECTRIS SIMPLON API 1.8

| Capability | Detail | Source |
|---|---|---|
| Threshold keys | `threshold/n/energy` (energies rising with n), `threshold/n/mode` (enabled or disabled; a disabled threshold reaches no interface), `threshold/n/flatfield` (`float[][]`), `threshold/n/pixel_mask` (`uint[][]`), `threshold/n/number_of_excluded_pixels`. `threshold_energy`, `flatfield` and `pixel_mask` map to threshold 1. | [SIMPLON §5.1.1] |
| Difference | `threshold/difference/mode`, with read-only `lower_threshold` and `upper_threshold` (threshold numbers). | [SIMPLON §5.1.1] |
| Mask bits | 0 gap, 1 dead, 2 under-responding, 3 over-responding, 4 noisy, 5–31 undefined. | [SIMPLON §5.1.1] |
| Corrections | `pixel_mask_applied`: masked pixels are set to 2^bit_depth_image − 1, or 0 with `mask_to_zero`. `flatfield_correction_applied`, `countrate_correction_applied`, `virtual_pixel_correction_applied`. | [SIMPLON §5.1.1] |
| Derived changes | A PUT returns the keys it changed implicitly. Setting `photon_energy` changes `threshold_energy` and `flatfield`, and a custom flat field is lost when the threshold or photon energy changes. | [SIMPLON §5.1.1] |
| Bit depth | `bit_depth_image` is read-only and follows `count_time` (the example PUT lists it). `roi_bit_depth` applies to the lines ROI. `compression` is `lz4` or `bslz4`, with `bslz4` advised. | [SIMPLON §2.2, §5.1.1] |
| Arrays over REST | `darray` JSON: type `<u4` or `<f4`, base64 data, or TIFF for large arrays. | [SIMPLON §5.1.1, §3.2.1] |
| Change mid-series | *Not stated.* `arm` "loads configuration to the detector". Repeating an acquisition means configure, arm, trigger, disarm. | [SIMPLON §3.1, §5.1.3] |
| Monitor | An HTTP buffer of recent exposures (`buffer_size`, `discard_new`, `dropped`). `images/<series>/<id>/<threshold_id>` returns a TIFF. `images/monitor` returns the newest, `images/next` returns FIFO. Each TIFF carries its own series UID, series and image numbers, threshold ID(s) and energy(ies) (2 for difference), exposure time, beam centre and lost-pixel count. Not for rates above 10 Hz. | [SIMPLON §5.2] |
| Stream config | `mode`, `format` (`legacy` or `cbor`), `header_detail`, `header_appendix`, `image_appendix`. | [SIMPLON §5.4.1, Stream V2] |
| Conflict | GitHub: `header_detail = none` is "same as `basic`" [Stream V2]. SIMPLON 1.8: `none` means "No meta data" [SIMPLON §5.4.1]. SIMPLON also describes `image_appendix` as included in *start* messages, where GitHub says image messages. | [Stream V2, SIMPLON] |

## NeXus NXmx and DECTRIS HDF5 files

| Capability | Detail | Source |
|---|---|---|
| Multi-channel data | Format `hdf5 nexus v2024.2 nxmx`: one dataset `/entry/data/data` `[nP, nC, i, j]` (a VDS over the data files), with axes `image_id, channel, ., .`. | [FileWriter] |
| Per-channel series data | `detector/<name>_channel` (NXdetector_channel), holding `threshold_energy`, `flatfield` and `pixel_mask`. For `difference_channel`: `threshold_energy` is `float[2]`, `pixel_mask` = lower OR upper, and there is no flat field. | [FileWriter, NXdetector_channel] |
| Legacy layout | `hdf5 nexus legacy nxmx` has no multi-channel data. `detectorSpecific` (NXcollection) holds one `flatfield` f32[i,j], one `pixel_mask` u32[i,j], `nimages`, `ntrigger`, `countrate_correction_count_cutoff`, and others. `threshold_energy` is in NXdetector. Page for EIGER API 1.5. | [FileWriter, DECTRIS header docs] |
| Per-frame masks | NXmx: `pixel_mask` may be `(i, j)` for the dataset or `(nP, i, j)` per frame. NXdetector_channel: "May include nP as the first dimension for masks that vary for each scan point." | [NXmx, NXdetector_channel] |
| Extra masks | `pixel_mask_N` entries. The effective mask is the OR of all. | [NXmx] |
| Mask bits | Adds bit 6 (cluster), bit 8 (user mask), bit 31 (virtual pixel). Bits in `mask & 0xFFFF` reject a pixel. Masks and flat fields should be compressed. | [NXmx] |
| Per-frame geometry | Goniometer axes as `[nP]` arrays with `_end` and `_increment_set`, and `two_theta[nP]`. | [FileWriter] |
| Linear corrections | `data_scaling_factor` and `data_offset`: corrected = (data + offset) × factor ("gain", "pedestal"). | [NXmx] |
| Compression filters | HDF5 filter 32008 bitshuffle (cd_values[4] = 2 for LZ4) and 32004 LZ4. The bitshuffle chunk starts with u64 BE uncompressed size and u32 BE block size. The DECTRIS FileWriter doc names no filter ID. | [HDF5 filters, bitshuffle, FileWriter] |

## PSI Jungfraujoch

| Capability | Detail | Source |
|---|---|---|
| Compatibility | "Based on and compatible with" Stream V2, with differences. LZ4 alone is not allowed. Some fields are absent and some added. It adds calibration and metadata messages. | [JFJ CBOR] |
| Multi-channel | The deserialiser throws "Single channel images only supported at the moment" when an image `data` map has more than one entry (`CBORStream2Deserializer.cpp:505–507`). It parses `flatfield_enabled` but not `flatfield`. | [JFJ deser] |
| Version check | `magic_number` on every message. | [JFJ CBOR] |
| Several writers | One PUSH socket per writer: "Multiple receivers must never be connected to one PUSH ZeroMQ socket", since start and end would reach only one. Start and end go on every socket, calibration on the first only, images split by file number. | [JFJ stream] |
| Start handshake | The start message times out after 1 s per socket, and collection fails if no puller is connected. In the TCP variant, `PREFLIGHT` (start without per-pixel arrays, ~1.4 kB against ~540 kB) and `START` must be acknowledged within 5 s. | [JFJ stream] |
| Series data | `pixel_mask` Map(name → Image), "multiple in case of storage cells". `threshold_energy` per channel. `az_int_map`, `az_int_bin_to_q`/`_two_theta`/`_phi`, `roi_map` (u16 bitmask per pixel). Geometry (`detector_modules`, orientation). Masks and maps are always `bslz4`. | [JFJ CBOR] |
| Calibration | `calibration` messages: one pedestal (or pedestal RMS) array per message, after start, first socket only, not acknowledged. Not sent for a DECTRIS source. | [JFJ stream, JFJ CBOR] |
| Per-frame results | Spots and reflections (lists of structures), spot counts, ROI sums (`sum`, `sum_square`, `pixels`, `max_count`, weighted sums), azimuthal profile, indexing lattice and cell, pixel statistics, packets expected and received, `storage_cell`, `xfel_pulse_id`. | [JFJ CBOR] |
| Frame numbering | `image_id`, plus `original_image_id` that counts frames removed by lossy reduction. | [JFJ CBOR] |
| End message | Run results: images collected and sent, efficiency, per-image arrays, azimuthal result, refined geometry, merged reflections. | [JFJ CBOR] |
| Other outputs | Preview: PUB with conflate, latest only, every viewer. Metadata: PUB, image metadata without arrays, bundled by period, order not guaranteed, end always last. | [JFJ stream] |
| Feedback | TCP ACKs per frame (`FATAL` for disk full etc.). For ZeroMQ, a writer notification socket reports `processed_images` and errors. | [JFJ stream] |
| Mask bits | DECTRIS-sourced: bit 1 (detector mask), bit 4 (pixels with signal in dark frames). Bit 9 beam-stop shadow "belongs to the run": cleared at each run. | [JFJ mask] |

## EPICS areaDetector

| Capability | Detail | Source |
|---|---|---|
| NDArray | `uniqueId`, `timeStamp`, `epicsTS`, `pAttributeList`, `codec` (name, level, shuffle, compressor), `compressedSize`. | [ADCore NDArray] |
| Codecs | `jpeg`, `zlib`, `blosc`, `lz4`, `lz4hdf5`, `bslz4`. Only plugins built `compressionAware` take compressed arrays, and other plugins drop them. | [ADCore NDCodec, NDPluginCodec] |
| NTNDArray | `value` union, `codec {name, parameters}`, `compressedSize`, `uncompressedSize`, `dimension[]`, `uniqueId`, `dataTimeStamp`, `attribute[] {name, value, descriptor, sourceType, source}`. The converter stores the uncompressed type in `codec.parameters`. All of it is per array: there is no series-level field. | [NTNDArray, ADCore ntnd] |
| Constant data | NDFileHDF5 layouts write attributes and datasets `OnFileOpen` or `OnFileClose`. NDPluginBadPixel reads its bad pixels from a JSON file, not from the stream. | [NDFileHDF5, NDPluginBadPixel] |
| ADEiger multi-threshold | R3-6, EIGER2 and PILATUS4: one NDArray per threshold. All go on asyn address 0, and threshold N goes on address N. Each carries the NDAttributes `ThresholdNumber` and `ThresholdEnergy`. The Monitor moved to address 10. | [ADEiger] |
| ADEiger series data | From the Stream V2 start it keeps `image_dtype`, size, `number_of_images` and `threshold_energy`. Flat field and mask are not read (`stream2Api.cpp:162–205`). The threshold number is parsed with `"threshold_%d"`, which a `difference` channel does not match (`:334`). | [ADEiger stream2] |
| ADEiger pass-through | Compressed `bslz4` is passed on after stripping the 12-byte HDF5 header (`stream2Api.cpp:318–319`). `lz4` becomes codec `lz4hdf5`. ADCore's `bslz4` is the bare `bshuf_compress_lz4` stream. | [ADEiger stream2, NDPluginCodec src] |
| Signed view | `SignedData` makes flagged pixels (2^32−1, 2^32−2, …) appear as −1, −2. | [ADEiger] |

## ESRF Lima2

| Capability | Detail | Source |
|---|---|---|
| Frame | Dimensions, `nb_channels` and pixel type. Channels are planar: total size = channel size × channels. | [Lima2 core] |
| Frame metadata | `idx`, `recv_idx`, `timestamp`, `channels[] {name, no_data_value}`, `is_final`, `is_valid`. | [Lima2 core] |
| DECTRIS plugin | Release dectris-v1.0.0: EIGER2 and PILATUS4 over SIMPLON 1.8, multi-threshold and difference, Stream V2 at 8/16/32 bits, LZ4 and bslz4, multiple receivers, raw saving of the compressed chunks. | [Lima2 notes] |
| Channels | Takes the channel count from `channels`. With difference enabled it keeps only the last channel (`acquisition_impl.cpp:708–715, 829–835`). | [Lima2 dectris] |
| Series data | Flat field, mask and threshold energies are only logged at trace level (`handle_start_msg`, `:212`). The `flatfield` and `pixel_mask` parameters are commented out (`params.hpp:55–56`). `bit_depth_image` travels in `header_appendix` (`control_impl.cpp:460–464`). | [Lima2 dectris] |
| Start to all receivers | The receiver that PULLs the start message re-publishes it over a ZeroMQ PUB topic to the others (`acquisition_impl.cpp:600–668`). Receivers are synchronised over MPI before arm. | [Lima2 dectris] |
| Processing constants | The legacy pipeline loads mask and flat field from HDF5 paths at pipeline build. One flat field is applied to every channel (`flatfield.hpp:30–31`). | [Lima2 legacy, Lima2 flatfield] |

## Bluesky, ophyd-async, Tiled, Diamond, MAX IV

| Capability | Detail | Source |
|---|---|---|
| event-model | StreamResource: `data_key`, `mimetype`, `uri`, `parameters`. StreamDatum: `indices` and `seq_nums` slices. The descriptor's `configuration` holds the readings "necessary for interpreting data". | [event-model] |
| ophyd-async Eiger (fastcs + Odin) | One dataset `/data`, `bslz4`, dtype from `bit_depth_image`. The pixel mask is a configuration signal holding a *path* (`<file>_meta.h5//mask`), not data. No threshold channels. | [ophyd-async eiger] |
| Tiled | The TiledWriter puts the descriptor's configuration in stream metadata. External arrays are registered from StreamResource. | [TiledWriter] |
| Diamond EigerFan | Stream V1 only. Header to all consumers, images in blocks: `((frame + offset) / block_size) % consumers`. End to all. The meta writer keeps flat field and count-rate from `header_detail = all`. | [EigerFan] |
| MAX IV streaming-receiver | Stream V2 over one PULL. Keeps scalar flags and `threshold_energy` from start, and drops flat field and mask. Looks up channels as `threshold_{i+1}`. Repacks to `(nthresh, y, x)` with chunks `(1, y, x)`. The file name comes from `user_data`. Forwards on PUSH. | [MAX IV SR] |
| NSLS-II | Not investigated. | — |

## Capabilities our design must not leave behind

Our design: named parts, whole-part selection, series parts sent once per session, a generation
number for updates, and every, latest and pull deliveries.

| # | Capability | Sources | Our design |
|---|---|---|---|
| 1 | Per-channel series arrays (flat field f32, mask u32), and a threshold energy per channel | Stream V2, FileWriter, NXdetector_channel, SIMPLON, JFJ CBOR | **Partly.** Series parts can hold them. **Miss:** nothing ties `flatfield_threshold_2` to the frame part `threshold_2`. A subscriber selecting one frame part cannot discover or receive its series parts by rule. It needs a declared "describes part X" link, and selection should be able to follow it. |
| 2 | Difference channel series data of a different shape (2 energies, OR-ed mask, no flat field) | FileWriter, NXdetector_channel, SIMPLON | **Miss** if `field_axes` assumes the same fields per threshold. Each part needs its own set of series data. |
| 3 | Opting out of large series data (`header_detail`) | Stream V2, SIMPLON | **Covered** if series parts are selectable by name. Decide the default. DECTRIS's default sends none. |
| 4 | Series-wide arrays not tied to a channel (count-rate LUT, `az_int_map`, `roi_map`, bin legends) | Stream V2, JFJ CBOR | **Covered** by series parts. **Miss:** a link from a per-frame part (azimuthal profile, ROI sums) to its legend series part, as in #1. |
| 5 | Series data keyed by an axis, not a part (mask and pedestal per storage cell or gain) | JFJ CBOR, NXmx | **Partly.** A series array with a `cell` axis works, but the link must name the axis it follows. |
| 6 | "Applied" flags and the masked/no-data value (2^bits−1, or 0 with `mask_to_zero`, `saturation_value`, `error_value`) | Stream V2, SIMPLON, JFJ CBOR, Lima2 core, ADEiger | **Miss.** Only application text can hold them. A consumer cannot reliably tell masked pixels from counts. Needed: typed per-part scalar series data, at least the no-data and saturation values. |
| 7 | Scalar series data (energies, times, geometry, serial, `number_of_images`) | Stream V2, JFJ CBOR | **Partly.** Application text is untyped. A small typed scalar series record would match Stream V2. |
| 8 | Series identity on every message (`series_id`, `series_unique_id`) | Stream V2, JFJ CBOR | **Covered** by the instance ID. Carry the detector's `series_id` and `image_id` as fields: `image_id`'s base is *not stated* in Stream V2, and the FileWriter's `image_nr_start` defaults to 1. |
| 9 | Frames removed upstream (`original_image_id`) | JFJ CBOR | **Partly.** The frame index is dense per instance. The detector's own number must be a record field. |
| 10 | Rational per-frame times on a series time base, relative to `series_date` | Stream V2 | **Covered** as record fields, with the time base as series data. |
| 11 | Encodings with framing and parameters | tag, examples, bitshuffle, NDPluginCodec src, ADEiger stream2 | **Partly. Miss:** the name `bslz4` means the HDF5-framed form (12-byte header) in DECTRIS and the bare form in areaDetector. `lz4` is HDF5-framed in Stream V2, raw in V1 and areaDetector. Encoding names must pin the framing, e.g. `bslz4-h5` and `lz4-h5`. Series parts may be compressed too (JFJ always sends masks as `bslz4`). |
| 12 | Series data reaching every consumer of a distributed set | JFJ stream, Lima2 dectris, EigerFan, ZMQ | **Covered:** series parts go per session, so to every puller. Keep that rule when pullers share frames. |
| 13 | A late or latest consumer getting the series data | SIMPLON Monitor (metadata per TIFF), JFJ preview (PUB, start lost if late) | **Covered** if a latest session gets the current generation at join, before its first frame. |
| 14 | Series data that changes within a series | NXmx `(nP, i, j)` mask, JFJ calibration messages, JFJ bit 9 per run | **Partly.** The generation number exists. **Miss:** (a) each frame's record should state the generation it was produced under, the NXmx per-frame model. (b) For every and pull, an update must arrive before the first frame that uses it. (c) A subscriber must keep the old generation until the frames that used it are released. (d) Under pressure, latest needs a rule: never skip a generation that a delivered frame needs. |
| 15 | Per-frame variable-length structured results (spots, reflections, ROI sums) | JFJ CBOR | **Covered** by the planned varying first axis with structured elements. |
| 16 | End-of-series data (counts, efficiency, run results, refined geometry) | JFJ CBOR, NDFileHDF5 `OnFileClose` | **Miss.** End is an outcome with no payload. Needed: series parts published at End, or a final generation. |
| 17 | Records-only and metadata-only consumers | JFJ stream (metadata stream) | **Covered** by the records-only selection and batches. |
| 18 | Per-frame user data of any size (`image_appendix`) | Stream V2, JFJ CBOR | **Partly.** Application fields are fixed-size. A bytes part with a declared encoding (JSON or CBOR) covers it. |
| 19 | Per-series user data (`header_appendix`), used for file name, bit depth and extra metadata | Stream V2, Lima2 dectris, MAX IV SR, JFJ CBOR | **Covered** by application text. |
| 20 | Channels as separate streams or one stacked array | ADEiger (address N or 0), FileWriter `[nP,nC,i,j]`, MAX IV SR, Lima2 core | **Covered:** whole parts give "address N", all parts give "address 0". A saver wanting `(nC, y, x)` stacks the parts itself. |
| 21 | Readiness before arm, and refusing a start with no consumer | JFJ stream, Lima2 dectris | **Covered** by the start fence. |
| 22 | Consumer errors reported back (disk full, processed count) | JFJ stream | **Partly.** Outcomes and Health exist. A subscriber-to-publisher error report is *not* in the design. |
| 23 | Geometry and modules as series data | FileWriter, JFJ CBOR, SIMPLON §4 | **Partly:** existing gap 4 in detectors.md. Series parts can carry it, with no schema. |
| 24 | Meaning of mask bits | SIMPLON, NXmx, JFJ mask | **Partly:** "after 0.1" in the roadmap. Name a convention, NXmx's, for mask series parts. |
| 25 | Bit depth chosen per series from exposure time | SIMPLON, ADEiger | **Covered** by a new instance per series (gap 2). |

## Open questions the sources do not settle

1. Does the DCU block or drop when no PULL consumer is connected? ZeroMQ PUSH blocks
   [ZMQ]. The DCU has a `dropped` counter for "images that got dropped as not requested"
   [SIMPLON]. Neither says what happens to a start message sent before any consumer connects.
2. Stream V2's `threshold_energy` is `Map(TextString -> Float)` [Stream V2]. How is the
   difference channel's pair of energies sent? FileWriter uses `float[2]` [FileWriter].
3. Does Stream V2 send a flat field for the difference channel? Only the FileWriter states
   there is none.
4. Is the count-rate table per threshold? Stream V2 and NXmx have one table.
5. What does `header_detail = none` send? GitHub and SIMPLON 1.8 disagree.
6. How many thresholds does PILATUS4 have? Only ADEiger's comment says 4. EIGER2 has 2
   [detectors.md, EIGER2].
7. Can any DECTRIS series datum change between arm and disarm? SIMPLON says nothing, and
   no source shows an update message.
8. Is `image_id` 0- or 1-based in Stream V2? It is not stated. The FileWriter default is 1.
9. Is the order of the `flatfield` and `pixel_mask` maps guaranteed to follow `channels`? Only
   the image `data` order is stated [Stream V2].

## Sources

- [Stream V2] DECTRIS, Stream V2, commit `732e6904` (2026-06-24): https://github.com/dectris/documentation/blob/732e6904635bd890d8582d3f916d8354cc4dba32/stream_v2/README.md
- [tag] DECTRIS, CBOR tag 56500, same commit: https://github.com/dectris/documentation/blob/732e6904635bd890d8582d3f916d8354cc4dba32/cbor/dectris-compression-tag.md
- [examples] DECTRIS, Stream V2 examples (`stream2.h`, `stream2.c`, `client.py`), same commit: https://github.com/dectris/documentation/tree/732e6904635bd890d8582d3f916d8354cc4dba32/stream_v2/examples
- [FileWriter] DECTRIS, FileWriter (v2024.2 NXmx), same commit: https://github.com/dectris/documentation/blob/732e6904635bd890d8582d3f916d8354cc4dba32/filewriter/README.md
- [SIMPLON] DECTRIS, SIMPLON 1.8 API documentation, document v3.2 (2024-04-05): https://media.dectris.com/filer_public/6d/57/6d5779b4-2c8c-45a7-8792-6ef447f1ddde/simplon_apireference_v1p8.pdf
- [DECTRIS header docs] DECTRIS, header groups (NeXus, EIGER API 1.5): https://www.dectris.com/support/downloads/header-docs/nexus
- [NXmx] NeXus definitions, `applications/NXmx.nxdl.xml`, commit `3b20fbcc` (2026-11-18): https://github.com/nexusformat/definitions/blob/3b20fbcc0c77eecaf2245c94f8be88eb781a1cc1/applications/NXmx.nxdl.xml
- [NXdetector_channel] Same commit: https://github.com/nexusformat/definitions/blob/3b20fbcc0c77eecaf2245c94f8be88eb781a1cc1/base_classes/NXdetector_channel.nxdl.xml
- [HDF5 filters] HDF Group, registered filter plugins (master, fetched 2026-11-22): https://github.com/HDFGroup/hdf5_plugins/blob/master/docs/RegisteredFilterPlugins.md
- [bitshuffle] `bshuf_h5filter.c`, commit `f57e2e50` (the one DECTRIS cites): https://github.com/kiyo-masui/bitshuffle/blob/f57e2e50cc6855d5cf7689b9bc688b3ef1dffe02/src/bshuf_h5filter.c
- [ZMQ] libzmq `zmq_socket.adoc`, ZMQ_PUSH, commit `46493370`: https://github.com/zeromq/libzmq/blob/46493370217ac135246617fa2f6ac819d8b61bfc/doc/zmq_socket.adoc
- [JFJ stream] PSI Jungfraujoch `docs/IMAGE_STREAM.md`, 1.0.0-rc.188, commit `3f87e056` (2026-11-20): https://gitea.psi.ch/mx/Jungfraujoch/src/commit/3f87e0565ffe82544c17273091ae1c65252eb478/docs/IMAGE_STREAM.md
- [JFJ CBOR] Same commit, `docs/CBOR.md`: https://gitea.psi.ch/mx/Jungfraujoch/src/commit/3f87e0565ffe82544c17273091ae1c65252eb478/docs/CBOR.md
- [JFJ mask] Same commit, `docs/PIXEL_MASK.md`: https://gitea.psi.ch/mx/Jungfraujoch/src/commit/3f87e0565ffe82544c17273091ae1c65252eb478/docs/PIXEL_MASK.md
- [JFJ deser] Same commit, `frame_serialize/CBORStream2Deserializer.cpp`: https://gitea.psi.ch/mx/Jungfraujoch/src/commit/3f87e0565ffe82544c17273091ae1c65252eb478/frame_serialize/CBORStream2Deserializer.cpp
- [ADEiger] areaDetector ADEiger `RELEASE.md` (R3-6), `docs/ADEiger/eiger.rst`, `eigerApp/src/eigerDetector.cpp`, commit `a7b25dca` (2026-06-22): https://github.com/areaDetector/ADEiger/tree/a7b25dca71a617c03348787bde9b3e1032a08cee
- [ADEiger stream2] Same commit, `eigerApp/src/stream2Api.cpp`: https://github.com/areaDetector/ADEiger/blob/a7b25dca71a617c03348787bde9b3e1032a08cee/eigerApp/src/stream2Api.cpp
- [ADCore NDArray], [ADCore NDCodec], [ADCore ntnd], [NDPluginCodec src] areaDetector ADCore, `ADApp/ADSrc/NDArray.h`, `NDCodec.h/.cpp`, `ADApp/ntndArrayConverterSrc/ntndArrayConverter.cpp`, `ADApp/pluginSrc/NDPluginCodec.cpp`, commit `36840e14` (2026-10-13): https://github.com/areaDetector/ADCore/tree/36840e14b683079a8eb8423278eef41f371b1914
- [NDPluginCodec], [NDFileHDF5], [NDPluginBadPixel] Same commit, `docs/ADCore/NDPluginCodec.rst`, `NDFileHDF5.rst`, `NDPluginBadPixel.rst`.
- [NTNDArray] EPICS normativeTypesCPP, `src/ntndarray.cpp`, `src/ntndarrayAttribute.cpp` (master, fetched 2026-11-22): https://github.com/epics-base/normativeTypesCPP/blob/master/src/ntndarray.cpp
- [Lima2 core] ESRF Lima2, `libs/core/include/lima/core/frame_metadata.hpp`, `frame_info.hpp`, develop `f62aacc2` (2026-11-19): https://gitlab.esrf.fr/limagroup/lima2/-/tree/f62aacc2c0a5ba628968682314bd65f533d6054b
- [Lima2 dectris] Same commit, `detectors/dectris/src/acquisition_impl.cpp`, `control_impl.cpp`, `include/lima/detectors/dectris/params.hpp`.
- [Lima2 notes] Same commit, `docs/release_notes.md` (core-v1.0.0, dectris-v1.0.0).
- [Lima2 legacy], [Lima2 flatfield] Same commit, `processings/legacy/src/pipeline_legacy.cpp`, `libs/proc_tbb/include/lima/processing/nodes/flatfield.hpp`.
- [event-model] bluesky event-model schemas `stream_resource.json`, `stream_datum.json`, `event_descriptor.json`, commit `79703246`: https://github.com/bluesky/event-model/tree/79703246ef15fcb62e70d68e52aefd469b75ee4a/src/event_model/schemas
- [ophyd-async eiger] ophyd-async `src/ophyd_async/fastcs/eiger/`, `fastcs/odin/_data_logic.py`, commit `5e4df8b2`: https://github.com/bluesky/ophyd-async/tree/5e4df8b27cf1108a844d43bc43454f6fee97f0a9/src/ophyd_async/fastcs
- [TiledWriter] bluesky `docs/tiled-writer.rst`, commit `d960763a`: https://github.com/bluesky/bluesky/blob/d960763a10645c5117aacd791424f67d68b5f544/docs/tiled-writer.rst
- [EigerFan] Diamond eiger-detector, `cpp/data/eigerfan/src/EigerFan.cpp` (lines 432, 490–551, 768–776), `python/src/eiger_detector/data/eiger_meta_writer.py`, commit `5013edf6`: https://github.com/DiamondLightSource/eiger-detector/tree/5013edf626a3591714617156b3564c72a8398d12
- [MAX IV SR] MAX IV streaming-receiver, `streaming_receiver/receiver/detector.py` (`DectrisStream2`, lines 453–550), GitHub mirror commit `28a847b4` (2025-09-18). Upstream is gitlab.maxiv.lu.se per its `Internals.md`: https://github.com/maxiv-science/streaming-receiver/tree/28a847b4f03ad3e9517a152e59c85bdb41019142
- [detectors.md], [EIGER2] Our note and its source: [detectors.md](detectors.md).
