# Detectors at light sources and the frame model

Notes for [roadmap.md](../roadmap.md), sections 2–4: can the planned frame model describe each
detector in its own form? These are drawn from the sources listed at the end. A claim not found
there is marked *unconfirmed*. Written 2026-11-15. The survey was stopped early: HEXITEC,
Xspress3/4, ePix, DSSC, Timepix4 and camera pixel formats are not covered.

## Corrections to roadmap section 4

| Claim in the roadmap | Verdict |
|---|---|
| Medipix3 colour mode: up to 8 thresholds per 110 µm pixel | **Corrected.** In spectroscopic mode the pitch is 110 µm and the array is 128 × 128, and there are 8 thresholds in SM-CSM mode: "4 SPM + 4 CSM". The counters are 8 at 1/2/6/12 bits or 4 at 24 bits (single read-write), and 4 counters in continuous read-write [kt.cern]. So the axis is `(counter, y, x)` with 4 or 8 entries depending on the mode, and the bit depth is one of five. |
| Timepix3 raw data: 64-bit packets, chunks with "TPX3" headers per chip | **Confirmed, with detail.** A chunk has an 8-byte header: `"TPX3"`, a chip index, a reserved byte, and 2 bytes of net size. Then come little-endian 8-byte words, with their type in the top nibble. These are pixel `0xb`, TDC `0x6`, global time `0x4` and control `0x7`. A pixel packet holds PixAddr (bits 59–44), ToA (43–30), ToT (29–20), FToA (19–16) and SPIDR time (15–0). Serval writes raw data to a file or sends it to a `tcp://` destination [Serval §6.1, §4]. |
| Timepix3 hits arrive out of time order | *Unconfirmed* in the gathered sources. |
| HEXITEC: a spectrum per pixel, histogrammed to `(y, x, energy)` | *Unconfirmed.* |
| Xspress3: `(channel, energy)` spectra | *Unconfirmed.* |
| Mono12p packing | *Unconfirmed.* |
| Threshold detectors, compressed, as DECTRIS Stream V2 | **Confirmed.** Each image message carries all of the exposure's active channels (thresholds) in a map from channel name to array. Compression is CBOR tag 56500 with `bslz4` and an element-size modifier [Stream V2, tag]. |

## Gaps, most detectors first

1. **Data that belongs to the series, not to a frame.** Stream V2's start message carries, per channel, the threshold energy, a flat field and a 32-bit pixel mask, plus count-rate tables. EuXFEL calibration needs constants per cell and per gain stage. This affects EIGER2, PILATUS4, ARINA, SELUN and AGIPD/LPD/JUNGFRAU. *Need:* arrays sent once per instance (with the description, or as a part sent once), not in every record.
2. **Bit depth chosen per series, including sub-byte depths.** EIGER2 picks 8, 16 or 32 bits from the exposure [EIGER2]. PSI EIGER uses 4/8/16/32 [sls dataformat], Merlin 1/6/12 bits [LiberTEM Merlin], and Medipix3 1/2/6/12/24 bits. A change between series fits as a new instance. *Need:* sub-byte depths (1, 4, 6, 12) as bytes encodings that decode to `u8` or `u16`.
3. **Storage cells and gain stages within a train.** AGIPD stores 352 images per train in pixel memory, each as an amplitude plus a gain frame [AGIPD]. Raw modules are arrays with 2 entries on the first axis (data, gain). LPD stores its three gains one after another in the train, and JUNGFRAU has memory cells on its second axis [extra-data]. A train fits as a frame `(cell, 2, y, x)`, with cell and pulse IDs as record fields that follow the cell axis (`field_axes`, planned). The number of cells per train varies (the planned varying first axis). *Need:* both planned features in 0.1. Selecting by gain stage or cell is an axis selection.
4. **Modules, gaps and geometry.** EIGER2 modules are 1028 × 512 pixels from 4 × 2 chips. Wide pixels bridge the chip gaps, and virtual-pixel correction can be turned off [EIGER2]. EuXFEL stores modules as separate sources, combined along a first axis [extra-data]. A module axis fits. *Need:* module positions (geometry) somewhere a subscriber can read them. Until then it is application text.
5. **Encodings with parameters.** `bslz4` carries an element size [tag]. SELUN encodes 12-bit counts as 8-bit floats (5-bit mantissa, 3-bit exponent) [SELUN]. Whether SELUN's output is decoded before it leaves the detector is *unconfirmed*. *Need:* the encoding name plus its parameters.
6. **Timestamps as rationals.** Stream V2 sends `start_time`, `stop_time` and `real_time` as rationals, with a time base fixed for the series [Stream V2]. They fit as record fields: two integers each, or one integer with the time base in the series data (gap 1).
7. **Event data** (Timepix3) fits as planned: bytes `tpx3`, cut on chunk boundaries, with a part per chip by its chip index.

## Detectors covered

| Detector | Kind | Native form | Encoding | Per frame | Interface | Rate | Fits |
|---|---|---|---|---|---|---|---|
| EIGER2 [EIGER2] | frames | modules 1028 × 512 of 4 × 2 chips (256 × 256, 75 µm); 2 thresholds; 8/16/32-bit output | `bslz4` | image ID, start/stop/real time | Stream V2 over ZeroMQ, CBOR | not in sources | gaps 1, 2, 4 |
| PILATUS4, ARINA | frames | not in the gathered sources | `bslz4` | as Stream V2 | Stream V2 | — | gap 1 |
| SELUN [SELUN] | frames | 192 × 192, 100 µm; one threshold; two 12-bit counters; 2 × 2 binning | 8-bit float counts | — | 10 Gb/s fibre | 20 kfps, 120 kfps compressed | gaps 2, 5 |
| Medipix3RX / Merlin [kt.cern, LiberTEM Merlin] | frames | 256 × 256 at 55 µm, or 128 × 128 at 110 µm with 4–8 counters | 1/6/12-bit (Merlin) | — | Merlin TCP | not in sources | gap 2 |
| Timepix3 / Serval [Serval] | events | 64-bit packets in chunks per chip | `tpx3` | chunk: chip, size | file or TCP | not in sources | yes |
| JUNGFRAU [extra-data, sls] | frames in trains | memory cells per train; gain bits in the pixel word *unconfirmed* | raw | train, cell | slsDetectorPackage UDP, 48-byte header | — | gap 3 |
| PSI EIGER [sls] | frames | 4/8/16/32-bit pixels | raw | — | UDP, 48-byte header | — | gap 2 |
| AGIPD [AGIPD, extra-data] | frames in trains | 352 cells × (amplitude, gain) × module | raw | train, pulse, cell | Karabo | up to 6.5 MHz in-train | gap 3 |
| LPD [extra-data] | frames in trains | three gains stored in sequence | raw | train, pulse, cell, gain | Karabo | — | gap 3 |

## Sources

- [Stream V2] DECTRIS, Stream V2: https://github.com/dectris/documentation/blob/main/stream_v2/README.md
- [tag] DECTRIS, CBOR compression tag 56500: https://github.com/dectris/documentation/blob/main/cbor/dectris-compression-tag.md
- [EIGER2] Donath et al., EIGER2 hybrid-photon-counting X-ray detectors, J. Synchrotron Rad. 2023: https://pmc.ncbi.nlm.nih.gov/articles/PMC10325006/
- [SELUN] DECTRIS SELUN article: https://pmc.ncbi.nlm.nih.gov/articles/PMC12809242/
- [kt.cern] CERN Knowledge Transfer, Medipix3: https://kt.cern/?p=3178
- [LiberTEM Merlin] https://libertem.github.io/LiberTEM-live/detectors/merlin.html
- [Serval] Amsterdam Scientific Instruments, SERVAL manual v1.21, §6.1 TPX3 raw file format: https://www.quantastro.bnl.gov/sites/default/files/2022-04/ASIServer%20TPX3%20manual%20V1.21.pdf
- [sls] PSI slsDetectorPackage docs: UDP header and data format, https://slsdetectorgroup.github.io/devdoc/udpheader.html, https://slsdetectorgroup.github.io/devdoc/dataformat.html
- [extra-data] EuXFEL, AGIPD/LPD/JUNGFRAU data: https://extra-data.readthedocs.io/en/latest/agipd_lpd_data.html
- [AGIPD] AGIPD at the European XFEL (MID instrument), arXiv:1908.02103: https://arxiv.org/abs/1908.02103
