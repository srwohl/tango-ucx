# What a full-size tomography pipeline asks of the transport

Status: **notes for planning, written 2026-10-08. Nothing here is built or decided.** The
pipeline is planned in the examples project, `tango-ucx-gpu-examples/tomography/NEXT.md`.
Terms are those of [CONTEXT.md](../CONTEXT.md).

## The load

| Stream | Frame | Rate |
|---|---|---|
| Raw detector frames, uint16 2048 × 2048 | 8 MiB | 240 frames/s, 1.875 GiB/s |
| Binned 2 × 2, uint32 1024 × 1024 | 4 MiB | 240 frames/s, 0.94 GiB/s |
| Slice blocks of a float32 volume, 64 slices of 2048 × 2048 | 1 GiB | up to 3.7 GiB/s |

A scan is about 2100 frames. The raw stream has two or three every subscribers: an archive,
a binning device and a reconstruction device.

## What already fits

Limits are in `src/transport/memory.cpp`.

| Limit | At this load |
|---|---|
| A frame of at most 2 GiB | A volume is published as blocks of at most 128 slices. The examples already do this |
| 1,024 source slots | 8 GiB of raw frames in flight at the publisher: over four seconds |
| A GPU receive ring of at most 4,096 frames | 32 GiB of raw frames: nearly two scans |
| 64 sessions | Three are needed |
| One active message per frame | 240 a second. The examples reached about 1,000 small frames a second on one host |

The launcher's 512 MiB and 1,024-frame caps on a link are the examples' own
(`tomography/pipeline_control.py`), not the library's.

## What is not known

Each of these is a measurement, not a change.

1. **Frames of 8 MiB at 240 a second.** The largest sustained load measured so far is 128 MiB
   volumes at about three a second and frames of 128 to 256 KiB at about 1,000 a second, both
   on one host over CUDA IPC. Measure `transfer_mean_ns` and pressure at the target size and
   rate, GPU to GPU on one host first.
2. **Between nodes.** No GPU-to-GPU transfer between nodes has been measured. The development
   machine's UCX has no `rc` and no `gdr_copy`. On the cluster, record `ucx_info -d` and the
   protocol `UCX_PROTO_INFO=y` reports for each link. An RC transport in `Health::transport`
   does not show that GPU memory was registered directly.
3. **GPU memory to a host subscriber on one host.** UCX 1.22 sends it as 8 KiB shared-memory
   fragments, each a separate device copy. The examples avoid it by receiving on the GPU and
   downloading once. A host archive of the raw stream would meet it at 1.875 GiB/s, so the
   archive should receive on the GPU, or the source should be in host memory.
4. **`UCX_SYSV_SEG_SIZE` above the default** corrupts frames in UCX 1.22: its receive length
   is 16 bits. Keep the default until a fixed UCX is validated.

## What the pipeline would ask for

In the order the pipeline's build order reaches them. None is needed for its first five steps.

| Ask | Why | Without it |
|---|---|---|
| A puller's range that need not be a whole number of batches | A scan of 2,100 frames cannot be a range with a batch of 16. Whole scans to several reconstruction GPUs is a pull set | The pipeline picks a batch that divides the scan, or sets the batch to one |
| A way to skip a whole range | A detector cannot be held. Every applies pressure; latest skips single frames, which leaves a scan with holes | The reconstruction device keeps an every subscription, reads everything and discards the scans it has no room for. That costs the transfer of every discarded frame and no change here |

## What the library already has for it

- The loops wait when there is no work (`src/transport/idle.h`). The prototype's four devices
  each kept a core busy polling.
- A Python batch caches its records: a repeated `batch.records` cost 37 microseconds and now
  costs 1.5.
