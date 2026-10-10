# tango-ucx

An implementation of the fixed-table design in
[`tango-bulk/prototypes/final_shape/fixed_tables_v2.md`](../tango-bulk/prototypes/final_shape/fixed_tables_v2.md):
the UCX transport, Tango control and a Python subscriber. A subscription receives into host
memory or, through DLPack, into CUDA memory. Opaque streams carry a detector's compressed
frames unchanged, each with its own length. Pull delivery shares one UCX stream among a set
of pullers, a range of frames at a time.

CUDA support also carries opaque frames into GPU memory, provides pinned host receive for
asynchronous uploads, and publishes from GPU source slots.
Runnable [GPU examples](../tango-ucx-gpu-examples/README.md) live in a separate application
project, with TCP and RDMA profiles and a two-node Slurm launcher.

The Python subscriber is built at `build/python/tango_ucx` by `pixi run build`.
Import it from the checkout with `PYTHONPATH=build/python pixi run python -c 'import tango_ucx'`.
Pass a Tango device name or an existing PyTango `DeviceProxy` to `every()`, `latest()` or
`pull()`, which also takes the range; each returned `Batch` exposes read-only NumPy payload
and record arrays, and a batch of an opaque stream exposes each frame's bytes with
`payload(i)`. Async iteration and callback delivery are available on the same subscription.

## Development

Use the default pixi environment for host development:

```sh
pixi run configure
pixi run build
pixi run test
pixi run test-python # focused Python suite; also included in pixi run test
pixi run check
```

Use `pixi run -e gpu test-gpu` for the CUDA environment. `pixi run check` runs
`tools/check.py`: the public names against the name lists in `tools/` and the glossary in
`CONTEXT.md`, and the layering of the targets.
