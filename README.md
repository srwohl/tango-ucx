# tango-ucx

First implementation attempt for the selected fixed-table design in
[`tango-bulk/prototypes/final_shape/fixed_tables_v2.md`](../tango-bulk/prototypes/final_shape/fixed_tables_v2.md).
The host-only UCX vertical slice, Tango control, and Python host subscriber are
implemented. CUDA receive through DLPack is Phase 3. Opaque streams, which carry a
detector's compressed frames unchanged and each with its own length, are Phase 4
([`NEXT.md`](NEXT.md)). Pull delivery shares one UCX stream among a set of pullers, a range
of frames at a time.

Library and test source must remain under the line ceiling in `budget.toml`, which also
records the estimates and per-file allowances.

The Python host subscriber is built at `build/python/tango_ucx` by `pixi run build`.
Import it from the checkout with `PYTHONPATH=build/python pixi run python -c 'import tango_ucx'`.
Pass a Tango device name or an existing PyTango `DeviceProxy` to `every()`, `latest()` or
`pull()`, which also takes the range; each returned `Batch` exposes read-only NumPy payload and record arrays, and a batch of
an opaque stream exposes each frame's bytes with `payload(i)`. Async
iteration and callback delivery are available on the same subscription.

## Development

Use the default pixi environment for host development:

```sh
pixi run configure
pixi run build
pixi run test
pixi run test-python # focused Python suite; also included in pixi run test
pixi run check
pixi run count
```

Use `pixi run -e gpu configure` and `pixi run -e gpu test-gpu` for the CUDA
environment. `tools/count.py` and `tools/check.py` run from the first commit,
before implementation files are added.
