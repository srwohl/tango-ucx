# tango-bulk2

First implementation attempt for the selected fixed-table design in
[`tango-bulk/prototypes/final_shape/fixed_tables_v2.md`](../tango-bulk/prototypes/final_shape/fixed_tables_v2.md).
The implementation is intentionally empty at this stage. This tree starts with
the build, packaging, line-budget, and architecture-check scaffolding; the first
code milestone is the host-only UCX vertical slice described in section 16 of
that design.

The first attempt covers C++ publication and Tango control, the Python
subscriber, and CUDA receive through DLPack. Pull delivery remains backlog.
Library and test source must remain under 10,000 physical lines, with the
published estimates and per-file allowances recorded in `budget.toml`.

## Development

Use the default pixi environment for host development:

```sh
pixi run configure
pixi run build
pixi run test
pixi run test-python
pixi run check
pixi run count
```

Use `pixi run -e gpu configure` and `pixi run -e gpu test-gpu` for the CUDA
environment. `tools/count.py` and `tools/check.py` run from the first commit,
before implementation files are added.
