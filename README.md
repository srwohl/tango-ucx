# tango-ucx

A bulk data plane for Tango devices, with C++ publishing and C++/Python subscriptions.
Tango carries discovery and control; UCX carries frames between processes on the same
host or across the network. Payloads can reside in host or CUDA memory.

Use it to send high-rate detector arrays or compressed frames to processing, archival
and live-view applications. Each frame carries its index, acquisition timestamp and
application fields, so interpreting it needs no Tango round trip. Memory budgets bound
the transport rings, and delivery modes let each application receive every frame,
keep up with the newest frames or share work with other subscribers.

## Build

With Pixi >=0.55 installed, run from this checkout:

```sh
pixi run build
PYTHONPATH=build/python pixi run python -c 'import tango_ucx'
```

`build` also configures CMake. The Linux environment provides a C++20 compiler, CMake,
UCX >=1.21, cppTango/PyTango >=10, Python >=3.11, pybind11 and nlohmann_json >=3.12.
The Python package is built in `build/python/tango_ucx`.

For CUDA source and receive support, use the GPU environment and a compatible NVIDIA driver:

```sh
pixi run -e gpu build-gpu
PYTHONPATH=build-gpu/python pixi run -e gpu python -c 'import tango_ucx'
```

The GPU environment uses CUDA 12.9 and CuPy >=14, with its own `build-gpu/` tree.

## Subscribe from Python

The Tango device must expose a tango-ucx publisher. Pass its name or an existing
PyTango `DeviceProxy` to `every()`, `latest()` or `pull()`.
For an array stream that is already publishing:

```python
import tango_ucx

with tango_ucx.latest("example/detector/1", budget="64MiB", batch=8) as subscription:
    for batch in subscription:
        print(batch.array.shape, batch.records)
```

Run a script with `PYTHONPATH=build/python pixi run python subscriber.py`.

| Delivery | Use for | Joining and pressure |
|---|---|---|
| `every(device, ...)` | Archival or processing that requires every frame, in order | Join before start; a full receive ring applies pressure |
| `latest(device, ...)` | Live views that can skip frames | Join at any time; frames without receive room are skipped |
| `pull(device, range, ...)` | Sharing processing among a set of pullers | Join before start; each frame goes to one puller, a range at a time |

For `every` and `pull`, enable `PublisherLimits::allow_every` in the device, join all
subscriptions, then start acquisition through the device's application command.
All pullers use the same range: a whole number of batches that fits each receive ring.
If no puller has room for a range, the set applies pressure.

`budget` accepts a positive byte count or a size such as `"256MiB"`; ring depth is
derived from it. `batch` limits frames per batch, and `max_wait` limits the wait to
fill a batch. `latest` also accepts `max_rate` in frames per second.
Async iteration and callback delivery are available on the same subscription.
Iteration raises `tango_ucx.Failed` on failure; `outcome` and `health()` report how it ended.

### Payloads and ownership

Array streams expose read-only NumPy payloads through `batch.array`. Opaque streams
carry bytes unchanged, with a separate length for each frame:

```python
with tango_ucx.latest("example/compressed/1") as subscription:
    for batch in subscription:
        for frame in range(batch.frames):
            print(batch.payload(frame).nbytes, batch.records[frame])
```

`batch.records` is a read-only host array for either kind of stream. Batches and their
views borrow receive memory: holding a batch, NumPy view or DLPack tensor holds its
receive storage. Use `batch.copy()` for an independent host copy when processing needs
to retain data. Release borrowed views promptly so the publisher can reuse the storage.

| Python `memory` option | Receive path | Application access |
|---|---|---|
| `"host"` (default) | Host memory | NumPy arrays or per-frame bytes |
| `"cuda:N"` | GPU N | `batch.gpu_view(stream)` with DLPack or per-frame GPU pointers |
| `"pinned:N"` | Page-locked host memory for uploads to GPU N | Queue uploads, then call `batch.release_after(stream)` |

GPU view completion events protect receive storage until queued GPU reads finish.
Pinned receive waits for recorded upload completion and for host views to be released.
GPU-over-TCP receive requires `allow_gpu_over_tcp=True`.
For complete programs and CUDA stream handling, see the
[GPU examples](https://github.com/srwohl/tango-ucx-gpu-examples) and their
[tomography pipeline](https://github.com/srwohl/tango-ucx-gpu-examples/tree/master/tomography).

## Publish from a Tango device

The public C++ interfaces are [ucx.h](include/tango-ucx/ucx.h) and
[tango.h](include/tango-ucx/tango.h). A device integrates them as follows:

1. Register `install_ucx_commands()` and `install_ucx_attributes()` with its Tango class.
2. Construct a `Publisher` with a `Description` and `PublisherLimits`, then
   `attach_publisher()` to the device. Set `allow_every` if every delivery or pull is needed.
3. Let subscriptions join, then call `ucx_start()` when acquisition starts.
4. `acquire()` a source slot, fill its payload and application fields, and publish it.
   An unavailable slot means the application must wait, buffer or stop.
5. Call `finish()` when publishing ends. Detach the publisher before destroying it.

For array streams, set element type and shape. For opaque streams, use `Element::Bytes`,
an empty shape and `max_payload_bytes`, then `publish_bytes(actual_length)` for each slot.
Set `PublisherLimits::gpu` to publish from CUDA source slots and queue writes on the
GPU stream supplied to the slot.

### Install for another project

```sh
pixi run cmake --install build --prefix "$PWD/stage"
```

Configure the application with `-DCMAKE_PREFIX_PATH=/path/to/stage` and link the installed target:

```cmake
find_package(tango-ucx CONFIG REQUIRED)
target_link_libraries(my_device PRIVATE tango-ucx::ucx)
```

For CUDA applications, install `build-gpu`, link `tango-ucx::cuda`, and include its
runtime registration objects with
`target_sources(my_device PRIVATE $<TARGET_OBJECTS:tango-ucx::cuda>)`.
The Python package installs under `lib/pythonX.Y/site-packages` in the prefix;
add that directory to `PYTHONPATH` and use the Python version that built the extension.

## Placement and network selection

Set `PublisherLimits::progress_cpu` or `SubscriptionOptions::progress_cpu` to pin an
instance's progress thread before UCX and ring setup. Python accepts
`tango_ucx.every(proxy, progress_cpu=6)`. The default `-1` inherits affinity.

The CPU must be allowed to the creating thread, affinity must be settable, and sysfs
must identify its SMT siblings. Explicit assignments reserve the whole physical core
within the process until the progress thread exits, including after setup failure.
Unpinned threads and other processes can still use those cores. Give separate
device-server processes disjoint CPU pools and choose NIC-local CPUs using the topology
report. Pinning preserves the memory policy; ring NUMA placement still needs checking.

Set `PublisherLimits::net_devices` or `SubscriptionOptions::net_devices` to select UCX
network devices per instance, such as `"mlx5_0:1"` or `"mlx5_0:1,mlx5_1:1"`.
Python accepts `tango_ucx.every(proxy, net_devices="mlx5_0:1")`, also for `latest` and `pull`.
The empty default inherits UCX environment/default selection. The selector overrides
`NET_DEVICES` in that instance's UCX context without changing the process environment.
UCX chooses transports and rails; shared-memory transports remain available.
Device selection does not reserve bandwidth.

## Health and CPU use

Publisher and subscription `health()` include counts, pressure, failures, negotiated
transports and a `topology` snapshot (`Health::topology` in C++). Frame and slot counts
are sampled about once per millisecond of progress; subscription setup, Ready and
outcome changes update immediately. Topology is sampled at setup and once a second.

| Topology field | Meaning |
|---|---|
| `cpu`, `numa_node` | Last sampled progress-thread CPU and its NUMA node; unpinned threads can move |
| `allowed_cpus` | Progress thread's Linux CPU list |
| `sibling_cpus` | CPUs sharing the sampled physical core, including the sampled CPU |
| `ring_numa_node` | NUMA nodes of the first, middle and last existing host-ring pages; `-1` unknown, including CUDA rings; `-2` mixed |
| `network_devices` | Observed endpoint network devices and their sysfs NUMA nodes; `-1` for unknown locality |

Ring sampling does not fault pages in, and a single sampled node does not establish
placement of the whole ring. Observed network devices do not establish which rails
carry payload traffic; local sessions can report no NIC.
Read the publisher's snapshot through the read-only Tango attribute `UcxTopology`:

```python
import json

topology = json.loads(proxy.read_attribute("UcxTopology").value)
```

Set `TANGO_UCX_IDLE` before creating publishers and subscriptions to choose progress
loop behavior for the process:

| Setting | Behavior |
|---|---|
| Unset | Poll for 200 microseconds after the last work, then wait for UCX or application events |
| `wait` | Wait as soon as a pass finds no work; lower CPU use with a possible wakeup-latency and throughput cost |
| `spin` | Poll continuously |

Compare modes at the application's frame rate and latency requirement using the
[CPU measurements](docs/benchmarks.md) and
[benchmark tools](https://github.com/srwohl/tango-ucx-benchmarks).

## Development and reference

```sh
pixi run test
pixi run test-python
pixi run check
pixi run -e gpu test-gpu
```

`test` includes the focused Python suite. `check` checks public names against the
lists in `tools/`, the glossary and target layering. Use `pixi run build-profile`
for an optimized build with symbols and frame pointers.

- [Architecture and ownership diagrams](docs/diagrams.md)
- [Terminology and delivery semantics](CONTEXT.md)
- [Tomography pipeline design](docs/tomography-pipeline.md)
- [Benchmark method and measurements](docs/benchmarks.md)

LGPL-3.0-or-later; see [LICENSE](LICENSE).
