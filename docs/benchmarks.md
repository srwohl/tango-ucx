# Benchmarks: the plan, and what the library needs for it

Status: **a plan, written 2026-10-10, with host bandwidth and per-frame experiments now built,
and a three-way CUDA IPC delivery comparison added 2026-10-11.**
The suite lives in its own project, `tango-ucx-benchmarks`, built against the installed package
as the GPU examples are. The remaining sweep and instrumentation below are planned unless
the measured section says otherwise.
Terms are those of [CONTEXT.md](../CONTEXT.md). "Zero-copy" below is UCX's name for a
rendezvous protocol, not a kind of batch.

## What it has to show

In this order:

1. tango-ucx carries frames about as fast as `ucx_perftest` does on the same link, and no
   slower than tango-bulk did. The C++ interface is measured first.
2. It does so at the loads the devices have, not at one convenient size.
3. Where it does not, the report says which part of the path is the reason.
4. The Python subscriber is measured against the C++ one, to find what to change in it.

## The bar tango-bulk left

From `tango-bulk/docs/THROUGHPUT.md`, measured with `tango-bulk-bench`:

| Setup | tango-bulk | Raw UCX |
|---|---|---|
| One host, shared memory, 1 MiB, ring depth 32 | 5.05 GiB/s | 18.0 GiB/s, `tag_bw` |
| The same, ring depth 2 | 10.9 GiB/s | the same |
| Two hosts, RoCE, 2 × 25 GbE, `rc_verbs` | 47.9 Gb/s, 96% of both rails | 23.4 Gb/s on one rail |

Its rules are kept:

- Compare at the same working set. `ucx_perftest` reuses one buffer; a ring does not.
- Compare at the same number of rails.
- Fill the payload once. Generating it was once 54% of the measurement.
- Stop the clock when the last frame is released, not when it is published.
- Verify the payloads, and show the verifier failing on frames damaged on purpose.

## What is already known

**Peer error handling changes the path.** tango-ucx creates its endpoints with
`UCP_ERR_HANDLING_MODE_PEER` (`Worker::endpoint`, `src/transport/ucp.cpp`); tango-bulk used
none. With it, UCX 1.22 selects zero-copy rendezvous lanes only on memory domains that can
invalidate a remote key (`src/ucp/wireup/select.c`). Those are the lanes it stripes over rails.

| Transport | Zero-copy rendezvous with peer error handling |
|---|---|
| `rc_mlx5`, `dc_mlx5` | Yes, read from the UCX source. Not measured |
| `rc_verbs` | No support was found in the source. Not measured |
| CMA | Only with `UCX_CMA_MEMORY_INVALIDATE=y`. Measured |
| `cuda_ipc` | Yes, read from the UCX source |

`ucx_perftest -t ucp_am_bw -s 1048576` on the development machine, UCX 1.22, shared memory, one
run each:

| Endpoints | Protocol for frames of 256 KiB and more | MB/s |
|---|---|---|
| No error handling | zero-copy read through CMA | 22,372 |
| Peer, with `MM_ERROR_HANDLING=y` as the library sets it | fragmented copy-in, copy-out through SysV | 21,272 |
| The same, with `UCX_CMA_MEMORY_INVALIDATE=y` | zero-copy read through CMA | 23,226 |

The library's own recorded lanes agree: `sysv/memory,cuda_ipc/cuda`, without CMA. On shared
memory the copying path costs nothing: in two sweeps of `ucx_perftest` from 16 KiB to 16 MiB,
with peer error handling it was as fast as without or faster, 9 GiB/s against 6 at 16 MiB. One
copy on each side runs on two cores; CMA's one copy runs on one. What it costs over RDMA is not
measured.

**The 3.7 GiB/s of slice blocks needs both rails** on 2 × 25 GbE: one rail carries 2.9 GiB/s.

**The conda UCX has RDMA.** It is built with verbs, mlx5 with DevX, and CUDA, but not
`gdr_copy`. The development machine shows no RDMA transport because it has no such hardware.

## The suite

Two programs, as `ib_send_bw` has two roles:

- `tango_ucx_perf_publisher`: a Tango device server, run with `-nodb`. A command gives it the
  description, the budget, the frame count and the rate.
- `tango_ucx_perftest <device> -t <test>`: the subscriber. It configures the publisher,
  measures, and prints a table and one JSON line for each point.

A Python driver plans the sweep, starts both roles on one host or two, runs the references and
writes the report.

### Loads

| Family | Frames | What limits it | Reported |
|---|---|---|---|
| Small, at a high rate | 1 KiB to 512 KiB | The cost of a frame | Frames/s, ns a frame, p50/p99/p99.9/max |
| Large | 1 MiB to 128 MiB | The cost of a byte | GiB/s, share of raw UCX |
| Volumes | 256 MiB to 2 GiB | A sustained rate | GiB/s against 3.7 GiB/s |

All in powers of two: 22 sizes. The small family also runs paced, at 1,000, 10,000 and 50,000
frames a second until other rates are named. Real points are marked in the sweep: compressed
detector frames of 410, 430 and 770 KB at 125 to 200 frames a second, as opaque streams with the
lengths of `testdata/`; raw frames of 8 MiB at 240 a second; volumes of 128 MiB.

### The sweep

| Axis | Values |
|---|---|
| Transport | Shared memory, GPU IPC, RDMA, TCP |
| Span | One host, two hosts. Shared memory and GPU IPC are of one host |
| Memory | Host to host, GPU to GPU, host to GPU, GPU to host, pinned host |
| Ring depth | 2, and the depth a device would use |
| Program | `ucx_perftest`, tango-ucx, `tango-bulk-bench` in host memory |
| Runs | Three or more: median, best, worst |

- A probe reads the host's free memory, its locked-memory limit and its cgroup limit, each
  GPU's free memory, and the transports of `ucx_info -d`. The budget of each point is computed
  from them. A point that does not fit is listed with the reason, never left out silently.
- Three sizes of sweep: minutes, about an hour, hours. A dry run prints the points and the
  time they need. An interrupted sweep continues.
- Between two hosts, the publisher and the subscriber are started over ssh or by Slurm, in both
  directions. The bare link is measured first with `ib_write_bw` or `iperf3`, then
  `ucx_perftest`, then tango-ucx.
- The reference that matches the library is
  `ucx_perftest -t ucp_am_bw -s <size> -H <header> -z -e -I`: one active message a frame with a
  header, registered memory, peer error handling, and the wakeup feature.

### A number counts only when the path was right

After Ready, the driver compares the lanes and the protocol UCX chose with what the point asked
for. A point that asked for RDMA and got TCP is marked invalid. So is a large frame without
zero-copy rendezvous, or one on fewer rails than its reference, unless the point is there to
measure that.

### CUDA IPC references now available

The benchmark project's `pixi run -e gpu cuda-ipc --out results/cuda-ipc.jsonl` runs two
processes on one host with device buffers. It compares the existing `ucx_perftest` direct
transport test (`put_bw -x cuda_ipc -d cuda -D zcopy -m cuda`) with its active-message test
(`ucp_am_bw -m cuda -H 40 -z -e -I`), with the library's peer-handling and keepalive settings.
The latter records the CUDA-source protocol table and accepts only a confirmed CUDA IPC
payload path. This avoids treating a `cuda_ipc` lane listed for host-memory traffic as a GPU
measurement.

The default is two processes using the same GPU. `--sender-gpu` and `--receiver-gpu` select
physical GPU indices or UUIDs for a PCIe or NVLink pair. Each size has three shuffled runs;
commands, versions, GPU identities, settings and UCX traces are saved. The optional GPU
environment includes the CUDA runtime needed by UCX's benchmark allocation plugin.

These are steady-state references that reuse buffers and exclude IPC handle exchange and
mapping from the timed loop. The three-way comparison below also measures rotating CUDA
rings in the custom UCP baseline and tango-ucx's end-to-end GPU delivery. NVIDIA's
[`simpleIPC`](https://github.com/NVIDIA/cuda-samples/tree/master/cpp/0_Introduction/simpleIPC)
is a native CUDA API example; [`nvbandwidth`](https://github.com/NVIDIA/nvbandwidth) supplies
GPU copy-bandwidth context for multi-GPU runs.

On the development machine, 2026-10-11, UCX 1.22 and CUDA 12.9, both processes used the
same GeForce RTX 2050, pinned to CPUs 0 and 17. Three runs per size, reusable device buffers,
100 warmup frames, and 32 outstanding operations gave these medians:

| Frame | Direct UCT CUDA IPC put | UCP active message |
|---|---|---|
| 1 MiB | 37.96 GiB/s | 36.85 GiB/s |
| 16 MiB | 40.34 GiB/s | 41.32 GiB/s |

These are copies on one GPU across process-owned allocations, not PCIe or NVLink rates or
tango-ucx delivery rates. All 24 runs across 4 KiB, 64 KiB, 1 MiB and 16 MiB completed with
confirmed CUDA IPC paths. Small-message runs varied substantially: the 64 KiB active-message
runs ranged from 6.14 to 18.62 GiB/s. Local data with full traces is in
`tango-ucx-benchmarks/results/cuda-ipc-reference.jsonl`. A separate 12-run smoke sweep,
memory-budget skips, missing-runtime diagnostics and the existing host reference test passed.

### The machine, and how the run went

Once for each host: the PCIe tree of every GPU and NIC with its bridges, NUMA node, and link
speed and width against their maximum; `nvidia-smi topo -m`; the NUMA layout; `ucx_info -d`.
The lowest of the link rates along a path is its ceiling. The development machine's GPU can use
16 lanes and has 4: about 7.9 GB/s.

For each point, from both ends:

| | Recorded |
|---|---|
| Path | Lanes, protocol for the frame size, rails, UCX settings in effect |
| Placement | The CPU of each loop, the NUMA node of the ring, of the NIC and of the GPU |
| CPU | User and system time of each thread, context switches |
| Memory | Peak resident size, locked and pinned bytes, page faults |
| GPU | Memory, utilisation, PCIe traffic |
| NIC | Bytes of each port before and after, retransmits, RoCE congestion counts |
| Library | Health, the outcome, the result of verification |
| Software | Commits, UCX version and build options, CUDA, kernel, compiler and build type |

The NIC's and GPU's own byte counts are what proves a claim about rails.

### The report

One HTML file and a Markdown summary, made from the JSON lines: the machine; tango-ucx as a
share of raw UCX for each transport and family, against a bar of 95% for frames of 1 MiB and
more; rate against frame size; the cost of a small frame; latency; the path of each size;
every point left out or invalid.

## What a host needs

The probe reads each of these before a sweep, lists the ones that are off in the report, and
changes none: most need root. A `sysctl -w` lasts until the next boot.

### Every host

| Setting | Wanted | Why | To see it |
|---|---|---|---|
| Locked memory | Unlimited, or more than both rings | RDMA pins every ring; a registration refused for this does not say so | `ulimit -l` |
| CPU governor | `performance` | A core that changes speed makes a run slower each time it is repeated | `cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor` |
| `kernel.numa_balancing` | 0 | A ring's pages must stay where they were placed | `sysctl kernel.numa_balancing` |
| `kernel.yama.ptrace_scope` | 0 or 1 | CMA reads the other process's memory. UCX asks for the permission itself at 1; at 2 or 3 there is no CMA | `sysctl kernel.yama.ptrace_scope` |
| `/dev/shm`, `kernel.shmmax` | More than both rings | The shared-memory transports | `df -h /dev/shm` |
| The host to itself | Nothing else running; a whole node under Slurm | Another job's load is in every number | `uptime`, `nvidia-smi` |

```sh
sudo cpupower frequency-set -g performance
sudo sysctl -w kernel.numa_balancing=0
echo '* - memlock unlimited' | sudo tee /etc/security/limits.d/90-memlock.conf   # then log in again
```

Under systemd or Slurm the limit is the unit's or the job's: `LimitMEMLOCK=infinity`, or
`PropagateResourceLimits` in `slurm.conf`.

### To profile

| Setting | Wanted | Why |
|---|---|---|
| `kernel.perf_event_paranoid` | 1 or less | At 2, `perf` sees a process's own code and not the kernel under it, where the system calls of a frame are. Ubuntu's 4 refuses everything |
| `kernel.kptr_restrict` | 0 | The names of kernel functions in a profile |
| The library | `pixi run build-profile` | Optimized, with frame pointers |

```sh
sudo sysctl -w kernel.perf_event_paranoid=1 kernel.kptr_restrict=0
```

### Two hosts

| Setting | Wanted | Why |
|---|---|---|
| The environment | The same commits and the same UCX on both | The driver refuses to compare otherwise |
| Reaching them | ssh without a password from the driver's host, or one Slurm allocation | The driver starts both roles |
| Names | Each host resolves the other's name | A Tango device is reached by name |
| Firewall | Everything from the other host, on the test network | Tango's port is chosen for each run, and UCX's TCP listens on ports of the local range. `ucx_perftest` uses 13337, `iperf3` 5201, `ib_write_bw` 18515 |
| MTU | The same on both hosts and the switch; 9000 for RoCE | A smaller one on the way fragments or loses packets |
| `net.core.rmem_max`, `wmem_max` | 64 MiB or more above 10 Gb/s | The TCP profile only |
| Clocks | chrony or PTP, and the offset recorded | Only for latency from one host to the other. Rates need none |

```sh
sudo ufw allow from <the other host> comment 'tango-ucx benchmarks'
sudo sysctl -w net.core.rmem_max=67108864 net.core.wmem_max=67108864
```

### RDMA

| Setting | Wanted | To see it |
|---|---|---|
| Ports | Active, at the speed expected, on both hosts | `rdma link`, `ibv_devinfo` |
| The environment's UCX | Lists `rc_mlx5` or `rc_verbs` for the device | `pixi run ucx_info -d` |
| Locked memory | Unlimited | `ulimit -l`, in the shell that starts the role |
| RoCE flow control | PFC or ECN, the same on both hosts and the switch | The NIC's out-of-sequence and congestion counts, which each point records |

The conda `rdma-core` has not been run against a cluster's kernel drivers.

### GPU

| Setting | Wanted | Why |
|---|---|---|
| Persistence mode | On: `sudo nvidia-smi -pm 1` | The driver otherwise unloads between runs, and the first point pays for it |
| GPU IPC | Both processes of one user, each seeing the GPU | `CUDA_VISIBLE_DEVICES` numbers the GPUs for each process |
| GPU to NIC between hosts | `nvidia_peermem` loaded; the GPU and the NIC under one PCIe bridge | Otherwise UCX stages the frame in host memory. Not checked on any host yet, and neither is what the IOMMU and ACS allow |

### The development machine on 2026-10-10

`perf_event_paranoid` 4, governor `powersave`, locked memory 1.9 GiB, `ufw` active, GPU
persistence off, `/dev/shm` 7.6 GiB, clock by `systemd-timesyncd`. `ptrace_scope` 1 and
`numa_balancing` 0 are as wanted. `lspci -vv` shows a link's state to root only, so the probe
reads `current_link_speed` and `current_link_width` in sysfs.

## What the library needs

`pixi run check` allows 87 public C++ names and 87 are used (`tools/check.py`). So none of this
adds a public name: what the loops measure is written to a file, as UCX does with its own.

### Before the first table

| Step | Where | Why |
|---|---|---|
| Name the two loop threads | `Pub::start`, `Sub::start_loop` | The time and CPU of a thread can be read from `/proc`, but not told apart. `perf` shows the name too |
| Keep every lane of a subscription | `Sub::transport`, 32 characters, in `tables.h` | Two rails of `rc_mlx5` need 35. The publisher keeps 64 |
| A build for profiling | `CMakeLists.txt`, beside `TANGO_UCX_SANITIZERS`; `pixi run build-profile` | `pixi run build` gives no build type where `CMAKE_ARGS` is empty, and so no optimization. `perf` needs frame pointers and symbols too |

### Counts on the loops

Plain integers the loop thread alone writes, always on:

| Count | Shows |
|---|---|
| Loop turns, turns with work, waits, wake signals sent | Whether a loop polls, waits or is woken for each frame |
| Frames that arrived eager, and by rendezvous | Where the frame size falls for this transport |
| Bytes copied into the ring | The eager copy in `on_frame`. tango-bulk had this count. It does not see a copy inside UCX |
| Credit messages sent and received | What the returning traffic costs at each batch size |
| Time under pressure, in total | Today only the time of the current pressure is known, a row at a time |
| Slots refused at the deadline | How often the device waited in vain |

### Times of each stage

Histograms of fixed size, by powers of two of nanoseconds, with no allocation. Off unless asked
for, since each costs a clock read a frame; the suite runs with and without to show the cost.

| Side | Stage |
|---|---|
| Publisher | Waiting for a slot · publish to the loop taking it · send to completion · completion to credit |
| Subscriber | Arrival to landed · landed to delivered · delivered to read · read to release · release to credit sent |

Send to completion has a mean and a maximum today (`Pub::on_send`), over the whole session.

### Where it comes out

`TANGO_UCX_REPORT=<directory>`, read as `TANGO_UCX_IDLE` is. Each publisher and each
subscription writes one JSON document when it ends: its plan, the counts, the histograms, the
CPU its loop was last on, the NUMA node of its ring, and what `ucp_ep_print_info` says of each
endpoint, which names the lanes and what each is for.

### Checks in this project's own tests

A `perf` label, on the pair of `tests/transport/fixtures/pair.h`, with no Tango. Times vary too
much to be a test on a shared runner. Counts do not:

- No allocation on the path of a frame.
- No byte copied into the ring for a frame above the rendezvous size, where the transport has
  zero-copy rendezvous.
- No more than a set number of wake signals a frame.

### What to measure first

The measurements below cover the first two candidates on the local C++ host path.

| Candidate | Where |
|---|---|
| Delivered and released batches signal an eventfd when their queue becomes nonempty, and clear it when it empties: several system calls a frame at batch size 1. The grant queue now uses only its condition variable | `IndexRing::push`, `tables.h` |
| Both ends now sample frame and slot Health about once a millisecond; the subscription previously locked and counted its ring bits every progress turn | `Sub::publish_health` |
| Zero-copy rendezvous through CMA is lost to peer error handling | `Context::Context`, beside `MM_ERROR_HANDLING` |
| The Python subscriber builds a NumPy dtype for every batch's records, and copies the description for every batch | `python/_core.cpp` |

## Order

1. The project, the publisher device and `tango_ucx_perftest -t bw` in host memory, taking a
   host and a port. The first three library steps.
2. The probe with its check of the host's settings, the machine's map, the planner, the checks of the path, and the starting of both
   roles locally and over ssh.
3. The report, and the first whole sweep on the development machine: shared memory and TCP.
4. The counts and the times of each stage in the library, and the `perf` label.
5. GPU source and receive, and GPU IPC (local three-way comparison implemented below).
6. Two hosts with RDMA, one rail and two, `rc_mlx5` and `rc_verbs`.
7. Paced local C++ latency and CPU runs (implemented below), then the Python subscriber and a look at ucTrace.

## Not decided

- The two hosts, and whether they are reached by Slurm or ssh.
- The rates of the paced runs, and the bar of 95%.
- Whether the project is public, and its licence: LGPL-3.0-or-later is assumed.
- `CMA_MEMORY_INVALIDATE=y` in the library. UCX leaves it off; what that guards is not yet read.
- A build type when none is given, as tango-bulk had. It would take the 15 `assert`s out of the
  default build.

## Measured per-frame cost: 2026-10-10

**One microsecond is not a fixed lower bound.** The C++ host path can sustain substantially
less time per frame with subscriber batching. These measurements are the inverse of sustained
frames/s, not individual-frame latency or the CPU time of a particular function.

The experiment used this working checkout, UCX 1.22, `RelWithDebInfo` with `-O2` and frame
pointers, one every subscriber and `max_sessions=1`. Both processes ran on the development
laptop, using `sysv/memory,cuda_ipc/cuda` lanes and host payloads. Four distinct performance
cores ran the publisher loop, producer, subscriber loop and reader. Each point measured one
million frames after 100,000 warmup frames. Five repetitions interleaved saved original and
modified executables, with the order shuffled and each before/after pair adjacent.

The profile of the original path showed substantial kernel and mutex work. On its performance
cores, 46.9% of sampled cycles were in the kernel, and `Pub::control_pass` alone took 9.3%
of sampled cycles across all threads. Two changes in `tables.h` address observed work:

- `IndexRing::push` writes its eventfd only when an empty queue becomes nonempty. Its fd stays
  readable until the last index is removed. Condition-variable notification still occurs on
  every push, preserving concurrent waiters, and explicit shutdown signals are unchanged.
- `ControlTable::drain` reads an entry's state before attempting compare-exchange. Inactive
  entries no longer execute eight locked read-modify-write operations each publisher loop turn.
  Posted requests still use the original compare-exchange and ownership protocol.

Median sustained microseconds per frame, including the benchmark's per-frame clock reads:

| Frame | Depth | Batch | Original | Modified |
|---|---:|---:|---:|---:|
| 1 KiB | 32 | 1 | 1.340 | 0.971 |
| 1 KiB | 32 | 8 | 0.949 | 0.766 |
| 1 KiB | 32 | 32 | 0.843 | 0.458 |
| 1 KiB | 128 | 1 | 1.297 | 1.443 |
| 1 KiB | 128 | 8 | 0.701 | 0.378 |
| 1 KiB | 128 | 32 | 0.794 | 0.371 |
| 4 KiB | 32 | 1 | 1.512 | 1.155 |
| 4 KiB | 32 | 8 | 0.982 | 0.829 |
| 4 KiB | 32 | 32 | 0.909 | 0.649 |
| 4 KiB | 128 | 1 | 1.066 | 0.631 |
| 4 KiB | 128 | 8 | 0.893 | 0.555 |
| 4 KiB | 128 | 32 | 0.862 | 0.766 |

The host was busy and thermal sensors reached about 100 degrees C despite the performance
governor. The 1 KiB, depth 128, batch 1 point is inconclusive: original runs ranged from
0.967 to 3.894 us/frame, modified runs from 0.605 to 2.245. A focused seven-run repeat of that
point, with two million measured frames and `perf` recording both builds, gave medians of
1.034 and 0.722 us/frame, with ranges of 0.984–2.499 and 0.694–1.987 respectively. Keep both
results; neither establishes a guaranteed single-frame rate. The batch 8 point at depth 128
was clearer: original 0.640–0.800, modified 0.363–0.567 us/frame.

The matched AM reference, `ucx_perftest -H 40 -z -e -I -y`, measured medians of 0.095 us/frame
at 1 KiB and 0.332 at 4 KiB, with the library's shared-memory error-handling settings. `-y`
adds the eager receive copy the library makes. The reference still reuses one buffer and has
fewer application threads, so its difference from the library is a throughput gap, not an
isolated measurement of library overhead.

CPU cost is a separate number. At 1 KiB, depth 128, batch 8, the four hot threads together
used approximately 2.01 CPU us/frame originally and 1.21 after the changes (medians, with each
side normalized over its own measurement window). Their work overlaps, producing the 0.701
and 0.378 us/frame throughput figures. A CPU budget must use the thread times, rather than
inverse throughput.

For current single-frame delivery, budgeting around a microsecond remains reasonable until
the deployment machine is measured. Batching amortizes delivery, release and credit handling,
but can delay delivery while the batch fills; this first experiment did not measure paced
latency. The following experiment addresses the unused grant-queue fd and the subscriber's
per-turn Health mutex and bit counts. Queue lock contention remains a target. GPU, RDMA,
multiple subscribers and Python need their own measurements.

The benchmark project now has `frame_cost.py` for this matrix. To repeat with the saved local
executables, choose four physical cores allowed by the driver and a new output filename:

```sh
pixi run taskset -c 0,2,4,6 python frame_cost.py \
    --build before=build-frame-cost-before --build after=build-frame-cost \
    --sizes 1K,4K --depth 32,128 --batch 1,8,32 --frames 1000000 --runs 5 \
    --thread-cpus 0,2,4,6 --reference --out results/frame-cost-repeat.jsonl
```

The output records executable hashes, settings, affinity assignments, shuffle seed, Health
and thread CPU times. Add `--resume` to continue interrupted points with the same inputs.
Local artifacts in `tango-ucx-benchmarks/results/` are `frame-cost-pinned.jsonl`,
`frame-cost-interleaved.jsonl` (inherited affinity), `frame-cost-batch1-confirmation.jsonl`,
and the corresponding text and `perf` files. Both builds' source hashes are in
`frame-cost-source-before.json` and `frame-cost-source-after.json`.

Validation: all 103 library CTest tests and all eight benchmark CTest tests passed, including
payload verification and deliberately damaged frames. Architecture and formatting checks
passed. The fd test also checks that removing one of two queued indices leaves it readable.

## CPU cost at fixed rates: 2026-10-10

**Polling between frames can dominate CPU use even when delivery is fast.** The default
progress loops poll for 200 us after their last work. At 10,000 frames/s, the 100 us arrival
interval usually keeps both loops polling continuously. Immediate event waits preserve that
rate with substantially less CPU, at a cost in typical delivery latency.

The library now supports `TANGO_UCX_IDLE=wait`, set before creating transports. A loop drains
available work, then arms UCX and waits as soon as a pass finds no work. The existing wakeup
protocol, timer bounds and GPU event checks remain in place. The default is still the 200 us
polling window, and `TANGO_UCX_IDLE=spin` still polls continuously. Two further changes remove
work: publisher grants use a condition variable without an unused eventfd, and subscriber
Health locks and counts ring bits about once a millisecond rather than every progress turn.
Setup, Ready and outcome changes force a subscription observation immediately; topology
retains its separate one-second sampling interval.

`cpu_cost.py` in the benchmark project compares saved builds and modes. This experiment used
1 KiB host frames, depth 128, batch 1, one every subscriber, and the same four physical
performance cores, optimized builds and UCX as the earlier experiment. Each paced point
measured two seconds after warmup, with three shuffled repetitions per build/mode/rate.
The baseline build includes the earlier queue-signaling and control-table changes; it excludes
the new grant-fd and Health changes. Both the current default and current wait modes use the
same executables. All points completed and achieved their requested average rate within 5%.

The producer timestamps each publication with the same host's steady clock; latency ends
when the application reads the batch. It excludes source pacing delays and includes transport,
scheduling and application wakeups. CPU us/frame sums the four hot threads, normalizing the
subscriber over measured frames and the publisher over all published frames including warmup.
Multiplying by achieved frames/s gives an estimate of occupied cores. Latency collection is
included in reader CPU time, while percentile sorting is excluded.

Medians of three runs, comparing the current library's default and wait modes:

| Frames/s | Default CPU cores | Wait CPU cores | CPU reduction | p50 default / wait (us) | p99 default / wait (us) |
|---:|---:|---:|---:|---:|---:|
| 1,000 | 0.434 | 0.032 | 93% | 15.3 / 12.6 | 1,322 / 200 |
| 10,000 | 2.020 | 0.223 | 89% | 4.5 / 9.6 | 977 / 792 |
| 50,000 | 2.425 | 1.990 | 18% | 9.3 / 19.7 | 1,508 / 1,681 |

At 10,000 frames/s, default progress threads alone used about 196 CPU us/frame, versus about
17 in wait mode. Total cost was 202 versus 22 CPU us/frame. For comparison, at saturation the
four threads used only a few CPU us/frame: the much larger paced cost comes largely from
polling between frames. Shorter arrival intervals leave less opportunity to sleep, and at
50,000 frames/s frequent wakeups also cost appreciable CPU. The grant-fd and Health changes
alone did not establish a consistent CPU reduction under default polling at fixed rates.

A focused seven-run unpaced check, with two million measured frames each, gave median
inverse-throughput costs of 0.869 us/frame for the baseline, 0.798 for the current default,
and 0.715 for wait mode. Median CPU costs were 2.74, 2.43 and 2.26 CPU us/frame respectively.
The throughput ranges overlapped widely: 0.524–1.746, 0.691–1.807 and 0.657–1.980 us/frame.
These runs show no reliable peak-throughput gain or regression, and do not justify changing
the default for every workload.

The first source used inherited Linux timer slack and often delivered bursts at 50,000
frames/s: roughly two-thirds of its wakes were over one frame period late. A repeat requested
1 ns timer slack for the producer thread only and verified that setting in every result.
At 50,000 frames/s, 1.8–5.9% of wakes were still more than one period late. The table uses
this repeat; it measures an average paced rate, rather than guaranteed uniform arrivals.

Thermal-throttle time counters increased in 24 of the 36 paced/unpaced invocation windows,
confirming actual throttling on the host. Temperatures alone in the earlier experiment did
not establish it. The counters and temperatures cover the whole laptop, including other
workloads and heat from previous
points. They do not prove how much this library caused, or that wait mode prevents throttling.
Latency tails are also noisy: the three 10,000 frames/s p99 values ranged from 695–2,664 us
in default mode and 343–894 us in wait mode. These are not deployment latency guarantees.

Use wait mode for sparse or paced streams when its measured latency meets the application
requirement. Retain default polling for workloads that need its latency or peak throughput.
Subscriber batching remains useful for amortizing delivery and release costs, with the
additional latency of filling or timing out a batch. Measure CPU us/frame, achieved rate and
p99 together on the deployment host; temperature is supplementary evidence.

To repeat the local comparison with the saved builds:

```sh
pixi run taskset -c 0,2,4,6 python cpu_cost.py \
    --variant before=build-cpu-before,default --variant after=build-cpu-after,default \
    --variant wait=build-cpu-after,wait --rates 0,1000,10000,50000 --seconds 2 --runs 3 \
    --size 1K --depth 128 --batch 1 --thread-cpus 0,2,4,6 \
    --out results/cpu-cost-repeat.jsonl
```

Artifacts in `tango-ucx-benchmarks/results/` are `cpu-cost-paced-tight-timer.jsonl` and its
text summary; the initial coalesced-source results are `cpu-cost-paced.jsonl`. The focused
peak-throughput check is `cpu-cost-unpaced-confirmation.jsonl` and its text summary. Build
metadata retains executable hashes; library source hashes are `cpu-cost-source-before.json`
and `cpu-cost-source-after.json`. Benchmark source hashes and copies of the final Python
drivers are saved beside them. Package throttle counters and temperatures are recorded
before and after each invocation, including setup and warmup.

Validation: all 104 library CTest tests passed in default mode; all 85 UCX and Tango tests
also passed with `TANGO_UCX_IDLE=wait`. All ten benchmark tests passed, including paced
payload verification and wait-mode eager, fragmented and rendezvous delivery. Paced
verification passed again after tightening the benchmark producer's timer slack. The new
grant-ring test checks both blocking acquire and terminal notification without an fd.
Architecture, Python compilation and C++ formatting checks passed.

## Matching the raw UCX comparison: 2026-10-10

**The stock test's hot working set accounts for a substantial part of the large-frame gap.
A substantial gap remains for small frames, and similar bandwidth can hide higher CPU use.**
The benchmark project now measures stock UCX, a custom rotating-ring raw UCP baseline, and
the full library separately. The raw baseline is our application using UCX directly, not
an altered or renamed `ucx_perftest`.

The installed UCX 1.22 reports revision `8a6b06f`, resolved to
`8a6b06fb880accbb933a79cda893883872c68d9d`. Its
[stream test](https://github.com/openucx/ucx/blob/8a6b06fb880accbb933a79cda893883872c68d9d/src/tools/perf/lib/ucp_tests.cc#L799)
reuses the same send and receive pointers each iteration. Its eager handler copies to that
receive buffer only with `-y`; rendezvous also targets the same receive buffer. The
[allocator](https://github.com/openucx/ucx/blob/8a6b06fb880accbb933a79cda893883872c68d9d/src/tools/perf/lib/libperf_memory.c#L100)
allocates a message-sized buffer per test thread. `-O` bounds outstanding sends; it does not
provide rotating frame buffers. `-i` spaces scatter-gather entries within a message and does
not rotate messages through a larger working set.

| Level | Active payload memory | Delivery work | Hot threads |
|---|---|---|---:|
| Stock UCX, matched flags | One send and one receive buffer used for payload | UCX completions and optional eager copy; no application release handoff | 2 |
| Custom `ucp_ring` | Registered rotating source and receive rings | In-order consumption, eager copy or rendezvous into the ring, credit after consumption, local completion before source reuse | 2 |
| Library | Registered source and receive rings | Separate producer/progress and progress/reader threads, slot/batch ownership and release queues, metadata and observations | 4 |

The stock command uses `-t ucp_am_bw -H 40 -z -e -I -y -O 128`,
`UCX_MM_ERROR_HANDLING=y` and `UCX_KEEPALIVE_INTERVAL=2s`. The custom baseline also uses
a 40-byte frame header, registered host memory, peer error handling, wakeup support, eager
receive copies and rendezvous receives. Its raw credit window is fixed at 128 in the main
comparison. The library has one every subscriber and ring depth 128. Each role runs on the
same physical performance CPU as its library progress counterpart; the library's producer
and reader use two additional physical cores. UCX, compiler and library builds are unchanged
from the previous measurements. This is genuine cross-process shared-memory communication.
Tango setup is outside the timed frame loop and there are no per-frame Tango RPCs.

The current bandwidth benchmark, including the library's default runs, fills source slots
once and reuses them. It does not represent new detector data in every frame. The fresh-frame
experiment below fills and checks every payload. Neither benchmark establishes GPU or RDMA
performance.

Five shuffled repetitions, medians of inverse throughput in us/frame:

| Frame | Stock UCX | Raw rotating UCP | Library batch 1 | Library batch 8 | Raw / library payload working set |
|---|---:|---:|---:|---:|---:|
| 1 KiB | 0.115 | 0.181 | 0.771 | 0.474 | 256 KiB |
| 4 KiB | 0.337 | 0.291 | 0.617 | 0.734 | 1 MiB |
| 256 KiB | 15.983 | 36.552 | 36.340 | 36.924 | 64 MiB |
| 1 MiB | 54.148 | 123.315 | 126.605 | 131.041 | 256 MiB |

The stock active payload footprint is only two frames: 2 KiB, 8 KiB, 512 KiB or 2 MiB
respectively. Footprints count application payload, excluding UCX internal buffers and other
metadata. The laptop has 2 MiB L2 on a performance core and 18 MiB shared L3. The small raw
and library rings also fit within cache capacity, so the stock test's smaller footprint does
not explain the remaining small-frame gap. The raw baseline still omits thread handoffs and
much of the library's state management. Subtracting its inverse throughput is not a timer
for those functions.

The host remains variable. At 1 KiB, library batch 1 ranged from 0.687–1.877 us/frame and raw
UCP from 0.176–0.187. At 256 KiB the ranges were 35.795–44.559 and 33.541–57.013; at 1 MiB,
125.688–134.175 and 121.273–134.612. The 4 KiB batching results overlap and establish no
batching improvement. Close large-frame medians show that the original stock comparison
overstates the throughput gap; they do not establish identical CPU efficiency.

### Isolating cache capacity

At constant 256 KiB message size, raw credit window 8, idle policy, executable and two CPUs,
only the ring sizes changed. Five interleaved repetitions measured 16,384 frames each:

| Slots per ring | Total active payload | Median us/frame | Range | Median CPU us/frame |
|---:|---:|---:|---:|---:|
| 8 | 4 MiB | 21.297 | 20.884–22.669 | 42.32 |
| 128 | 64 MiB | 34.399 | 33.954–34.758 | 68.29 |

The larger working set increased frame time by 62%, with no overlap in these runs. Three
additional `perf stat` repetitions per case, 32,768 measured frames each, recorded median
`cpu_core/cache-misses/` counts of 2.68 million and 271.01 million, about 101 times as many.
Counter time running was 99–100%. Counts include initialization and warmup and both role
processes; they are not measured-window DRAM-byte counts. The combination of a controlled
working-set change and hardware counters supports a cache-capacity effect.

A separate 1 KiB test held the raw credit window at 128 and compared 128 versus 32,768 slots
per ring: 256 KiB versus 64 MiB total payload. Five runs gave 0.352 versus 0.427 us/frame,
and 0.70 versus 0.85 CPU us/frame. Small-message fixed work remains dominant. Absolute raw
rates differed from the earlier matrix on this shared host; use the interleaved comparison
within each experiment. The library's publisher caps source slots at 1024, so this last
experiment isolates cache behavior in the raw baseline rather than pretending to match
two 32,768-slot library rings.

### Fresh payloads and CPU cost

`--verify` calls the same `bench::fill` and `bench::matches` routines on every raw and library
frame. Generation and checking are inside the timed work. Stock UCX is omitted because it
does not do that work. Three shuffled repetitions at depth 128:

| Frame | Raw UCP us/frame / CPU us/frame | Library batch 1 us/frame / CPU us/frame | Library batch 8 us/frame / CPU us/frame |
|---|---:|---:|---:|
| 1 KiB | 0.221 / 0.44 | 0.890 / 2.77 | 0.408 / 1.34 |
| 256 KiB | 49.353 / 97.56 | 48.522 / 166.45 | 46.941 / 156.89 |

At 256 KiB, similar throughput coexists with substantially more library CPU. The separate
application/progress threads overlap their work and can poll while other stages fill, copy
or check payloads. Cache effects therefore do not excuse the remaining CPU cost.

Raw CPU includes both role processes, receiver time over measured frames and sender time
over all published frames including warmup. Library CPU sums the four hot threads with the
same receiver/sender normalization, excluding other helper threads. Raw rings are prefilled
before timing, whereas the library's initial source fills happen during warmup. CPU scope
and setup differ and remain recorded. Stock CPU covers the whole invocation, including
setup and warmup, and is stored separately; its measured-window CPU column is left empty.

### Paced delivery at the same rate

At 10,000 frames/s, 1 KiB, depth 128 and batch 1, three two-second repetitions gave:

| Benchmark / idle mode | CPU us/frame | Approximate occupied cores | p50 publish-to-read (us) | p99 (us) |
|---|---:|---:|---:|---:|
| Raw UCP / default 200 us polling window | 198.71 | 1.99 | 0.5 | 0.9 |
| Library / default 200 us polling window | 195.21 | 1.95 | 4.3 | 1,524 |
| Raw UCP / immediate waits | 12.78 | 0.128 | 3.1 | 878 |
| Library / immediate waits | 23.26 | 0.233 | 10.4 | 1,586 |

Both default modes spend about two cores polling between frames. Immediate waits cut CPU in
both, while the library retains more CPU and application-delivery latency. The raw producer
shares the polling thread and checks its pacing deadline there; the library has a sleeping
producer thread plus a progress thread. The raw consumer also reads directly on its progress
thread. This difference in scheduling is part of what the table measures, not a claim that
the two delivery APIs are interchangeable.

Raw default p99 ranged from 0.8–1.1 us; library default from 1,435–1,918. In wait mode,
ranges were 387–1,406 and 1,135–1,831. These are host-specific results, including scheduling
contention. All points achieved the requested average rate within 5%, with source lateness
retained in the output. Stock UCX supports sleep progress but has no paced-source or rotating
frame-buffer CLI option, so its unpaced run is not compared to paced library CPU or latency.

### Reproduction and validation

In `tango-ucx-benchmarks`, using the isolated build linked to this library:

```sh
pixi run taskset -c 0,2,4,6 python compare_ucx.py --build build-ucx-comparison \
    --runs 5 --thread-cpus 0,2,4,6 --out results/ucx-comparison-repeat.jsonl
pixi run taskset -c 0,2,4,6 python compare_ucx.py --build build-ucx-comparison --raw-only \
    --case 256K:8 --case 256K:128 --window 8 --frames 16384 --batch 1 --runs 5 \
    --thread-cpus 0,2,4,6 --out results/ucx-cache-repeat.jsonl
```

For paced runs add `--case 1K:128 --rates 1000,10000 --modes default,wait --batch 1`.
For fresh payloads add `--case 1K:128 --case 256K:128 --verify`. Each output file must be new.
The benchmark README describes all three levels and remaining differences.

Artifacts in its ignored `results/` directory: `ucx-comparison-matched.jsonl` (80 points),
`ucx-comparison-working-set.jsonl` (10), `ucx-comparison-cache-256K.jsonl` (10),
`ucx-comparison-paced.jsonl` (24), `ucx-comparison-fresh.jsonl` (18), their text summaries,
and `ucx-cache-counters.jsonl` with six per-run counter files. Every comparison point passed.
Metadata records executable/source hashes, commands, CPU assignments, protocols, thermal
snapshots and the different CPU accounting scopes.

All 11 benchmark CTests passed. The new baseline test checks payload integrity and source
reuse with one and multiple slots, eager, fragmented and rendezvous frames, both idle modes,
and paced latency sample counts. The baseline links directly to UCX, without Tango or the
library transport. Library code is unchanged in this comparison.

## CUDA IPC delivery comparison: 2026-10-11

The benchmark now compares stock `ucx_perftest`, rotating raw UCP CUDA rings and the full
tango-ucx GPU slot/view path. It uses the library's existing GPU APIs, including producer
readiness and consumer release events. Raw UCP consumes immediately after a completed
transfer; the library returns credit after the consumer event completes. These differences
are retained in the report rather than treated as identical application delivery.

On the development machine, both processes used the same GeForce RTX 2050, UCX 1.22.0
revision `8a6b06f`, CUDA 12.9 and driver 580.178.04. Publisher progress, producer, subscriber
progress and reader used physical cores 0, 2, 4 and 6. Stock and raw UCP used cores 0 and 4.
There were three shuffled repetitions of 16,384 measured frames and 1,640 warmup frames per
variant. Payloads were prefilled and reused; setup and IPC mapping were outside the timed loop.
All 60 points completed and confirmed `cuda_ipc/cuda` in their CUDA-source protocol tables.

Median GiB/s:

| Frame | Slots per ring | Stock UCX | Rotating raw UCP | Library batch 1 | Library batch 8 |
|---|---:|---:|---:|---:|---:|
| 1 KiB | 128 | 0.22 | 0.21 | 0.18 | 0.16 |
| 4 KiB | 128 | 0.67 | 0.81 | 0.54 | 0.59 |
| 256 KiB | 128 | 62.19 | 34.40 | 32.36 | 32.69 |
| 1 MiB | 128 | 39.91 | 40.14 | 40.01 | 40.09 |
| 16 MiB | 32 | 41.72 | 38.84 | 39.42 | 40.25 |

At 1 MiB the library reached 100.2% and 100.4% of stock bandwidth, or 99.7% and 99.9% of
rotating raw UCP. Its batch-1 range was 39.90–40.07 GiB/s, versus 39.77–39.93 for stock and
39.60–40.18 for raw UCP. These overlapping ranges establish similar bandwidth on this GPU.
At 16 MiB, batch 8 reached 96.5% of stock and 103.6% of raw UCP; its range was
40.07–40.36 GiB/s. Batch 1 reached 94.5% of stock. This is same-GPU cross-process copy
bandwidth; PCIe/NVLink GPU pairs and GPU traffic between hosts have not been measured here.

Stock still has a much smaller payload working set. At 256 KiB it reuses 512 KiB across
both roles, while raw UCP and the library rotate 64 MiB. The library reaches about 95% of
the rotating baseline and 53% of stock at that size. This experiment does not isolate a GPU
cache cause: stock ranged from 40.26–62.50 GiB/s and raw from 20.02–36.60. Small-frame runs
also varied, and these three repetitions establish no consistent batch-8 improvement there.

Similar bandwidth retains a CPU difference. At 1 MiB, raw UCP used 48.52 CPU us/frame,
library batch 1 used 69.52, and batch 8 used 55.28. Raw CPU covers two role processes;
library CPU covers four hot application/progress threads. Receiver CPU excludes warmup,
while sender CPU includes it. Library producer initialization and first fills occur during
that interval; raw ring setup and fills precede its timer. Stock's whole-invocation CPU is
stored separately and is excluded from this comparison column.

To reproduce in `tango-ucx-benchmarks`:

```sh
TANGO_UCX_SOURCE=/path/to/this/checkout pixi run -e gpu cuda-compare \
    --case 1K:128 --case 4K:128 --case 256K:128 --case 1M:128 --case 16M:32 \
    --frames 16384 --runs 3 --out results/cuda-comparison-repeat.jsonl
TANGO_UCX_SOURCE=/path/to/this/checkout pixi run -e gpu test-gpu
```

The GPU build and install use separate `build-gpu/` and `stage-gpu/` directories. The driver
selects four allowed physical cores automatically, or accepts `--thread-cpus p,g,s,r`.
`--sender-gpu` and `--receiver-gpu` select physical GPU indices or UUIDs. It checks actual
library ring depths, aligns warmup across variants, matches the outstanding window, and
requires at least two batches per GPU ring. Rings exceeding half the free GPU memory are
recorded as skipped. Normal timing includes completion of the final consumer stream event.
`--verify` adds fresh host generation/upload and full download/check work and omits stock UCX.

Artifacts are `tango-ucx-benchmarks/results/cuda-comparison-2026-10-11.jsonl`, its Markdown
report and text output. They retain full UCX traces, commands, GPU identity, placement,
working sets, source/executable hashes and thermal snapshots. All three repetitions had
matching requested and actual ring depths and the same warmup count; recorded hashes still
matched after the sweep.

Validation: all 12 benchmark CTests passed in the CUDA build and all 11 passed in a separate
non-CUDA build. CUDA verification exercises one-slot and multi-slot raw reuse, both idle
modes, small and large rendezvous payloads, library batching, explicit CPU placement and
intentional partial-word corruption. Disabling IPC produced a successful staged transfer
through SysV, which the path validator rejected. Insufficient ring depths were rejected
before allocation, and an oversized-ring case was recorded as skipped. Python compilation,
C++ formatting and diff whitespace checks passed.

## Related work

ucTrace attributes UCX traffic to devices and to the calls that caused it, by interposing on
UCT. It needs its own fork of UCX 1.16 and this library needs 1.21, so it is cited and not
used: E. Gencer, M. K. T. Issa, I. Turimbetov, J. D. Trotter and D. Unat, "ucTrace: A
Multi-Layer Profiling Tool for UCX-driven Communication", IPDPS 2026,
[arXiv:2602.19084](https://arxiv.org/abs/2602.19084).
