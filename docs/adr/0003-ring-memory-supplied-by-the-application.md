<!--
SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-ucx project

SPDX-License-Identifier: LGPL-3.0-or-later
-->

# Ring memory supplied by the application

Status: **proposed**

## Context

Every ring's memory is chosen by the library today. A receive ring is `new std::byte[]`, or
`gpu_storage` for `cuda:N` and `pinned:N`, and then registered with `Region::adopt`
(`Sub::setup`, `src/transport/subscription.cpp:211-229`). A GPU source ring is `gpu_storage` and
`Region::adopt` (`Pub::setup`, `src/transport/publisher.cpp:71-79`); a host source ring, lent or
not, is a region of the arena that `Region::allocate` makes (`src/transport/engine.cpp:216`).
So the registration path already takes memory the library did not map itself, with an owner
(`std::shared_ptr<void>`) that keeps it alive; only the choice of allocator is closed.

Applications keep asking for that choice:

- **A relay** subscribes to one stream and publishes it again, so that one session crosses the
  detector's link and fan-out costs the consumers' side (tango-bulk's M6, deferred with "whether
  it re-registers or forwards registered memory is a measurement"). With library memory on both
  sides it copies every frame from a receive slot into a source slot. With one memory as both
  the subscription's receive ring and the publisher's lent ring, it receives into slot k,
  publishes from buffer k with `publish_from`, and releases the batch in `done`: no copy.
- **Placement.** The arena's NUMA node is wherever its helper thread runs (`TODO(numa)` in
  `src/transport/engine.cpp`), and receive rings follow the creating thread. An application
  that places its writer threads, huge pages or direct-I/O alignment cannot place the ring.
- **GPU pipelines** keep their own stream-ordered pools (CuPy, PyTorch). A ring from `gpu_storage`
  sits beside the pool instead of in it.
- **Storage writers** (tango-ucx-storage-demo, 2026-11-21) write shards and chunks straight from
  the receive ring. Page-aligned or huge-page rings would let them use direct I/O on the same
  path; today they cannot ask for it.

The GPU work already settled the hard part, the lifetime: a batch is released only once its
last view is gone and every event recorded by its GPU views has completed, and `release_after`
does the same for pinned uploads. A lent ring hands a buffer back through `done` once the
frame's last send has completed. Memory the application supplies needs exactly these rules, and
no others.

## Decision

**Ring memory is a choice of two independent things:** who provides it (the library allocates;
the library allocates and lends it to the device; the application supplies it) and what it is
(host, pinned host, CUDA). Supplied memory is the third provider, for both sides:

| | host | pinned host | CUDA |
|---|---|---|---|
| The library allocates | receive and source rings | receive (`pinned:N`) | receive and source |
| The library lends it to the device | source (lent ring) | — | — |
| **The application supplies it** | **receive and source** | **receive** | **receive and source** |

**The application supplies memory through a provider, called with the size the library
planned.** `SubscriptionOptions::memory` and `PublisherLimits::memory` take a
`MemoryProvider`: a function of `(bytes, MemoryKind)` that returns a `SuppliedMemory`, an owner
(`std::shared_ptr<void>`, whose deleter gives the memory back) with its size and kind. The
library plans the ring from the budget exactly as now, then asks for `plan.payload_bytes` (a
subscription) or the arena's size (a publisher). Sizing stays in one place, and the application
never has to work out a ring's size itself. Memory larger than asked for is accepted; only the
planned bytes are used.

**What stays the library's:** the plan and its depth, the records, the operation tables, the
batch rows and GPU events, and the registration. The provider gives payload memory only.

**The library registers supplied memory once per UCX context and keeps its owner** for as long
as the ring may be read or written: until the subscription is closed and its last receive has
landed, or the arena is replaced and nothing of it is in flight. Under quarantine it keeps the
owner until the process exits, as it keeps its own memory; the application's deleter then never
runs. Supplied host memory counts towards `RLIMIT_MEMLOCK` as allocated memory does
(`reserve_pin`).

**The application may not write supplied memory the library holds.** For a receive ring that
is every slot not released; for a source ring every slot acquired, published or in flight, and
for a lent ring the buffer until `done`. Release, borrow, GPU views and `release_after` are
unchanged.

**Checks at setup:** the memory is at least the planned size, its kind matches the subscription
(`gpu`, `pinned`) or the publisher (`gpu`), CUDA memory belongs to the GPU named, and host memory
is page-aligned. A failed check refuses the subscription or `begin`, as an unsupported option
does now.

**A publisher's supplied memory is its arena.** ADR 0001 is kept: the arena is supplied once,
lives as long as the publisher and holds the source ring of each instance, and `begin` asks the
provider again only when a ring needs a larger arena. The library still faults supplied host
memory in, on its helper thread. With `lend_source`, the lent ring's buffers are the supplied
memory, so a device can supply buffers it already owns, such as a camera SDK's.

**The wire does not change.** Open still says only whether the receive ring is GPU memory.

**DLPack is how memory comes in, as it is how frames go out.** A GPU view already exports a
batch as DLPack 1.0 (`DLManagedTensorVersioned`, read-only, the batch held until the
consumer's deleter runs; `python/_core.cpp`, `PythonGpuView::dlpack`). Supplied memory is the
same contract in the other direction: a `DLManagedTensorVersioned` is an owner whose deleter
gives the memory back, with a pointer, a size and a device. So:

- In C++, `SuppliedMemory` can be made from a `DLManagedTensorVersioned *`; the library calls
  its deleter when it gives the memory back. Any library that speaks DLPack's C ABI can supply
  ring memory: PyTorch's `toDLPack`, Holoscan's tensors, MatX, RAPIDS.
- In Python, `memory=` takes an object with `__dlpack__`, or a callable `(bytes, kind)` that
  returns one. The library calls `__dlpack__(max_version=(1, 0), copy=False)`, so the producer
  must hand over its own memory and never a copy, and renames the capsule as a consumer does.
  NumPy, CuPy, PyTorch, JAX and RMM all qualify; so does memory from other owners once wrapped,
  such as a NumPy view of an mpi4py shared window or of DPDK huge pages.
- DLPack's device gives the memory kind:

  | DLPack device | Memory kind | Rings |
  |---|---|---|
  | `kDLCPU` | host | receive, source, lent |
  | `kDLCUDAHost` | pinned host | receive (`pinned:N`) |
  | `kDLCUDA`, `device_id` N | CUDA on GPU N | receive (`cuda:N`), GPU source |
  | `kDLCUDAManaged` and others | refused until tested | |

- The tensor must be writable (no `DLPACK_FLAG_BITMASK_READ_ONLY`), compact (no strides, or C
  order), at least the planned size counting `byte_offset`, and, for host memory, page-aligned.
  Its dtype and shape are not used: the ring is bytes.
- For CUDA memory the library passes its stream to `__dlpack__` and synchronizes once at setup,
  so the producer's pending work on the memory is complete before UCX writes to it. Afterwards
  the memory is the library's until it is given back.

Round trip: a CuPy pool allocation supplied as the ring, frames received into it, and batches
exported as read-only DLPack views of that same memory to CuPy again. Neither direction copies.

## Interoperating with other owners of memory

The interoperability is in memory only. The wire stays tango-ucx's own: sharing UCP is not
interoperability (tango-bulk RFC §11.2). The RFC already claims that a frame in a device
allocation is consumable by other libraries through its pointer (§11.1), and its MPI design
needs this ADR: "the receive ring is an `MPI_Win_allocate_shared` window every rank reads in
place" (§11.3) cannot be built while the library allocates every receive ring.

It goes both ways:

| Direction | Needs | Examples |
|---|---|---|
| Another library owns the memory; tango-ucx uses it | Supplied memory, as a DLPack tensor | an MPI shared window, one session per node (RFC §11.3); `ncclMemAlloc` buffers; RMM, CuPy and PyTorch pools, and Holoscan's tensors; DPDK huge-page memory as the source ring of a detector's UDP receiver |
| tango-ucx owns the memory; another library registers it | The ring's memory, visible and aligned | io_uring fixed buffers for direct-I/O writes; cuFile buffer registration for GPUDirect Storage; NIXL's memory registration; `ncclCommRegister`; `MPI_Win_create` |

For the second direction this ADR also decides:

- **A subscription shows its receive ring:** `Subscription::receive_ring()` gives its base,
  bytes and memory kind, valid until the subscription is closed, as `Publisher::lent_ring()`
  does for a lent ring. A library that registers existing memory registers the whole ring once.
  In Python it is a read-only DLPack export of the ring's bytes that keeps the subscription's
  ring alive, like a GPU view's export keeps its batch.
- **Library-allocated host rings are page-aligned.** A receive ring is `new std::byte[]` today,
  which does not promise page alignment (glibc places a header before large allocations), so
  direct I/O from the ring is refused.

**Completion follows the existing rules** (RFC §7.5: a view is not released before every
consumer of it has finished). Stream-ordered GPU work, such as NCCL, cuFile's stream API or
PyTorch, uses GPU views and `release_after`, as now. Asynchronous host work, such as an io_uring
write, a NIXL transfer or other MPI ranks reading a shared window, holds the batch until its
completion arrives, and drops it there; a batch may be released from any thread.

**Several UCX users in one process.** MPI, NIXL and Holoscan load the same `libucx` with contexts
of their own. tango-ucx reads its configuration under `TANGO_UCX`
(`ucp_config_read("TANGO_UCX", …)`, `src/transport/ucp.cpp:42`), so their tuning does not reach
it; whether a plain `UCX_*` setting still applies underneath is the RFC's open question (§8.3).
All of them must use one UCX build, and memory both register counts twice under `reserve_pin`.

**To be tested, kind by kind:** memory UCX may not register as `UCS_MEMORY_TYPE_CUDA`, such as
the CUDA virtual-memory allocations `ncclMemAlloc` makes, managed memory and DMA-BUF exports.

## Consequences

- **A relay of array streams needs no copy.** One supplied memory is both the receive ring
  (depth slots of `frame_bytes`, back to back) and the publisher's lent ring (buffers of a slot,
  back to back), registered in both contexts. Opaque streams arrive in a packed ring, which has
  no buffer indices: relaying them needs `publish_from` with an offset, a later change.
- **NUMA, huge pages and alignment become the application's choice.** `TODO(numa)` remains for
  library-allocated memory only.
- **Memory registered in two contexts is counted twice** by `reserve_pin`. Counting by address
  range is possible but not needed for 0.1.
- **An application pool must tolerate quarantine:** a quarantined ring's memory is never given
  back.
- **Testing:** a test provider that poisons memory in its deleter and checks it was never written
  after release finds use after release in either direction.

## Considered

- **Library allocators only (today).** No placement control, and a relay copies every frame.
- **The application passes a buffer and the depth follows from its size** ("the budget is the
  buffer"). It would also answer the sizing complaint, but it makes a second sizing rule beside
  the budget, and the tables still need one. Revisit if budgets are replaced by depths.
- **Registering whatever the application sends from or receives into, on demand** (UCX's
  registration cache). The implicit cache ADR 0001 rejected: no owner for pinned memory, and
  memory-hook and fork trouble.
- **Registering per frame.** A registration per send or receive costs more than the frame at
  small sizes, and loses the pre-faulted arena.
- **The buffer protocol or `__cuda_array_interface__` for supplied memory.** The buffer
  protocol is host only; the CUDA array interface carries no owner and no read-only flag.
  DLPack 1.0 has both, covers host, pinned and CUDA memory, has a C ABI for C++ libraries, and is
  what tango-ucx already exports.

Prior art: MPI has both `MPI_Win_allocate` (library memory, which may be special) and
`MPI_Win_create` (the application's memory), the same split as here. NCCL's
`ncclCommRegister`, NIXL's registered memory descriptors and libfabric's `fi_mr_reg` register
memory the application owns; DPDK's `rte_extmem_register` and io_uring's registered buffers do
the same for packet and I/O buffers.

## Proposed glossary entry

**Supplied memory**:
Ring memory the application gives the library through a provider, for a receive ring, an arena
or a lent ring. The library registers it, keeps its owner while the ring may be read or written,
and never writes or frees memory it does not hold. Quarantine keeps it until the process exits.
_Avoid_: external memory, user buffer, zero copy

Written against `publisher-instances` at `615f19d`.
