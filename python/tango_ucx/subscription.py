# SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-ucx project
# SPDX-License-Identifier: LGPL-3.0-or-later

import importlib
import json
import math
import re

from . import Failed
from ._async import AsyncSubscription


_MAJOR = 3
_MARKER = re.compile(r"^tango-ucx/([0-9]+) (open|close)$")
_BUDGET = re.compile(r"^\s*([0-9]+)\s*(B|KiB|MiB|GiB)\s*$", re.IGNORECASE)
_MEMORY = re.compile(r"^(cuda|pinned):(0|[1-9][0-9]*)$")
_UNITS = {"b": 1, "kib": 1024, "mib": 1024**2, "gib": 1024**3}
_SYNC = {"wait": True}


def _tango():
    return importlib.import_module("tango")


def _budget_bytes(value):
    if isinstance(value, int) and not isinstance(value, bool):
        if value <= 0:
            raise ValueError("budget must be positive")
        return value
    if isinstance(value, str):
        match = _BUDGET.fullmatch(value)
        if match:
            result = int(match.group(1)) * _UNITS[match.group(2).lower()]
            if result > 0:
                return result
    raise ValueError("budget must be a positive byte count or a size such as '256MiB'")


def _seconds(value, name):
    if isinstance(value, bool):
        raise ValueError(f"{name} must be a non-negative number")
    try:
        seconds = float(value)
    except (OverflowError, TypeError, ValueError) as error:
        raise ValueError(f"{name} must be a non-negative number") from error
    if not math.isfinite(seconds) or seconds < 0:
        raise ValueError(f"{name} must be a non-negative number")
    return seconds


def _device_proxy(device, tango):
    if isinstance(device, str):
        return tango.DeviceProxy(device)
    return device


def _commands(proxy, tango):
    dev_string = tango.CmdArgType.DevString
    found = {}
    other_major = None
    for command in proxy.command_list_query():
        description = getattr(command, "in_type_desc", "")
        match = _MARKER.fullmatch(description) if isinstance(description, str) else None
        if not match or command.in_type != dev_string or command.out_type != dev_string:
            continue
        major, role = int(match.group(1)), match.group(2)
        if major != _MAJOR:
            other_major = major
            continue
        if description != f"tango-ucx/{_MAJOR} {role}":
            continue
        if role in found:
            raise RuntimeError("device has two sets of tango-ucx commands")
        found[role] = command.cmd_name
    if "open" not in found or "close" not in found:
        if other_major is not None:
            raise RuntimeError(
                f"device speaks tango-ucx/{other_major}; this library speaks tango-ucx/{_MAJOR}"
            )
        raise RuntimeError("device has no tango-ucx commands")
    return found["open"], found["close"]


def _command(proxy, name, document, tango, timeout=None, green_mode=None):
    kwargs = {
        **_SYNC,
        "green_mode": tango.GreenMode.Synchronous if green_mode is None else green_mode,
    }
    if timeout is not None:
        kwargs["timeout"] = timeout
    return proxy.command_inout(name, document, **kwargs)


def _subscribe(
    device,
    *,
    every,
    budget,
    batch,
    max_wait,
    max_rate,
    memory,
    allow_gpu_over_tcp,
    label,
    range=0,
):
    budget = _budget_bytes(budget)
    if isinstance(batch, bool) or not isinstance(batch, int) or batch < 1:
        raise ValueError("batch must be a positive integer")
    max_wait = _seconds(max_wait, "max_wait")
    max_rate = 0.0 if max_rate is None else _seconds(max_rate, "max_rate")
    gpu = {"range": range} if range else {}
    if memory != "host":
        match = _MEMORY.fullmatch(memory) if isinstance(memory, str) else None
        if not match:
            raise ValueError("memory must be 'host', 'cuda:N' or 'pinned:N', N being the GPU's number")
        gpu["gpu" if match.group(1) == "cuda" else "pinned"] = int(match.group(2))
    if not isinstance(label, str):
        raise ValueError("label must be a string")

    tango = _tango()
    proxy = _device_proxy(device, tango)
    open_name, close_name = _commands(proxy, tango)
    stream = proxy.read_attribute("UcxStream").value
    if not isinstance(stream, str):
        raise RuntimeError("UcxStream is not a JSON string")

    _core = importlib.import_module("tango_ucx._core")

    pending = _core.prepare(
        stream,
        every,
        budget,
        batch,
        max_wait,
        max_rate,
        allow_gpu_over_tcp,
        label,
        **gpu,
    )
    open_reply = _command(proxy, open_name, pending.open_message(), tango)

    def close_command(document, timeout):
        try:
            reply = _command(
                proxy,
                close_name,
                document,
                tango,
                timeout,
                tango.GreenMode.Futures,
            )
        except (
            TimeoutError,
            tango.DevFailed,
            tango.ConnectionFailed,
            tango.CommunicationFailed,
        ):
            return False
        try:
            result = json.loads(reply)
            return isinstance(result, dict) and result.get("status") == "ok"
        except (TypeError, ValueError):
            return False

    native = pending.start(open_reply, close_command, 5.0)
    return Subscription(native)


def every(
    device,
    *,
    budget="256MiB",
    batch=1,
    max_wait=0.01,
    memory="host",
    allow_gpu_over_tcp=False,
    label="",
):
    return _subscribe(
        device,
        every=True,
        budget=budget,
        batch=batch,
        max_wait=max_wait,
        max_rate=None,
        memory=memory,
        allow_gpu_over_tcp=allow_gpu_over_tcp,
        label=label,
    )


def latest(
    device,
    *,
    budget="256MiB",
    batch=1,
    max_wait=0.01,
    max_rate=None,
    memory="host",
    allow_gpu_over_tcp=False,
    label="",
):
    return _subscribe(
        device,
        every=False,
        budget=budget,
        batch=batch,
        max_wait=max_wait,
        max_rate=max_rate,
        memory=memory,
        allow_gpu_over_tcp=allow_gpu_over_tcp,
        label=label,
    )


def pull(
    device,
    range,
    *,
    budget="256MiB",
    batch=1,
    max_wait=0.01,
    memory="host",
    allow_gpu_over_tcp=False,
    label="",
):
    """A puller of the device's set, which takes `range` frames at each of its turns."""
    if isinstance(range, bool) or not isinstance(range, int) or range < 1:
        raise ValueError("range must be a positive integer")
    return _subscribe(
        device,
        every=False,
        budget=budget,
        batch=batch,
        max_wait=max_wait,
        max_rate=None,
        memory=memory,
        allow_gpu_over_tcp=allow_gpu_over_tcp,
        label=label,
        range=range,
    )


class Batch:
    def __init__(self, native):
        self._native = native

    @property
    def array(self):
        array = self._native.array
        array.setflags(write=False)
        return array

    def payload(self, frame):
        return self._native.payload(frame)

    @property
    def records(self):
        records = self._native.records
        records.setflags(write=False)
        return records

    @property
    def frames(self):
        return self._native.frames

    @property
    def gpu(self):
        return self._native.gpu

    def copy(self):
        return Batch(self._native.copy())

    def gpu_view(self, stream=None):
        return GpuView(self._native.gpu_view(stream))

    def release_after(self, stream=None):
        """Pinned host receive: record completion after queueing the last upload read."""
        self._native.release_after(stream)


class GpuView:
    def __init__(self, native):
        self._native = native

    def __dlpack__(self, stream=None, max_version=None, dl_device=None, copy=None):
        return self._native.dlpack(stream, max_version, dl_device, copy)

    def __dlpack_device__(self):
        return self._native.device

    def payload(self, frame):
        """One frame's bytes, with DLPack and a pointer for external CUDA adapters."""
        return _GpuPayload(self._native, frame)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        self._native.close()


class _GpuPayload:
    def __init__(self, native, frame):
        self._native = native
        self._frame = frame
        native.payload_bytes(frame)  # validate while the view is open

    @property
    def ptr(self):
        return self._native.pointer(self._frame)

    @property
    def nbytes(self):
        return self._native.payload_bytes(self._frame)

    def __dlpack__(self, stream=None, max_version=None, dl_device=None, copy=None):
        return self._native.dlpack(stream, max_version, dl_device, copy, self._frame)

    def __dlpack_device__(self):
        return self._native.device


class Subscription(AsyncSubscription):
    def __init__(self, native):
        self._native = native

    @property
    def fd(self):
        return self._native.fd

    @property
    def outcome(self):
        return self._native.outcome

    @property
    def description(self):
        return self._native.description

    def health(self):
        return self._native.health()

    def read(self, timeout=None):
        batch = self._native.read(timeout=timeout)
        return None if batch is None else Batch(batch)

    def close(self, timeout=5.0):
        self._native.close(timeout)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.close()

    def __iter__(self):
        return self

    def __next__(self):
        batch = self.read()
        if batch is not None:
            return batch
        if self.outcome == "failed":
            health = self.health()
            raise Failed(
                health.get("failure", "subscription failed"),
                last_index=health.get("last_index", 0),
            )
        raise StopIteration
