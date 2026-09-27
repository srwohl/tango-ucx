# SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
# SPDX-License-Identifier: LGPL-3.0-or-later

import json
import sys
import types

import numpy as np
import pytest

import tango_bulk
import tango_bulk.subscription as subscription_module


STREAM = json.dumps({"interface": "tango-bulk-stream/3.0", "incarnation": 12})
OPEN_REPLY = json.dumps({"interface": "tango-bulk-reply/3.0", "status": "ok"})
CLOSE_REPLY = json.dumps({"interface": "tango-bulk-reply/3.0", "status": "ok"})


class Command:
    def __init__(self, name, marker=""):
        self.cmd_name = name
        self.in_type = "DevString"
        self.out_type = "DevString"
        self.in_type_desc = marker


class TangoFailure(Exception):
    pass


class Proxy:
    def __init__(self):
        self.calls = []
        self.close_error = None

    def command_list_query(self, **kwargs):
        self.calls.append(("command_list_query", kwargs))
        return [
            Command("Unrelated"),
            Command("OpenTransfer", "tango-bulk/3 open"),
            Command("CloseTransfer", "tango-bulk/3 close"),
        ]

    def read_attribute(self, name, **kwargs):
        self.calls.append(("read_attribute", name, kwargs))
        return types.SimpleNamespace(value=STREAM)

    def command_inout(self, name, document, **kwargs):
        self.calls.append(("command_inout", name, document, kwargs))
        if name == "CloseTransfer" and self.close_error is not None:
            raise self.close_error
        return OPEN_REPLY if name == "OpenTransfer" else CLOSE_REPLY


class NativeSubscription:
    fd = 9
    description = {"element": "u16", "shape": (2, 3), "field_bytes": 0, "field_dtype": ""}

    def __init__(self):
        self.closed_with = None
        self.closer = None
        self.close_result = None
        self.outcome = None
        self.deliveries = []
        self.read_timeouts = []
        self.health_sample = {"last_index": 0, "failure": ""}

    def read(self, timeout=None):
        self.read_timeouts.append(timeout)
        if self.deliveries:
            return self.deliveries.pop(0)
        return None

    def health(self):
        return self.health_sample

    def close(self, timeout):
        self.closed_with = timeout
        if self.closer is not None:
            self.close_result = self.closer("close document", timeout)


class NativeBatch:
    def __init__(self, array, records):
        self.array = array
        self.records = records
        self.frames = len(array)
        self.gpu = False

    def copy(self):
        return NativeBatch(self.array.copy(), self.records.copy())


class Pending:
    def __init__(self, proxy):
        self.proxy = proxy
        self.started = None
        self.sub = NativeSubscription()

    def open_message(self):
        return "open document"

    def start(self, reply, closer, timeout):
        self.started = (reply, closer, timeout)
        self.sub.closer = closer
        return self.sub


class Core:
    def __init__(self, proxy):
        self.proxy = proxy
        self.calls = []
        self.pending = Pending(proxy)

    def prepare(self, *args):
        self.calls.append(args)
        return self.pending


def install_tango(monkeypatch, proxy=None):
    proxy = proxy or Proxy()
    synchronous = object()
    futures = object()
    created = []

    def device_proxy(name):
        created.append(name)
        return proxy

    module = types.SimpleNamespace(
        DeviceProxy=device_proxy,
        GreenMode=types.SimpleNamespace(Synchronous=synchronous, Futures=futures),
        CmdArgType=types.SimpleNamespace(DevString="DevString"),
        DevFailed=TangoFailure,
        ConnectionFailed=TangoFailure,
        CommunicationFailed=TangoFailure,
    )
    monkeypatch.setitem(sys.modules, "tango", module)
    return proxy, synchronous, futures, created


def install_core(monkeypatch, proxy):
    core = Core(proxy)
    monkeypatch.setitem(sys.modules, "tango_bulk._core", core)
    return core


@pytest.mark.parametrize("budget", [0, "0B", True, "12MB"])
def test_budget_must_be_positive_and_use_supported_units(budget):
    with pytest.raises(ValueError, match="budget"):
        tango_bulk.every("sys/test/1", budget=budget)


@pytest.mark.parametrize("option", ["max_wait", "max_rate"])
def test_time_options_reject_booleans(option):
    with pytest.raises(ValueError, match=f"{option} must be a non-negative number"):
        tango_bulk.latest("sys/test/1", **{option: True})


def test_latest_discovers_marked_commands_and_keeps_the_passed_proxy(monkeypatch):
    proxy, synchronous, futures, _ = install_tango(monkeypatch)
    core = install_core(monkeypatch, proxy)

    sub = tango_bulk.latest(
        proxy,
        budget="2MiB",
        batch=3,
        max_wait=0.025,
        max_rate=4.5,
        label="viewer",
    )
    sub.close(timeout=3.0)

    assert core.calls == [(STREAM, False, 2 * 1024**2, 3, 0.025, 4.5, False, "viewer")]
    assert core.pending.started[0] == OPEN_REPLY
    assert core.pending.started[2] == 5.0
    assert core.pending.sub.close_result is True
    assert [call[0] for call in proxy.calls] == [
        "command_list_query",
        "read_attribute",
        "command_inout",
        "command_inout",
    ]
    assert proxy.calls[0][1] == {}
    assert proxy.calls[1][2] == {}
    assert all(call[-1]["wait"] is True for call in proxy.calls[2:])
    assert proxy.calls[2][-1]["green_mode"] is synchronous
    assert proxy.calls[-1][-1]["green_mode"] is futures
    assert proxy.calls[-1][2] == "close document"
    assert proxy.calls[-1][3]["timeout"] == 3.0
    assert sub.fd == 9


def test_device_name_constructs_one_proxy_and_every_uses_default_latest_rate(monkeypatch):
    proxy, _, _, created = install_tango(monkeypatch)
    core = install_core(monkeypatch, proxy)

    tango_bulk.every("sys/test/1")

    assert created == ["sys/test/1"]
    assert core.calls == [(STREAM, True, 256 * 1024**2, 1, 0.01, 0.0, False, "")]
    assert all(call[-1]["wait"] is True for call in proxy.calls[2:])


def test_subscription_is_a_context_manager_and_forwards_read_timeout(monkeypatch):
    proxy, _, _, _ = install_tango(monkeypatch)
    core = install_core(monkeypatch, proxy)
    sub = tango_bulk.every(proxy)

    assert sub.read(timeout=0.125) is None
    with sub:
        pass
    assert core.pending.sub.closed_with == 5.0
    assert core.pending.sub.read_timeouts == [0.125]


def test_batches_expose_read_only_views_and_copy_to_independent_arrays(monkeypatch):
    proxy, _, _, _ = install_tango(monkeypatch)
    core = install_core(monkeypatch, proxy)
    payload = np.arange(6, dtype=np.uint16).reshape(1, 2, 3)
    records = np.array([(7, 9001)], dtype=[("index", "<u8"), ("timestamp", "<u8")])
    core.pending.sub.deliveries.append(NativeBatch(payload, records))

    batch = tango_bulk.every(proxy).read()
    assert batch.frames == 1
    assert batch.gpu is False
    assert batch.array.shape == (1, 2, 3)
    assert batch.array.tolist() == [[[0, 1, 2], [3, 4, 5]]]
    assert batch.array.flags.writeable is False
    assert batch.records[0]["index"] == 7
    assert batch.records.flags.writeable is False
    with pytest.raises(ValueError):
        batch.array[0, 0, 0] = 4

    copied = batch.copy()
    assert np.array_equal(copied.array, batch.array)
    assert np.shares_memory(copied.array, batch.array) is False


def test_iteration_raises_failed_after_delivered_batches(monkeypatch):
    proxy, _, _, _ = install_tango(monkeypatch)
    core = install_core(monkeypatch, proxy)
    payload = np.array([[11]], dtype=np.uint8)
    records = np.array([(22, 33)], dtype=[("index", "<u8"), ("timestamp", "<u8")])
    core.pending.sub.deliveries.append(NativeBatch(payload, records))
    core.pending.sub.outcome = "failed"
    core.pending.sub.health_sample = {"last_index": 22, "failure": "publisher died"}

    iterator = iter(tango_bulk.every(proxy))
    assert next(iterator).array.item() == 11
    with pytest.raises(tango_bulk.Failed, match="publisher died") as failure:
        next(iterator)
    assert failure.value.last_index == 22


def test_iteration_stops_at_end(monkeypatch):
    proxy, _, _, _ = install_tango(monkeypatch)
    core = install_core(monkeypatch, proxy)
    core.pending.sub.outcome = "end"

    with pytest.raises(StopIteration):
        next(iter(tango_bulk.every(proxy)))


def test_close_timeout_leaves_the_native_fence_unproved(monkeypatch):
    proxy, _, _, _ = install_tango(monkeypatch)
    core = install_core(monkeypatch, proxy)
    sub = tango_bulk.every(proxy)
    proxy.close_error = TimeoutError("close timed out")

    sub.close(timeout=0.001)

    assert core.pending.sub.close_result is False
    assert proxy.calls[-1][-1]["timeout"] == 0.001
