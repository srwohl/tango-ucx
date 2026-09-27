# SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
# SPDX-License-Identifier: LGPL-3.0-or-later

import gc
import os
import select
import signal
import socket
import time

import numpy as np
import pytest

import tango_bulk


def _free_loopback_port():
    while True:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        if not 45000 <= port <= 45999:
            return port


def _stop_server(pid):
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        pass

    until = time.monotonic() + 10
    while time.monotonic() < until:
        try:
            waited, _ = os.waitpid(pid, os.WNOHANG)
        except ChildProcessError:
            return
        if waited == pid:
            return
        time.sleep(0.05)

    try:
        os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    try:
        os.waitpid(pid, 0)
    except ChildProcessError:
        pass


def _start_fixture_device():
    executable = os.environ.get("TANGO_BULK_FIXTURE_DEVICE")
    if executable is None:
        pytest.skip("TANGO_BULK_FIXTURE_DEVICE is not set")

    port = _free_loopback_port()
    read_fd, write_fd = os.pipe()
    args = [
        executable,
        "test",
        "-nodb",
        "-dlist",
        "test/bulk/1",
        "-ORBendPoint",
        f"giop:tcp:127.0.0.1:{port}",
    ]
    try:
        pid = os.posix_spawn(
            executable,
            args,
            os.environ.copy(),
            file_actions=[
                (os.POSIX_SPAWN_CLOSE, read_fd),
                (os.POSIX_SPAWN_DUP2, write_fd, 3),
                (os.POSIX_SPAWN_CLOSE, write_fd),
            ],
        )
    except BaseException:
        os.close(read_fd)
        os.close(write_fd)
        raise
    os.close(write_fd)

    output = bytearray()
    deadline = time.monotonic() + 60
    try:
        while b"READY" not in output and time.monotonic() < deadline:
            readable, _, _ = select.select(
                [read_fd], [], [], min(0.25, max(0, deadline - time.monotonic()))
            )
            if not readable:
                continue
            chunk = os.read(read_fd, 256)
            if not chunk:
                break
            output.extend(chunk)

        if b"READY" not in output:
            pytest.fail(f"fixture device did not become ready: {output.decode(errors='replace')}")
    except BaseException:
        _stop_server(pid)
        raise
    finally:
        os.close(read_fd)

    return pid, f"127.0.0.1:{port}/test/bulk/1#dbase=no"


def test_nodb_subscription_exposes_readonly_views_that_outlive_close():
    pid, device_name = _start_fixture_device()
    try:
        import tango

        proxy = tango.DeviceProxy(device_name)
        subscription = tango_bulk.every(proxy, batch=4, max_wait=0.5)
        assert proxy.command_inout("Publish", 4) == 4

        expected = np.arange(4, dtype=np.uint8)
        indices = []
        payload = records = None
        while len(indices) < 4:
            batch = subscription.read(timeout=5)
            assert batch is not None, (
                f"outcome={subscription.outcome!r}, health={subscription.health()!r}"
            )
            assert batch.gpu is False
            if payload is None:
                payload = batch.array
                records = batch.records
            frame_indices = batch.records["index"].astype(np.uint8)
            indices.extend(frame_indices.tolist())
            assert np.all(batch.array == frame_indices[:, np.newaxis])
            assert np.all(batch.records["timestamp"] > 0)
            assert batch.records["fields"].tolist() == frame_indices.tolist()
            del batch

        assert indices == [0, 1, 2, 3]
        assert payload.shape[1:] == (4096,)
        assert payload.dtype == np.dtype(np.uint8)
        assert records.dtype.names == ("index", "timestamp", "fields")
        assert records["timestamp"].dtype == np.dtype(np.uint64)
        assert not payload.flags.writeable
        assert not records.flags.writeable
        with pytest.raises(ValueError):
            payload[0, 0] = 255
        with pytest.raises(ValueError):
            records[0]["index"] = 99
        with pytest.raises(ValueError):
            payload.setflags(write=True)
        with pytest.raises(ValueError):
            records.setflags(write=True)

        subscription.close()
        del subscription
        gc.collect()

        retained_indices = expected[:payload.shape[0]]
        assert np.array_equal(payload[:, 0], retained_indices)
        assert records["index"].tolist() == retained_indices.tolist()
        assert records["fields"].tolist() == retained_indices.tolist()
    finally:
        _stop_server(pid)
