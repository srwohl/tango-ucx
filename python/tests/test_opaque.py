# SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-ucx project
# SPDX-License-Identifier: LGPL-3.0-or-later

import numpy as np
import pytest

import tango_ucx
from test_nodb import server  # noqa: F401

MAX = 1 << 20


def _opaque(device_name, fields):
    """The fixture device, publishing opaque frames of up to MAX bytes with these fields."""
    import tango

    proxy = tango.DeviceProxy(device_name)
    dtype = np.dtype(fields)
    proxy.command_inout("Opaque", ([MAX, dtype.itemsize], [str(dtype.descr)]))
    return proxy, dtype


def _publish(proxy, fields, payload):
    proxy.command_inout("PublishBytes", fields.tobytes() + bytes(payload))


def test_opaque_frames_of_different_lengths_keep_their_bytes_and_records(server):
    proxy, dtype = _opaque(server[1], [("tag", "<u8")])
    random = np.random.default_rng(7)
    frames = [random.integers(0, 256, n, dtype=np.uint8) for n in (1, 1000, 300_000, MAX)]
    with tango_ucx.every(proxy, budget="16MiB", batch=4, max_wait=1.0) as subscription:
        assert subscription.description["max_payload_bytes"] == MAX
        assert subscription.description["shape"] == ()
        for tag, frame in enumerate(frames):
            _publish(proxy, np.array((100 + tag,), dtype=dtype), frame)
        batch = subscription.read(timeout=5)
        assert batch.frames == 4
        assert batch.records["index"].tolist() == [0, 1, 2, 3]
        assert batch.records["tag"].tolist() == [100, 101, 102, 103]
        for i, frame in enumerate(frames):
            assert batch.payload(i).dtype == np.uint8
            assert np.array_equal(batch.payload(i), frame)
        assert not batch.payload(0).flags.writeable
        with pytest.raises(ValueError, match="no array"):
            batch.array
        with pytest.raises(IndexError):
            batch.payload(4)
        copied = batch.copy()
        assert np.array_equal(copied.payload(2), frames[2])
        assert not np.shares_memory(copied.payload(2), batch.payload(2))
        last = batch.payload(3)

    assert np.array_equal(last, frames[3])  # a frame is a view: it outlives the close


def test_a_frame_over_the_maximum_is_refused_by_the_device_and_uses_no_frame_index(server):
    import tango

    proxy, dtype = _opaque(server[1], [("tag", "<u8")])
    fields = np.array((1,), dtype=dtype)
    with tango_ucx.every(proxy, budget="16MiB", max_wait=0.01) as subscription:
        for refused in (MAX + 1, 0):
            with pytest.raises(tango.DevFailed, match="an opaque frame has 1 to 1048576"):
                _publish(proxy, fields, bytes(refused))
        _publish(proxy, fields, b"x")
        batch = subscription.read(timeout=5)
        assert batch.records["index"].tolist() == [0]
        assert bytes(batch.payload(0)) == b"x"


def test_a_gpu_subscription_to_an_opaque_stream_is_refused(server):
    proxy, _ = _opaque(server[1], [("tag", "<u8")])
    with pytest.raises(ValueError, match="GPU"):  # no runtime here, or no opaque GPU batches
        tango_ucx.every(proxy, budget="16MiB", memory="cuda:0")
