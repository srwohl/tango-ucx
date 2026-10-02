# SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-ucx project
# SPDX-License-Identifier: LGPL-3.0-or-later

import gc
import time

import pytest

import tango_ucx
from test_nodb import server  # noqa: F401

cupy = pytest.importorskip("cupy")
pytestmark = pytest.mark.gpu


@pytest.fixture
def subscription(server):
    """An every subscription into GPU 0 with eight frames published, four to a batch.

    A batch holds its four slots from landing, read or not, until it is released.
    """
    import tango

    with tango_ucx.every(server[1], batch=4, max_wait=0.5, memory="cuda:0") as subscription:
        assert tango.DeviceProxy(server[1]).command_inout("Publish", 8) == 8
        yield subscription


def _held(subscription, slots):
    until = time.monotonic() + 5
    while subscription.health()["slots_held"] != slots and time.monotonic() < until:
        time.sleep(0.01)
    return subscription.health()["slots_held"] == slots


def _first_bytes(tensor):
    return cupy.asnumpy(tensor[:, 0]).tolist()


def test_a_cupy_tensor_kept_past_its_gpu_view_holds_the_batch_until_it_is_deleted(subscription):
    batch = subscription.read(timeout=5)
    assert batch.gpu is True and batch.frames == 4
    with pytest.raises(ValueError, match="GPU batch"):
        batch.array
    records = batch.records  # host memory
    assert records["index"].tolist() == records["fields"].tolist() == [0, 1, 2, 3]

    with batch.gpu_view() as view:
        assert view.__dlpack_device__() == (2, 0)  # kDLCUDA, GPU 0
        tensor = cupy.from_dlpack(view)
        with pytest.raises(RuntimeError, match="one GPU view"):
            batch.gpu_view()
    assert tensor.shape == (4, 4096) and tensor.dtype == cupy.uint8
    assert tensor.device.id == 0 and _first_bytes(tensor) == [0, 1, 2, 3]

    second = subscription.read(timeout=5)
    del batch, view, records, second
    gc.collect()
    time.sleep(0.1)
    # Credit is cumulative: the second batch waits behind the tensor's
    assert subscription.health()["slots_held"] == 8
    assert int(tensor.sum()) == 4096 * (1 + 2 + 3)  # still the first batch's frames
    del tensor
    assert _held(subscription, 0)


def test_a_gpu_batch_is_released_when_its_view_and_its_records_are_gone(subscription):
    batch = subscription.read(timeout=5)
    with batch.gpu_view() as view:
        assert _first_bytes(cupy.from_dlpack(view)) == [0, 1, 2, 3]
    records = batch.records
    del batch, view
    assert not _held(subscription, 4)  # the record array is a view of the batch
    assert records["index"].tolist() == [0, 1, 2, 3]
    del records
    assert _held(subscription, 4)  # the unread batch
    assert subscription.read(timeout=5).copy().array[:, 0].tolist() == [4, 5, 6, 7]


def test_an_abandoned_dlpack_capsule_releases_the_batch(subscription):
    batch = subscription.read(timeout=5)
    with batch.gpu_view() as view:
        capsule = view.__dlpack__(max_version=(1, 0))
        consumed = cupy.from_dlpack(view)
    del batch, view, consumed
    gc.collect()
    time.sleep(0.1)
    assert subscription.health()["slots_held"] == 8  # the capsule alone holds the batch
    del capsule
    assert _held(subscription, 4)  # the unread batch


def test_every_export_of_a_gpu_view_uses_the_views_gpu_stream(subscription):
    stream = cupy.cuda.Stream()
    with subscription.read(timeout=5).gpu_view(stream=stream.ptr) as view:
        with pytest.raises(BufferError, match="the view's GPU stream"):
            cupy.from_dlpack(view)  # CuPy asks for its current stream, the default one
        with stream:
            tensor = cupy.from_dlpack(view)
            assert int(tensor.sum()) == 4096 * (1 + 2 + 3)
    with subscription.read(timeout=5).gpu_view() as view, stream:
        later = cupy.from_dlpack(view)  # the first export fixes the stream
        with pytest.raises(BufferError, match="the view's GPU stream"):
            view.__dlpack__(stream=1, max_version=(1, 0))
        assert _first_bytes(later) == [4, 5, 6, 7]
    del tensor, later, view
    assert _held(subscription, 0)


def test_a_gpu_view_refuses_what_it_cannot_export(subscription):
    batch = subscription.read(timeout=5)
    with pytest.raises(RuntimeError, match="needs a GPU batch"):
        batch.copy().gpu_view()
    with batch.gpu_view() as view:
        with pytest.raises(BufferError, match="max_version"):
            view.__dlpack__()  # the unversioned capsule
        with pytest.raises(BufferError, match="does not copy"):
            view.__dlpack__(max_version=(1, 0), copy=True)
        with pytest.raises(BufferError, match="GPU of its batch"):
            view.__dlpack__(max_version=(1, 0), dl_device=(1, 0))  # kDLCPU
        with pytest.raises(BufferError, match="stream must be"):
            view.__dlpack__(stream=0, max_version=(1, 0))
    with pytest.raises(BufferError, match="has ended"):
        cupy.from_dlpack(view)
    del batch, view
    assert _held(subscription, 4)  # a view that exported nothing records no event
