# SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
# SPDX-License-Identifier: LGPL-3.0-or-later

import asyncio
import os
import queue
import threading

import pytest

from tango_bulk._async import AsyncSubscription


class FakeBatch:
    pass


class FakeSubscription(AsyncSubscription):
    def __init__(self):
        self._read_fd, self._write_fd = os.pipe2(os.O_NONBLOCK | os.O_CLOEXEC)
        self._ready = queue.Queue()
        self._outcome = None
        self.close_calls = 0

    @property
    def fd(self):
        return self._read_fd

    @property
    def outcome(self):
        return self._outcome

    def read(self, timeout=None):
        try:
            item = self._ready.get(timeout=timeout)
        except queue.Empty:
            return None
        try:
            os.read(self._read_fd, 1)
        except BlockingIOError:
            pass
        if item[0] == "batch":
            return item[1]
        self._outcome = item[1]
        return None

    def push_batch(self, batch):
        self._ready.put(("batch", batch))
        os.write(self._write_fd, b"b")

    def push_outcome(self, outcome):
        self._ready.put(("outcome", outcome))
        os.write(self._write_fd, b"o")

    def close(self, timeout=5.0):
        self.close_calls += 1
        if self._outcome is None:
            self.push_outcome("cancelled")

    def health(self):
        return {"failure": "publisher stopped", "last_index": 4}

    def dispose(self):
        os.close(self._read_fd)
        os.close(self._write_fd)


def test_async_iteration_reads_a_batch_when_fd_becomes_readable():
    sub = FakeSubscription()
    batch = FakeBatch()

    async def receive():
        waiting = asyncio.create_task(sub.__anext__())
        await asyncio.sleep(0)
        sub.push_batch(batch)
        return await waiting

    try:
        assert asyncio.run(receive()) is batch
    finally:
        sub.dispose()


def test_async_iteration_stops_after_end_and_keeps_batch_order():
    sub = FakeSubscription()
    batches = [FakeBatch(), FakeBatch()]
    sub.push_batch(batches[0])
    sub.push_batch(batches[1])
    sub.push_outcome("end")

    async def receive_all():
        return [batch async for batch in sub]

    try:
        assert asyncio.run(receive_all()) == batches
    finally:
        sub.dispose()


def test_async_iteration_stops_after_cancelled_outcome():
    sub = FakeSubscription()
    sub.push_outcome("cancelled")

    async def receive():
        return await sub.__anext__()

    try:
        with pytest.raises(StopAsyncIteration):
            asyncio.run(receive())
    finally:
        sub.dispose()


def test_async_iteration_raises_failed_with_failure_and_last_index():
    from tango_bulk import Failed

    sub = FakeSubscription()
    sub.push_outcome("failed")

    async def receive():
        return await sub.__anext__()

    try:
        with pytest.raises(Failed, match="publisher stopped") as error:
            asyncio.run(receive())
        assert error.value.last_index == 4
    finally:
        sub.dispose()


def test_cancelling_async_wait_closes_subscription():
    sub = FakeSubscription()

    async def cancel_wait():
        task = asyncio.create_task(sub.__anext__())
        await asyncio.sleep(0)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            return
        raise AssertionError("the cancelled iteration should propagate cancellation")

    try:
        asyncio.run(cancel_wait())
        assert sub.close_calls == 1
    finally:
        sub.dispose()


def test_async_context_manager_closes_subscription():
    sub = FakeSubscription()

    async def use_context():
        async with sub:
            pass

    try:
        asyncio.run(use_context())
        assert sub.close_calls == 1
    finally:
        sub.dispose()


def test_callbacks_deliver_batches_then_outcome_and_wait_for_completion():
    sub = FakeSubscription()
    batch = FakeBatch()
    events = []
    listener = sub.callbacks(
        lambda value: events.append(("batch", value)),
        lambda value: events.append(("outcome", value)),
    )
    sub.push_batch(batch)
    sub.push_outcome("end")

    try:
        listener.wait(timeout=2)
        assert events == [("batch", batch), ("outcome", "end")]
    finally:
        listener.stop()
        sub.dispose()


def test_callback_first_exception_stops_delivery_and_is_raised_by_wait():
    sub = FakeSubscription()
    batches = [FakeBatch(), FakeBatch()]
    outcomes = []

    class CallbackError(Exception):
        pass

    def on_batch(batch):
        raise CallbackError("first callback failed")

    listener = sub.callbacks(on_batch, outcomes.append)
    sub.push_batch(batches[0])
    sub.push_batch(batches[1])
    sub.push_outcome("end")
    try:
        with pytest.raises(CallbackError, match="first callback failed"):
            listener.wait(timeout=2)
        assert outcomes == []
    finally:
        listener.stop()
        sub.dispose()


def test_callback_context_manager_stops_delivery_when_it_exits():
    sub = FakeSubscription()
    called = threading.Event()

    with sub.callbacks(lambda batch: called.set()):
        pass

    sub.push_batch(FakeBatch())
    try:
        assert not called.wait(timeout=0.1)
    finally:
        sub.dispose()


def test_stopping_callbacks_waits_for_running_callback_and_skips_queued_batches():
    sub = FakeSubscription()
    entered = threading.Event()
    release = threading.Event()
    stop_returned = threading.Event()
    first, second = FakeBatch(), FakeBatch()
    seen = []

    def on_batch(batch):
        seen.append(batch)
        entered.set()
        release.wait(timeout=2)

    listener = sub.callbacks(on_batch)
    sub.push_batch(first)
    assert entered.wait(timeout=1)
    sub.push_batch(second)

    stopper = threading.Thread(
        target=lambda: (listener.stop(), stop_returned.set()), daemon=True
    )
    stopper.start()
    try:
        assert not stop_returned.wait(timeout=0.05)
        release.set()
        assert stop_returned.wait(timeout=1)
        assert seen == [first]
        assert sub.read(timeout=0) is second  # still there for the application
    finally:
        release.set()
        listener.stop()
        stopper.join(timeout=1)
        sub.dispose()


def test_stopping_callbacks_inside_callback_does_not_self_join():
    sub = FakeSubscription()
    returned = threading.Event()
    batches = []
    helper = None

    def on_batch(batch):
        batches.append(batch)
        helper.stop()
        returned.set()

    helper = sub.callbacks(on_batch)
    sub.push_batch(FakeBatch())
    try:
        assert returned.wait(timeout=1)
        helper.wait(timeout=1)
        assert len(batches) == 1
    finally:
        helper.stop()
        sub.dispose()
