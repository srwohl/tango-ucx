# SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
# SPDX-License-Identifier: LGPL-3.0-or-later

"""Async iteration and callback delivery for a Subscription."""

import asyncio
import threading


class AsyncSubscription:
    """Mixin for subscriptions exposing ``fd``, ``read`` and ``outcome``."""

    def __aiter__(self):
        return self

    async def __anext__(self):
        loop = asyncio.get_running_loop()
        while True:
            batch = self.read(timeout=0)
            if batch is not None:
                return batch

            outcome = self.outcome
            if outcome == "end" or outcome == "cancelled":
                raise StopAsyncIteration
            if outcome == "failed":
                from . import Failed

                health = self.health()
                raise Failed(
                    health.get("failure", "subscription failed"),
                    last_index=health.get("last_index", 0),
                )

            try:
                await self._wait_readable(loop, self.fd)
            except asyncio.CancelledError:
                self.close()
                raise

    @staticmethod
    async def _wait_readable(loop, fd):
        ready = loop.create_future()

        def wake():
            if not ready.done():
                ready.set_result(None)

        loop.add_reader(fd, wake)
        try:
            await ready
        finally:
            loop.remove_reader(fd)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        self.close()

    def callbacks(self, on_batch, on_outcome=None):
        return _Callbacks(self, on_batch, on_outcome)


class _Callbacks:
    def __init__(self, subscription, on_batch, on_outcome):
        self._subscription = subscription
        self._on_batch = on_batch
        self._on_outcome = on_outcome
        self._stop_requested = threading.Event()
        self._error = None
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self):
        try:
            while not self._stop_requested.is_set():
                batch = self._subscription.read(timeout=0.05)
                if batch is not None:
                    self._on_batch(batch)
                    continue

                if self._stop_requested.is_set():
                    return
                outcome = self._subscription.outcome
                if outcome is not None:
                    if self._on_outcome is not None:
                        self._on_outcome(outcome)
                    return
        except BaseException as error:
            self._error = error

    def wait(self, timeout=None):
        if threading.current_thread() is not self._thread:
            self._thread.join(timeout)
        if not self._thread.is_alive() and self._error is not None:
            raise self._error

    def stop(self):
        self._stop_requested.set()
        if threading.current_thread() is not self._thread:
            self._thread.join()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.stop()
