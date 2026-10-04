# SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-ucx project
# SPDX-License-Identifier: LGPL-3.0-or-later

import json
import importlib
import sys

import pytest

import tango_ucx


def native_core():
    return importlib.import_module("tango_ucx._core")


@pytest.fixture(autouse=True)
def release_native_core_module():
    yield
    native = sys.modules.pop("tango_ucx._core", None)
    if getattr(tango_ucx, "_core", None) is native:
        del tango_ucx._core


@pytest.fixture
def ucx_stream():
    return json.dumps(
        {
            "interface": "tango-ucx-stream/3.0",
            "instance_id": 12648430,
            "element": "u16",
            "shape": [4, 8],
            "field_bytes": 8,
            "field_dtype": "[('timestamp','<f8')]",
            "allow_every": True,
            "max_sessions": 4,
        }
    )


def test_prepare_builds_an_open_message_from_ucx_stream_and_options(ucx_stream):
    pending = native_core().prepare(
        ucx_stream,
        False,
        8 << 20,
        4,
        0.025,
        2.0,
        False,
        "preview",
    )

    message = json.loads(pending.open_message())
    assert message["interface"] == "tango-ucx-open/3.2"
    assert message["instance_id"] == 12648430
    assert message["delivery"] == "latest"
    assert message["batch"] == 4
    assert message["min_interval_ns"] == 500_000_000
    assert message["label"] == "preview"
    assert message["gpu"] is False


def test_prepare_rejects_every_when_the_publisher_does_not_allow_it(ucx_stream):
    offer = json.loads(ucx_stream)
    offer["allow_every"] = False

    with pytest.raises(ValueError, match="refuses every"):
        native_core().prepare(json.dumps(offer), True, 8 << 20, 1, 0.01, 0.0, False, "")


def test_start_calls_the_python_closer_after_an_accepted_open_cannot_reach_ready(ucx_stream):
    pending = native_core().prepare(ucx_stream, False, 8 << 20, 1, 0.01, 0.0, False, "")
    session = json.loads(pending.open_message())["session"]
    calls = []

    def closer(document, remaining_seconds):
        calls.append((json.loads(document), remaining_seconds))
        return True

    reply = '{"interface":"tango-ucx-reply/3.0","status":"ok"}'
    with pytest.raises(RuntimeError, match="endpoint"):
        pending.start(reply, closer, 0.5)

    assert len(calls) == 1
    assert calls[0][0] == {
        "interface": "tango-ucx-close/3.2",
        "session": session,
    }
    assert calls[0][1] > 0


def test_start_rejects_non_finite_timeouts_before_consuming_the_pending_subscription(ucx_stream):
    pending = native_core().prepare(ucx_stream, False, 8 << 20, 1, 0.01, 0.0, False, "")

    with pytest.raises(ValueError, match="timeout"):
        pending.start("{}", None, float("nan"))

    assert json.loads(pending.open_message())["delivery"] == "latest"
