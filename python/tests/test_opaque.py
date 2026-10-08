# SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-ucx project
# SPDX-License-Identifier: LGPL-3.0-or-later

import gc
import struct
import weakref

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


def test_cached_records_keep_their_storage_and_outlive_the_batch_and_subscription(server):
    proxy, dtype = _opaque(server[1], [("tag", "<u8"), ("shape", "<u4", (2,))])
    with tango_ucx.every(proxy, budget="16MiB", batch=2, max_wait=1.0) as subscription:
        for tag in (100, 101):
            _publish(proxy, np.array((tag, (2, 3)), dtype=dtype), b"x")
        batch = subscription.read(timeout=5)
        assert batch.frames == 2
        records = batch.records
        assert batch.records is not records
        assert np.shares_memory(batch.records, records)
        assert np.shares_memory(records, batch._native.records)
        assert not records.flags.writeable
        with pytest.raises(ValueError):
            records.setflags(write=True)
        copied = batch.copy()
        copied_records = copied.records
        assert np.shares_memory(copied.records, copied_records)
        assert np.array_equal(copied_records, records)
        assert not np.shares_memory(copied_records, records)
        reference = weakref.ref(batch)
        del batch, copied
        gc.collect()
        assert reference() is None
        assert subscription.health()["slots_held"] == 2

    del subscription
    gc.collect()
    assert records["tag"].tolist() == [100, 101]
    assert records["shape"].tolist() == [[2, 3], [2, 3]]
    assert np.array_equal(copied_records, records)
    assert not copied_records.flags.writeable


def test_cached_records_return_independent_array_descriptors(server):
    proxy, dtype = _opaque(
        server[1], [("tag", "<u8"), ("nested", [("value", "<u8")]),
                    ("array", [("entry", ">u4")], (2,))]
    )
    with tango_ucx.every(proxy, budget="16MiB", batch=2, max_wait=1.0) as subscription:
        for tag in (100, 101):
            _publish(proxy, np.array((tag, (200,), [(3,), (4,)]), dtype=dtype), b"x")
        batch = subscription.read(timeout=5)
        original = batch.records
        changed = batch.records
        changed.shape = (1, 2)
        changed.dtype = np.dtype("u1")
        subsequent = batch.records
        assert subsequent.shape == original.shape == (2,)
        assert subsequent.dtype == original.dtype
        assert subsequent["tag"].tolist() == [100, 101]
        assert np.shares_memory(subsequent, changed)
        assert not subsequent.flags.writeable
        renamed = batch.records
        renamed.dtype.fields["nested"][0].names = ("changed_value",)
        renamed.dtype.fields["array"][0].subdtype[0].names = ("changed_entry",)
        renamed.dtype.names = ("changed_index", "changed_timestamp", "changed_tag",
                               "changed_nested", "changed_array")
        subsequent = batch.records
        assert subsequent.dtype == original.dtype
        assert subsequent.dtype.fields["nested"][0].names == ("value",)
        assert subsequent.dtype.fields["array"][0].subdtype[0].names == ("entry",)
        assert subsequent.dtype.fields["array"][0].subdtype[0].fields["entry"][0].byteorder == ">"
        assert subsequent["nested"]["value"].tolist() == [200, 200]
        assert subsequent["array"]["entry"].tolist() == [[3, 4], [3, 4]]


def test_cached_records_still_validate_alignment_on_first_access(server):
    proxy, dtype = _opaque(server[1], [("tag", "u1"), ("value", "<u8")])
    with tango_ucx.every(proxy, budget="16MiB", max_wait=0.01) as subscription:
        _publish(proxy, np.array((1, 2), dtype=dtype), b"x")
        batch = subscription.read(timeout=5)
        for attempt in range(2):
            with pytest.raises(ValueError, match="alignment is incompatible"):
                batch.records


def test_gpu_and_pinned_host_receive_are_exclusive_for_an_opaque_stream(server):
    from tango_ucx import _core

    proxy, _ = _opaque(server[1], [("tag", "<u8")])
    with pytest.raises(ValueError, match="choose GPU receive or pinned host receive"):
        _core.prepare(
            proxy.read_attribute("UcxStream").value,
            True, 16 << 20, 1, 0.01, 0.0, False, "", gpu=0, pinned=0,
        )


# What DECTRIS detectors send (SIMPLON stream, and HDF5's bitshuffle filter): "bs32-lz4<" is
# little-endian 32-bit elements, bitshuffled and LZ4-compressed in blocks, after a 12-byte
# big-endian header of the decoded size and the block size, both in bytes. The frame here is
# generated, not captured from a detector.
def _bslz4(image, block):
    bitshuffle = pytest.importorskip("bitshuffle")
    header = struct.pack(">QI", image.nbytes, block * image.itemsize)
    return header + bitshuffle.compress_lz4(image, block).tobytes()


def _decode(frame, record):
    """Decodes a frame from its bytes and its record alone, as an archive's reader would."""
    bitshuffle = pytest.importorskip("bitshuffle")
    encoding = record["encoding"].decode()
    assert encoding in ("bs32-lz4<", "bs16-lz4<")
    dtype = np.dtype(record["type"].decode()).newbyteorder("<")
    assert dtype.itemsize * 8 == int(encoding[2:4])
    size, block = struct.unpack(">QI", bytes(frame[:12]))
    shape = tuple(int(n) for n in record["shape"])
    assert size == dtype.itemsize * shape[0] * shape[1]
    return bitshuffle.decompress_lz4(frame[12:], shape, dtype, block // dtype.itemsize)


def test_bitshuffle_lz4_frames_decode_from_their_record_fields(server):
    proxy, dtype = _opaque(
        server[1], [("encoding", "S16"), ("type", "S8"), ("shape", "<u4", (2,))]
    )
    random = np.random.default_rng(11)
    images = [  # sparse counts, as a photon-counting detector's: each compresses differently
        random.poisson(rate, (514, 1030)).astype(element)
        for rate, element in ((0.01, "<u4"), (0.5, "<u4"), (3.0, "<u2"))
    ]
    sent = []
    with tango_ucx.every(proxy, budget="16MiB", batch=3, max_wait=1.0) as subscription:
        for image in images:
            bits = image.itemsize * 8
            fields = np.array((f"bs{bits}-lz4<", f"uint{bits}", image.shape), dtype=dtype)
            sent.append(_bslz4(image, 8192 // image.itemsize))
            _publish(proxy, fields, sent[-1])
        batch = subscription.read(timeout=5)
        assert batch.frames == 3
        lengths = [len(batch.payload(i)) for i in range(3)]
        assert lengths == [len(frame) for frame in sent]
        assert len(set(lengths)) == 3 and max(lengths) < images[0].nbytes // 2
        for i, image in enumerate(images):
            assert bytes(batch.payload(i)) == sent[i]  # byte for byte
            decoded = _decode(batch.payload(i), batch.records[i])
            assert decoded.dtype == image.dtype and np.array_equal(decoded, image)
