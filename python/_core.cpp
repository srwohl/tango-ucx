// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-ucx project
// SPDX-License-Identifier: LGPL-3.0-or-later

#include "transport.h"

#include <tango-ucx/ucx.h>

#include <dlpack/dlpack.h>
#include <pybind11/numpy.h>
#include <pybind11/pybind11.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace py = pybind11;
using namespace TangoUcx;

namespace
{
using Clock = std::chrono::steady_clock;

std::string require_string(py::handle value, const char *name)
{
    if(!py::isinstance<py::str>(value))
        throw std::invalid_argument(std::string(name) + " must be a string");
    return py::cast<std::string>(value);
}

std::uint64_t require_unsigned(py::handle value, const char *name)
{
    if(!PyLong_Check(value.ptr()) || PyBool_Check(value.ptr()))
        throw std::invalid_argument(std::string(name) + " must be a non-negative integer");
    auto result = PyLong_AsUnsignedLongLong(value.ptr());
    if(PyErr_Occurred())
    {
        PyErr_Clear();
        throw std::invalid_argument(std::string(name) + " must be a non-negative integer");
    }
    return static_cast<std::uint64_t>(result);
}

bool require_bool(py::handle value, const char *name)
{
    if(!PyBool_Check(value.ptr()))
        throw std::invalid_argument(std::string(name) + " must be a bool");
    return value.ptr() == Py_True;
}

double require_seconds(py::handle value, const char *name)
{
    if(PyBool_Check(value.ptr()) || !PyNumber_Check(value.ptr()))
        throw std::invalid_argument(std::string(name) + " must be a non-negative number");
    const double seconds = PyFloat_AsDouble(value.ptr());
    if(PyErr_Occurred())
    {
        PyErr_Clear();
        throw std::invalid_argument(std::string(name) + " must be a non-negative number");
    }
    if(!std::isfinite(seconds) || seconds < 0)
        throw std::invalid_argument(std::string(name) + " must be a finite non-negative number");
    return seconds;
}

Clock::time_point deadline_after(double seconds)
{
    if(!std::isfinite(seconds) || seconds < 0)
        throw std::invalid_argument("timeout must be a finite non-negative number");
    const auto now = Clock::now();
    const auto room = Clock::time_point::max() - now;
    const auto requested = std::chrono::duration<double>(seconds);
    if(requested >= room)
        return Clock::time_point::max();
    return now + std::chrono::duration_cast<Clock::duration>(requested);
}

Clock::time_point read_deadline(py::handle timeout)
{
    if(timeout.is_none())
        return Clock::time_point::max();
    return deadline_after(require_seconds(timeout, "timeout"));
}

std::string element_name(Element element)
{
    switch(element)
    {
        case Element::U8:
            return "u8";
        case Element::U16:
            return "u16";
        case Element::U32:
            return "u32";
        case Element::U64:
            return "u64";
        case Element::I8:
            return "i8";
        case Element::I16:
            return "i16";
        case Element::I32:
            return "i32";
        case Element::I64:
            return "i64";
        case Element::F32:
            return "f32";
        case Element::F64:
            return "f64";
        case Element::Bytes:
            return "bytes";
    }
    throw std::invalid_argument("unknown element type");
}

py::dict description_dict(const Description &description)
{
    py::dict result;
    result["element"] = element_name(description.element);
    py::tuple shape(description.shape.size());
    for(std::size_t i = 0; i < description.shape.size(); ++i)
        shape[i] = py::int_(description.shape[i]);
    result["shape"] = std::move(shape);
    result["field_bytes"] = description.field_bytes;
    result["field_dtype"] = description.field_dtype;
    result["max_payload_bytes"] = description.max_payload_bytes; // nonzero: an opaque stream
    return result;
}

py::dict health_dict(const Health &health)
{
    py::dict result;
    result["frames"] = health.frames;
    result["skipped"] = health.skipped;
    result["every"] = health.every;
    result["latest"] = health.latest;
    result["failed_every"] = health.failed_every;
    result["pressure"] = health.pressure;
    result["pressure_from"] = health.pressure_from;
    result["slots_free"] = health.slots_free;
    result["slots_held"] = health.slots_held;
    result["last_index"] = health.last_index;
    result["transfer_max_ns"] = health.transfer_max_ns;
    result["transfer_mean_ns"] = health.transfer_mean_ns;
    result["quarantined_bytes"] = health.quarantined_bytes;
    result["memory_hooks"] = health.memory_hooks;
    result["started"] = health.started;
    result["transport"] = health.transport;
    result["failure"] = health.failure;
    return result;
}

py::object outcome_name(std::optional<Outcome> outcome)
{
    if(!outcome)
        return py::none();
    switch(*outcome)
    {
        case Outcome::End:
            return py::str("end");
        case Outcome::Failed:
            return py::str("failed");
        case Outcome::Cancelled:
            return py::str("cancelled");
    }
    return py::none();
}

py::dtype payload_dtype(Element element)
{
    switch(element)
    {
        case Element::U8:
        case Element::Bytes:
            return py::dtype::of<std::uint8_t>();
        case Element::U16:
            return py::dtype::of<std::uint16_t>();
        case Element::U32:
            return py::dtype::of<std::uint32_t>();
        case Element::U64:
            return py::dtype::of<std::uint64_t>();
        case Element::I8:
            return py::dtype::of<std::int8_t>();
        case Element::I16:
            return py::dtype::of<std::int16_t>();
        case Element::I32:
            return py::dtype::of<std::int32_t>();
        case Element::I64:
            return py::dtype::of<std::int64_t>();
        case Element::F32:
            return py::dtype::of<float>();
        case Element::F64:
            return py::dtype::of<double>();
    }
    throw std::invalid_argument("unknown element type");
}

py::object numpy_field_dtype(const py::module_ &numpy, const std::string &spelling)
{
    try
    {
        auto ast = py::module_::import("ast");
        return numpy.attr("dtype")(ast.attr("literal_eval")(spelling));
    }
    catch(const py::error_already_set &)
    {
        PyErr_Clear();
    }
    return numpy.attr("dtype")(spelling);
}

void validate_dtype_alignment(py::handle dtype, std::uintptr_t record_address,
                              std::uint64_t offset, std::uint64_t record_stride,
                              std::vector<std::uint64_t> &array_strides)
{
    const auto alignment = dtype.attr("alignment").cast<std::uint64_t>();
    if(alignment && ((record_address + offset) % alignment || record_stride % alignment ||
                     std::any_of(array_strides.begin(), array_strides.end(),
                                 [alignment](auto stride) { return stride % alignment != 0; })))
        throw std::invalid_argument(
            "field_dtype alignment is incompatible with the 8-byte padded record layout");

    py::object subdtype = dtype.attr("subdtype");
    if(!subdtype.is_none())
    {
        py::tuple subarray = subdtype.cast<py::tuple>();
        py::handle base = subarray[0];
        array_strides.push_back(base.attr("itemsize").cast<std::uint64_t>());
        validate_dtype_alignment(base, record_address, offset, record_stride, array_strides);
        array_strides.pop_back();
        return;
    }

    py::object names = dtype.attr("names");
    if(names.is_none())
        return;
    py::dict fields = dtype.attr("fields");
    for(py::handle name : names)
    {
        py::tuple field = fields[name].cast<py::tuple>();
        validate_dtype_alignment(field[0], record_address, offset + field[1].cast<std::uint64_t>(),
                                 record_stride, array_strides);
    }
}

py::dtype records_dtype(const Description &description, const void *record_data)
{
    auto numpy = py::module_::import("numpy");
    const std::uint64_t padded_fields = (std::uint64_t(description.field_bytes) + 7) & ~7ull;
    const std::uint64_t record_stride = 16 + padded_fields;
    const auto record_address = reinterpret_cast<std::uintptr_t>(record_data);
    if(record_address % 8 || record_stride % 8)
        throw std::runtime_error("record storage is not 8-byte aligned");

    py::list names;
    py::list formats;
    py::list offsets;
    names.append("index");
    names.append("timestamp");
    formats.append(numpy.attr("dtype")("<u8"));
    formats.append(numpy.attr("dtype")("<u8"));
    offsets.append(0);
    offsets.append(8);

    const std::uint32_t app_offset = 16;
    if(description.field_bytes)
    {
        py::object app_dtype = description.field_dtype.empty()
                                   ? numpy.attr("dtype")("u1")
                                   : numpy_field_dtype(numpy, description.field_dtype);
        if(app_dtype.attr("hasobject").cast<bool>())
            throw std::invalid_argument("field_dtype cannot contain Python object fields");
        auto field_names = app_dtype.attr("names");
        if(field_names.is_none())
        {
            py::object field_format = app_dtype;
            const auto itemsize = app_dtype.attr("itemsize").cast<std::uint64_t>();
            if(!itemsize || description.field_bytes % itemsize)
                throw std::invalid_argument(
                    "field_dtype itemsize must divide field_bytes for scalar application fields");
            const auto count = description.field_bytes / itemsize;
            if(count > 1)
                field_format = numpy.attr("dtype")(py::make_tuple(app_dtype, count));
            std::vector<std::uint64_t> array_strides;
            validate_dtype_alignment(field_format, record_address, app_offset, record_stride,
                                     array_strides);
            names.append("fields");
            formats.append(field_format);
            offsets.append(app_offset);
        }
        else
        {
            std::vector<std::uint64_t> array_strides;
            validate_dtype_alignment(app_dtype, record_address, app_offset, record_stride,
                                     array_strides);
            py::dict app_fields = app_dtype.attr("fields");
            for(py::handle field_name : field_names)
            {
                const std::string name = py::cast<std::string>(field_name);
                if(name == "index" || name == "timestamp")
                    throw std::invalid_argument(
                        "application field names cannot be 'index' or 'timestamp'");
                py::tuple field = app_fields[field_name].cast<py::tuple>();
                const auto offset = field[1].cast<std::uint64_t>();
                if(offset > description.field_bytes ||
                   field[0].attr("itemsize").cast<std::uint64_t>() >
                       description.field_bytes - offset)
                    throw std::invalid_argument("field_dtype extends beyond field_bytes");
                names.append(field_name);
                formats.append(field[0]);
                offsets.append(app_offset + offset);
            }
        }
    }

    py::dict spec;
    spec["names"] = names;
    spec["formats"] = formats;
    spec["offsets"] = offsets;
    spec["itemsize"] = app_offset + padded_fields;
    return numpy.attr("dtype")(spec).cast<py::dtype>();
}

py::ssize_t checked_ssize(std::uint64_t value)
{
    if(value > static_cast<std::uint64_t>(std::numeric_limits<py::ssize_t>::max()))
        throw std::overflow_error("array dimensions exceed NumPy's index range");
    return static_cast<py::ssize_t>(value);
}

class PythonBatch
{
  public:
    PythonBatch(Batch batch, Description description, int gpu = -1)
        : batch_(std::move(batch)), description_(std::move(description)), gpu_(gpu)
    {
    }

    std::size_t frames() const
    {
        return batch_.frames();
    }

    std::uint64_t index(std::size_t frame) const
    {
        return batch_.index(frame);
    }

    bool gpu() const
    {
        return batch_.gpu();
    }

    py::array array(const std::shared_ptr<PythonBatch> &self) const
    {
        if(batch_.gpu()) // its payload is GPU memory: NumPy would read a device pointer
            throw py::value_error("a GPU batch has no host payload array");
        if(description_.max_payload_bytes)
            throw py::value_error("an opaque batch has no array: read each frame");
        const auto dtype = payload_dtype(description_.element);
        std::vector<py::ssize_t> shape;
        shape.reserve(description_.shape.size() + 1);
        shape.push_back(checked_ssize(batch_.frames()));
        for(auto extent : description_.shape)
            shape.push_back(checked_ssize(extent));

        std::vector<py::ssize_t> strides(shape.size());
        py::ssize_t stride = dtype.itemsize();
        for(std::size_t i = shape.size(); i > 0; --i)
        {
            strides[i - 1] = stride;
            if(shape[i - 1] && stride > std::numeric_limits<py::ssize_t>::max() / shape[i - 1])
                throw std::overflow_error("array strides exceed NumPy's index range");
            stride *= shape[i - 1];
        }

        py::array result(dtype, shape, strides, const_cast<void *>(batch_.payload()),
                         py::cast(self));
        result.attr("setflags")(py::arg("write") = false);
        return result;
    }

    // One frame's payload, as it is in the receive ring.
    py::array payload(const std::shared_ptr<PythonBatch> &self, std::size_t frame) const
    {
        if(batch_.gpu())
            throw py::value_error("a GPU batch's frames are in GPU memory");
        if(frame >= batch_.frames())
            throw py::index_error("batch frame");
        const auto bytes = batch_.payload(frame);
        py::array result(py::dtype::of<std::uint8_t>(), {checked_ssize(bytes.size())}, {1},
                         const_cast<std::byte *>(bytes.data()), py::cast(self));
        result.attr("setflags")(py::arg("write") = false);
        return result;
    }

    py::array records(const std::shared_ptr<PythonBatch> &self) const
    {
        py::array result(records_dtype(description_, batch_.records()),
                         {checked_ssize(batch_.frames())},
                         {checked_ssize(16 + ((description_.field_bytes + 7ull) & ~7ull))},
                         const_cast<void *>(batch_.records()), py::cast(self));
        result.attr("setflags")(py::arg("write") = false);
        return result;
    }

    std::shared_ptr<PythonBatch> copy() const
    {
        return std::make_shared<PythonBatch>(batch_.copy(), description_);
    }

  private:
    friend class PythonGpuView;
    Batch batch_;
    Description description_;
    int gpu_ = -1;
    bool viewed_ = false;
};

// DLPack's stream: None and -1 name none, 1 is CUDA's default stream, 2 its per-thread default,
// anything larger a stream's pointer.
std::optional<GpuStream> gpu_stream(py::handle stream)
{
    if(stream.is_none())
        return std::nullopt;
    const auto value = stream.cast<std::intptr_t>();
    if(value == -1)
        return std::nullopt;
    if(value <= 0)
        throw py::buffer_error("stream must be None, -1, 1, 2 or a CUDA stream's pointer");
    return GpuStream{value == 1 ? nullptr : reinterpret_cast<void *>(value)};
}

// One DLPack export. The managed-tensor deleter destroys it: the hold ends before the batch.
struct Export
{
    DLManagedTensorVersioned managed{};
    std::vector<std::int64_t> shape, strides;
    std::shared_ptr<PythonBatch> batch;
    std::shared_ptr<void> hold;
};

// The GPU view of one batch. The first export begins the batch's hold on the view's GPU stream;
// the view's scope and every export keep it, and the last of them to end records the event.
class PythonGpuView
{
  public:
    PythonGpuView(std::shared_ptr<PythonBatch> batch, py::handle stream)
        : batch_(std::move(batch)), stream_(gpu_stream(stream))
    {
        if(!batch_->gpu() || std::exchange(batch_->viewed_, true))
            throw std::logic_error("gpu_view needs a GPU batch; a batch has one GPU view");
    }

    py::tuple device() const
    {
        return py::make_tuple(int(kDLCUDA), require_open().gpu_);
    }

    py::capsule dlpack(py::handle stream, py::handle max_version, py::handle dl_device,
                       py::handle copy)
    {
        const PythonBatch &batch = require_open();
        // The unversioned capsule cannot say read-only, and the receive ring is.
        if(max_version.is_none() || max_version.cast<std::pair<int, int>>().first < 1)
            throw py::buffer_error("a GPU view exports DLPack 1.0: pass max_version=(1, 0)");
        if(!dl_device.is_none() && !dl_device.equal(device()))
            throw py::buffer_error("a GPU view exports on the GPU of its batch only");
        if(copy.ptr() == Py_True)
            throw py::buffer_error("a GPU view does not copy");
        const auto named = gpu_stream(stream);
        if(named && stream_ && named->handle != stream_->handle)
            throw py::buffer_error("every export of a GPU view uses the view's GPU stream");
        if(!stream_)
            stream_ = named.value_or(GpuStream{});
        if(!hold_)
            hold_ = transport::begin_gpu_view(batch.batch_, *stream_);

        auto e = std::make_unique<Export>();
        e->batch = batch_;
        e->hold = hold_;
        e->shape.push_back(checked_ssize(batch.frames()));
        for(auto extent : batch.description_.shape)
            e->shape.push_back(checked_ssize(extent));
        e->strides.assign(e->shape.size(), 1); // in elements
        for(std::size_t i = e->shape.size() - 1; i > 0; --i)
            e->strides[i - 1] = e->strides[i] * e->shape[i];
        const auto dtype = payload_dtype(batch.description_.element);
        const auto code = dtype.kind() == 'u' ? kDLUInt : dtype.kind() == 'i' ? kDLInt : kDLFloat;
        e->managed.version = {1, 0};
        e->managed.manager_ctx = e.get();
        e->managed.deleter = [](DLManagedTensorVersioned *m)
        { delete static_cast<Export *>(m->manager_ctx); };
        e->managed.flags = DLPACK_FLAG_BITMASK_READ_ONLY;
        e->managed.dl_tensor = {const_cast<void *>(batch.batch_.payload()),
                                {kDLCUDA, batch.gpu_},
                                std::int32_t(e->shape.size()),
                                {std::uint8_t(code), std::uint8_t(dtype.itemsize() * 8), 1},
                                e->shape.data(),
                                e->strides.data(),
                                0};
        // A consumer renames the capsule and owns the deleter; an abandoned capsule calls it here.
        py::capsule capsule(&e->managed, "dltensor_versioned", [](PyObject *abandoned) {
            if(PyCapsule_IsValid(abandoned, "dltensor_versioned"))
            {
                auto *m = static_cast<DLManagedTensorVersioned *>(
                    PyCapsule_GetPointer(abandoned, "dltensor_versioned"));
                m->deleter(m);
            }
        });
        (void)e.release();
        return capsule;
    }

    void close()
    {
        hold_.reset();
        batch_.reset();
    }

  private:
    const PythonBatch &require_open() const
    {
        if(!batch_)
            throw py::buffer_error("the GPU view has ended");
        return *batch_;
    }

    std::shared_ptr<PythonBatch> batch_;
    std::shared_ptr<void> hold_; // ends before batch_
    std::optional<GpuStream> stream_;
};

class PythonCloser
{
  public:
    explicit PythonCloser(py::function function) : function_(function.ptr())
    {
        Py_INCREF(function_);
    }

    ~PythonCloser()
    {
        if(function_ && python_runtime_available())
        {
            const auto state = PyGILState_Ensure();
            Py_DECREF(function_);
            PyGILState_Release(state);
        }
    }

    bool call(std::string_view document, Deadline deadline) const
    {
        const double remaining =
            std::max(0.0, std::chrono::duration<double>(deadline - Clock::now()).count());
        if(!python_runtime_available())
            return false;
        py::gil_scoped_acquire acquire;
        PyObject *doc = PyUnicode_FromStringAndSize(document.data(), document.size());
        if(!doc)
        {
            PyErr_Clear();
            return false;
        }
        PyObject *seconds = PyFloat_FromDouble(remaining);
        if(!seconds)
        {
            Py_DECREF(doc);
            PyErr_Clear();
            return false;
        }
        PyObject *result = PyObject_CallFunctionObjArgs(function_, doc, seconds, nullptr);
        Py_DECREF(doc);
        Py_DECREF(seconds);
        if(!result)
        {
            PyErr_Clear();
            return false;
        }
        const int accepted = PyObject_IsTrue(result);
        Py_DECREF(result);
        if(accepted < 0)
        {
            PyErr_Clear();
            return false;
        }
        return accepted != 0;
    }

  private:
    static bool python_runtime_available() noexcept
    {
        if(Py_IsInitialized() == 0)
            return false;
#if PY_VERSION_HEX >= 0x030D0000
        return Py_IsFinalizing() == 0;
#else
        return _Py_IsFinalizing() == 0;
#endif
    }

    PyObject *function_ = nullptr;
};

class PythonSubscription
{
  public:
    PythonSubscription(Subscription subscription, int gpu)
        : subscription_(std::move(subscription)), description_(subscription_.description()),
          gpu_(gpu)
    {
    }

    ~PythonSubscription()
    {
        try
        {
            close(2.0);
        }
        catch(...)
        {
        }
    }

    py::object read(py::handle timeout)
    {
        const auto deadline = read_deadline(timeout);
        std::optional<Batch> batch;
        {
            py::gil_scoped_release release;
            batch = subscription_.read(deadline);
        }
        if(!batch)
            return py::none();
        return py::cast(std::make_shared<PythonBatch>(std::move(*batch), description_, gpu_));
    }

    int fd() const
    {
        return subscription_.fd();
    }

    py::object outcome() const
    {
        return outcome_name(subscription_.outcome());
    }

    py::dict description() const
    {
        return description_dict(description_);
    }

    py::dict health() const
    {
        return health_dict(subscription_.health());
    }

    void close(double timeout)
    {
        const auto deadline = deadline_after(timeout);
        py::gil_scoped_release release;
        subscription_.close(deadline);
    }

  private:
    Subscription subscription_;
    Description description_;
    int gpu_;
};

class PythonPendingSubscription
{
  public:
    PythonPendingSubscription(transport::PendingSubscription pending, int gpu)
        : pending_(std::move(pending)), gpu_(gpu)
    {
    }

    std::string open_message() const
    {
        return pending_.open_message();
    }

    std::shared_ptr<PythonSubscription> start(const std::string &open_reply, py::object closer,
                                              double timeout_seconds)
    {
        const auto deadline = deadline_after(timeout_seconds);
        transport::Closer close;
        if(!closer.is_none())
        {
            if(!PyCallable_Check(closer.ptr()))
                throw std::invalid_argument("closer must be callable or None");
            auto callback =
                std::make_shared<PythonCloser>(py::reinterpret_borrow<py::function>(closer));
            close = [callback = std::move(callback)](std::string_view document, Deadline by)
            { return callback->call(document, by); };
        }

        std::optional<Subscription> subscription;
        {
            py::gil_scoped_release release;
            subscription.emplace(std::move(pending_).start(open_reply, deadline, std::move(close)));
        }
        return std::make_shared<PythonSubscription>(std::move(*subscription), gpu_);
    }

  private:
    transport::PendingSubscription pending_;
    int gpu_;
};

std::shared_ptr<PythonPendingSubscription> prepare(py::handle ucx_stream_json, py::handle every,
                                                   py::handle budget, py::handle batch,
                                                   py::handle max_wait_seconds, py::handle max_rate,
                                                   py::handle allow_gpu_over_tcp, py::handle label,
                                                   int gpu)
{
    const auto document = require_string(ucx_stream_json, "ucx_stream_json");
    const bool delivery_every = require_bool(every, "every");
    SubscriptionOptions options;
    options.budget = require_unsigned(budget, "budget");
    const auto batch_size = require_unsigned(batch, "batch");
    if(batch_size > std::numeric_limits<std::uint32_t>::max())
        throw std::invalid_argument("batch is outside the supported range");
    options.batch = static_cast<std::uint32_t>(batch_size);

    const double max_wait = require_seconds(max_wait_seconds, "max_wait_seconds");
    const std::chrono::duration<double, std::micro> max_wait_us = std::chrono::duration<double>(max_wait);
    if(max_wait_us.count() > std::numeric_limits<std::int64_t>::max())
        throw std::invalid_argument("max_wait_seconds is outside the supported range");
    options.max_wait = std::chrono::microseconds(static_cast<std::int64_t>(max_wait_us.count()));

    if(max_rate.is_none())
        options.max_rate = 0;
    else
        options.max_rate = require_seconds(max_rate, "max_rate");
    options.allow_gpu_over_tcp = require_bool(allow_gpu_over_tcp, "allow_gpu_over_tcp");
    options.label = require_string(label, "label");
    options.gpu = gpu;

    auto offer = transport::parse_ucx_stream(document);
    std::optional<transport::PendingSubscription> pending;
    {
        py::gil_scoped_release release;
        pending.emplace(transport::prepare(offer, delivery_every, std::move(options)));
    }
    return std::make_shared<PythonPendingSubscription>(std::move(*pending), gpu);
}

} // namespace

PYBIND11_MODULE(_core, module)
{
    module.doc() = "Native host transport bridge for tango_ucx.";

    py::class_<PythonBatch, std::shared_ptr<PythonBatch>>(module, "Batch")
        .def_property_readonly("array", [](const std::shared_ptr<PythonBatch> &batch)
                               { return batch->array(batch); })
        .def_property_readonly("records", [](const std::shared_ptr<PythonBatch> &batch)
                               { return batch->records(batch); })
        .def_property_readonly("frames", &PythonBatch::frames)
        .def_property_readonly("gpu", &PythonBatch::gpu)
        .def("index", &PythonBatch::index, py::arg("frame"))
        .def(
            "payload", [](const std::shared_ptr<PythonBatch> &batch, std::size_t frame)
            { return batch->payload(batch, frame); }, py::arg("frame"))
        .def("copy", &PythonBatch::copy)
        .def(
            "gpu_view",
            [](const std::shared_ptr<PythonBatch> &batch, py::handle stream)
            { return std::make_shared<PythonGpuView>(batch, stream); },
            py::arg("stream") = py::none());

    py::class_<PythonGpuView, std::shared_ptr<PythonGpuView>>(module, "GpuView")
        .def("dlpack", &PythonGpuView::dlpack, py::arg("stream"), py::arg("max_version"),
             py::arg("dl_device"), py::arg("copy"))
        .def_property_readonly("device", &PythonGpuView::device)
        .def("close", &PythonGpuView::close);

    py::class_<PythonSubscription, std::shared_ptr<PythonSubscription>>(module, "Subscription")
        .def("read", &PythonSubscription::read, py::arg("timeout") = py::none())
        .def_property_readonly("fd", &PythonSubscription::fd)
        .def_property_readonly("outcome", &PythonSubscription::outcome)
        .def_property_readonly("description", &PythonSubscription::description)
        .def("health", &PythonSubscription::health)
        .def("close", &PythonSubscription::close, py::arg("timeout") = 5.0);

    py::class_<PythonPendingSubscription, std::shared_ptr<PythonPendingSubscription>>(
        module, "PendingSubscription")
        .def("open_message", &PythonPendingSubscription::open_message)
        .def("start", &PythonPendingSubscription::start, py::arg("open_reply"), py::arg("closer"),
             py::arg("timeout_seconds"));

    module.def("prepare", &prepare, py::arg("ucx_stream_json"), py::arg("every"), py::arg("budget"),
               py::arg("batch"), py::arg("max_wait_seconds"), py::arg("max_rate"),
               py::arg("allow_gpu_over_tcp"), py::arg("label"), py::arg("gpu") = -1);
}
