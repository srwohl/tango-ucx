// SPDX-FileCopyrightText: 2026 Copyright contributors to the tango-bulk project
// SPDX-License-Identifier: LGPL-3.0-or-later
#include "transport.h"

#include <cuda_runtime.h>

#include <atomic>
#include <format>
#include <memory>
#include <stdexcept>

namespace TangoBulk::transport
{
namespace
{
void check(cudaError_t status, const char *call, int gpu)
{
    if(status != cudaSuccess)
        throw std::runtime_error(
            std::format("{} failed on GPU {}: {}", call, gpu, cudaGetErrorString(status)));
}
// CUDA's current GPU belongs to the thread, and the caller is the loop or any application
// thread: each call selects its GPU and puts the thread's own choice back.
class Selected
{
  public:
    explicit Selected(int gpu) noexcept
    {
        if(cudaGetDevice(&previous_) != cudaSuccess)
            previous_ = -1;
        status = previous_ == gpu ? cudaSuccess : cudaSetDevice(gpu);
        restore_ = previous_ >= 0 && previous_ != gpu && status == cudaSuccess;
    }
    Selected(const Selected &) = delete;
    Selected &operator=(const Selected &) = delete;
    ~Selected()
    {
        if(restore_)
            (void)cudaSetDevice(previous_);
    }
    cudaError_t status = cudaSuccess;

  private:
    int previous_ = -1;
    bool restore_ = false;
};
struct Event
{
    cudaEvent_t event = nullptr;
    int gpu = -1;
    std::atomic<bool> failed{false}; // record runs on an application thread, query on the loop
};
Event &event_of(GpuEvent e)
{
    return *static_cast<Event *>(e.handle);
}

class CudaRuntime final : public GpuRuntime
{
  public:
    Allocation allocate(int gpu, std::size_t bytes) override
    {
        Selected on(gpu);
        check(on.status, "cudaSetDevice", gpu);
        void *base = nullptr;
        check(cudaMalloc(&base, bytes), "cudaMalloc", gpu);
        return {base, bytes, gpu};
    }
    void release(Allocation a) override
    {
        Selected on(a.gpu);
        if(a.base && on.status == cudaSuccess)
            (void)cudaFree(a.base);
    }
    GpuEvent make_event(int gpu) override
    {
        Selected on(gpu);
        check(on.status, "cudaSetDevice", gpu);
        auto e = std::make_unique<Event>();
        e->gpu = gpu;
        check(cudaEventCreateWithFlags(&e->event, cudaEventDisableTiming), "cudaEventCreate", gpu);
        return {e.release()};
    }
    void destroy_event(GpuEvent e) override
    {
        if(!e.handle)
            return;
        std::unique_ptr<Event> owned(&event_of(e));
        Selected on(owned->gpu);
        if(on.status == cudaSuccess)
            (void)cudaEventDestroy(owned->event);
    }
    void record(GpuEvent e, GpuStream stream) override
    {
        Event &ev = event_of(e);
        Selected on(ev.gpu);
        if(on.status != cudaSuccess ||
           cudaEventRecord(ev.event, static_cast<cudaStream_t>(stream.handle)) != cudaSuccess)
            ev.failed.store(true, std::memory_order_release);
    }
    GpuEventState query(GpuEvent e) override
    {
        Event &ev = event_of(e);
        if(ev.failed.load(std::memory_order_acquire))
            return GpuEventState::Failed;
        Selected on(ev.gpu);
        cudaError_t status = on.status == cudaSuccess ? cudaEventQuery(ev.event) : on.status;
        if(status == cudaErrorNotReady)
            return GpuEventState::Pending;
        return status == cudaSuccess ? GpuEventState::Done : GpuEventState::Failed;
    }
    void copy_to_host(void *to, const void *from, std::size_t bytes) override
    {
        cudaPointerAttributes where{};
        check(cudaPointerGetAttributes(&where, from), "cudaPointerGetAttributes", -1);
        Selected on(where.device);
        check(on.status, "cudaSetDevice", where.device);
        check(cudaMemcpy(to, from, bytes, cudaMemcpyDeviceToHost), "cudaMemcpy", where.device);
    }
};
CudaRuntime runtime;
[[maybe_unused]] const bool registered = (register_gpu_runtime(runtime), true);
} // namespace
} // namespace TangoBulk::transport
