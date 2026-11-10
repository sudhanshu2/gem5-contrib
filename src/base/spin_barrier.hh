/*
 * Copyright (c) 2026 ARCANA Research Group (University of Illinois
 * Urbana-Champaign) All rights reserved
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met: redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer;
 * redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution;
 * neither the name of the copyright holders nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef __BASE_SPIN_BARRIER_HH__
#define __BASE_SPIN_BARRIER_HH__

#include <atomic>
#include <cstdint>
#include <utility>

#include "base/logging.hh"

namespace gem5
{

template <typename BarrierFunction> class SpinBarrier
{
  public:
    SpinBarrier(std::uint32_t expected_threads,
                BarrierFunction barrier_function)
        : expected_threads(expected_threads),
          barrier_function(std::move(barrier_function))
    {
        panic_if(expected_threads == 0,
                 "SpinBarrier needs at least one thread.");
    }

    SpinBarrier(const SpinBarrier &) = delete;
    SpinBarrier &operator=(const SpinBarrier &) = delete;

    void
    arrive_and_wait()
    {
        const bool curr_phase = global_phase.load(std::memory_order_relaxed);
        if (threads_at_barrier.fetch_add(1, std::memory_order_acq_rel) + 1 ==
            expected_threads) {
            barrier_function();
            threads_at_barrier.store(0, std::memory_order_relaxed);
            global_phase.store(!curr_phase, std::memory_order_release);
            return;
        }
        while (global_phase.load(std::memory_order_acquire) == curr_phase) {
#if defined(__x86_64__)
            __builtin_ia32_pause();
#endif
        }
    }

  private:
    alignas(64) std::atomic<std::uint32_t> threads_at_barrier{0};
    alignas(64) std::atomic<bool> global_phase{false};
    alignas(64) const std::uint32_t expected_threads;
    BarrierFunction barrier_function;
};

} // namespace gem5

#endif // __BASE_SPIN_BARRIER_HH__
