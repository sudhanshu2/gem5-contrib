/*
 * Copyright (c) 2026 Google LLC
 * All rights reserved.
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

#ifndef __SIM_SIM_THREADS_HH__
#define __SIM_SIM_THREADS_HH__

#include <atomic>
#include <barrier>
#include <cstdint>
#include <functional>
#include <thread>
#include <vector>

#include "base/barrier.hh"
#include "sim/eventq.hh"
#include "sim/global_event.hh"

namespace gem5
{

/**
 * Coordinates simulation execution across M main event queues using a pool of
 * N <= M host worker threads (the calling thread as worker 0 plus N - 1
 * subordinate worker threads).
 *
 * When N == M (the default when Root.num_threads is 0 or >= M), worker w
 * statically services mainEventQueue[w] in every phase, preserving the fixed
 * thread-to-queue affinity required by KVM CPUs and host cache locality.
 * When N < M, workers dynamically self-schedule queues from [0, M) off the
 * shared atomic index nextQueue in each phase. In both modes, all workers
 * synchronize at phaseBarrier whenever the queues reach a global event
 * (quantum sync, stat dump, debug break, or simulation loop exit), and
 * finishPhase() dispatches the global event once on mainEventQueue[0].
 */
class SimulatorThreads
{
  public:
    using QueueServiceFn = std::function<Event *(EventQueue *)>;

    SimulatorThreads() = delete;
    SimulatorThreads(const SimulatorThreads &) = delete;
    SimulatorThreads &operator=(SimulatorThreads &) = delete;

    SimulatorThreads(uint32_t num_queues, uint32_t num_threads,
                     QueueServiceFn service_queue);

    ~SimulatorThreads();

    Event *runUntilLocalExit();

    void terminateThreads();

  protected:
    template <typename T> struct alignas(64) CacheAlignedData
    {
        T data;
    };

    struct FinishPhase
    {
        SimulatorThreads *simThreads;
        void
        operator()() const noexcept
        {
            simThreads->finishPhase();
        }
    };

    static uint32_t selectNumThreads(uint32_t num_queues,
                                     uint32_t requested_threads);

    /**
     * Advance all main event queues through one or more global-event
     * phases (quanta, stat dumps, debug breaks, or exits) until a global
     * exit event or async_exception terminates the simulation pass.
     *
     * When numThreads == numQueues, worker_id statically services
     * mainEventQueue[worker_id] in every phase. When numThreads < numQueues,
     * the N worker threads dynamically self-schedule queues from
     * [0, numQueues) off the shared atomic counter nextQueue. For each
     * queue q, serviceQueue() processes events until EventQueue::serviceOne()
     * encounters a BaseGlobalEvent::BarrierEvent and returns it unprocessed,
     * storing that barrier event in phaseEvents[q]. Once all numQueues
     * queues have stopped at their barrier event for the phase, all N
     * worker threads synchronize at phaseBarrier, which invokes
     * finishPhase() on the last arriving thread before releasing the
     * workers into the next phase (or exiting the loop if phaseExit is
     * set).
     */
    void runPhases(uint32_t worker_id);

    /**
     * Completion function for phaseBarrier. Runs single-threaded on the
     * last worker thread to arrive at the end of a phase while all other
     * worker threads are blocked at phaseBarrier.
     */
    void finishPhase() noexcept;

    // Deschedule the sim quantum barrier from every event queue that still
    // has it pending. Only called from the main thread while all
    // subordinate threads are parked on barrier.
    void descheduleSimQuantum();

    // non-async schedule sim quantum on eventq
    void scheduleSimQuantum(uint32_t eventq_idx, EventQueue *eventq);

    /**
     * The main function for all subordinate worker threads (workers
     * 1..numThreads-1). Each pass waits on barrier, services event queues
     * phase-by-phase alongside worker 0 until a global exit event is
     * reached, and synchronizes at barrier before returning to idle.
     */
    void thread_main(uint32_t worker_id);

    std::atomic<bool> terminate;
    uint32_t numQueues;
    uint32_t numThreads;
    QueueServiceFn serviceQueue;
    std::atomic<uint32_t> nextQueue;
    std::vector<Event *> phaseEvents;
    Event *phaseExitEvent;
    bool phaseExit;
    std::vector<std::thread> threads;
    std::barrier<> barrier;
    Barrier<FinishPhase> phaseBarrier;

    // queues schedule their own sim quantum if this flag is set
    bool schedule_sim_quantum_event = false;

    // true if this simulation uses a sim quantum (more than one event
    // queue and a non-zero simQuantum)
    bool use_sim_quantum = false;

    // global event whose barriers are used for sim quantum
    static GlobalFastSyncEvent *sim_quantum_event;

    // shared variable for the next sim quantum tick
    Tick next_sim_quantum = 0;

    // barriers in the sim quantum global event, one per event queue
    std::vector<CacheAlignedData<Event *>> sim_quantum_barriers;
};

} // namespace gem5

#endif // __SIM_SIM_THREADS_HH__
