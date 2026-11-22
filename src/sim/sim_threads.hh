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
#include <cstdint>
#include <thread>
#include <vector>

#include "base/barrier.hh"
#include "sim/eventq.hh"

namespace gem5
{

class GlobalFastSyncEvent;

Event *doSimLoop(EventQueue *eventq);

/**
 * Coordinates simulation execution across M main event queues using a pool of
 * N <= M host worker threads (the gem5 main thread plus N - 1
 * subordinate worker threads).
 *
 * There are two potential execution scenarios:
 *
 * N = M
 * Threads are mapped to a single event queue when SimulatorThreads is
 * constructed.
 *
 * N < M
 * Threads dynamically self-schedule event queues. The main thread always
 * executes event queue 0 to ensure a single thread controls the Python GIL.
 *
 * In both modes, all threads synchronize at phaseBarrier, also called a
 * simulation quantum.
 *
 * Note that KVM requires a static thread-to-queue affinity, and so N should be
 * equal to M.
 */
class SimulatorThreads
{
  public:
    SimulatorThreads();

    SimulatorThreads(const SimulatorThreads &) = delete;
    SimulatorThreads &operator=(SimulatorThreads &) = delete;


    ~SimulatorThreads();

    Event *runUntilLocalExit();

    void terminateThreads();

  protected:
    struct FinishPhase
    {
        SimulatorThreads *simThreads;
        void
        operator()() const noexcept
        {
            simThreads->finishPhase();
        }
    };

    static uint32_t selectNumThreads();

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
    void scheduleSimQuantum(uint32_t eventq_idx);

    /**
     * The main function for all subordinate worker threads (workers
     * 1..numThreads-1). Each pass waits on barrier, services event queues
     * phase-by-phase alongside worker 0 until a global exit event is
     * reached, and synchronizes at barrier before returning to idle.
     */
    void subordinateThreadMain(bool dynamic, uint32_t eventq_idx);

    /**
     * Static subordinate thread execution that only processes events on
     * eventq_idx
     */
    void runStatic(uint32_t eventq_idx);

    /**
     * The subordinate self-schedules threads (except event queue 0)
     */
    void runSubordinateDynamic();

    /**
     * The main thread self-schedules threads but also starts by processing
     * queue 0
     */
    void runMainDynamic();

    std::atomic<bool> terminate;
    bool phaseExit;

    // Number of threads may be different from numSimThreads
    uint32_t numThreads;

    std::atomic<uint32_t> nextQueue;

    std::vector<std::thread> threads;

    // This barrier helps persist threads through multiple runUntilLocalExit
    // calls
    Barrier<> barrier;

    // A phase is separated by global or exit events
    Barrier<FinishPhase> phaseBarrier;

    // Return value of doSimLoop() for all event queues
    std::vector<Event *> phaseEvents;

    // Queues schedule their own sim quantum if this flag is set
    bool scheduleSimQuantumEvent = false;

    // Global event whose barriers are used for sim quantum
    GlobalFastSyncEvent *simQuantumEvent = nullptr;

    // Shared variable for the next sim quantum tick
    Tick nextSimQuantum = 0;

    // Barriers in the sim quantum global event, one per event queue
    std::vector<Event *> simQuantumBarriers;
};

} // namespace gem5

#endif // __SIM_SIM_THREADS_HH__
