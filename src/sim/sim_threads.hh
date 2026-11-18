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
#include <list>
#include <memory>
#include <thread>
#include <vector>

#include "base/barrier.hh"
#include "sim/eventq.hh"

namespace gem5
{

class GlobalFastSyncEvent;

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
  private:
    template <typename T>
    struct alignas(64) CacheAlignedData
    {
      T data;;
    };

    class SimThread
    {
      public:
        SimThread(SimulatorThreads *ctrl)
            : ctrl(ctrl), run_dynamic(true) { }
        SimThread(SimulatorThreads *ctrl, uint32_t eventq_idx)
            : ctrl(ctrl), run_dynamic(false), eventq_idx(eventq_idx) {}

        SimThread(const SimThread &) = delete;
        SimThread &operator=(const SimThread &) = delete;

        void start();
        void thread_main();
        void join();

      private:
        // 1-1 queue to thread mapping sim loop
        void runStatic();

        // threads dynamically select queues for execution
        void runDynamic();

        SimulatorThreads *ctrl;
        bool run_dynamic;
        uint32_t eventq_idx;
        std::thread thread;
    };

    struct BarrierFunction
    {
        SimulatorThreads *simulator_threads;
        void operator()() const { simulator_threads->processBarrier(); }
    };

    // Simulator threads that execute non-main event queues
    std::list<SimThread> threads;

    // threads terminate if this flag is set
    bool terminate;

    // number of threads available to simulator (including main thread)
    uint32_t num_threads;

    // number of subordinate threads (num_threads - 1)
    uint32_t num_subordinate_threads;
    
    // Used for dynamically selecting event queues
    std::atomic<uint32_t> next_queue;

    // exit events for all event queues
    std::vector<CacheAlignedData<Event*>> phase_events;

    // queues schedule their own sim quantum if this flag is set
    bool schedule_sim_quantum_event;

    // global event whose barriers are used for sim quantum
    static GlobalFastSyncEvent *sim_quantum_event;

    // shared variable for the next sim quantum tick
    Tick next_sim_quantum;

    // barriers in sim_quantum_event global event
    std::vector<CacheAlignedData<Event*>> sim_quantum_barriers;

    // barrier used to synchronize threads
    std::unique_ptr<Barrier<BarrierFunction>> barrier;

    // barrier function executed by the last thread at it
    void processBarrier();

    // main thread sim loop on the main thread
    void runMainStatic();

     // main thread and other event queues on main thread
    void runMainDynamic();

    // non-async schedule sim quantum on eventq
    void scheduleSimQuantum(uint32_t eventq_idx, EventQueue *eventq);

  public:
    SimulatorThreads();

    SimulatorThreads(const SimulatorThreads &) = delete;
    SimulatorThreads &operator=(SimulatorThreads &) = delete;

    ~SimulatorThreads();

    Event *runUntilLocalExit();
    void terminateThreads();
};

} // namespace gem5

#endif // __SIM_SIM_THREADS_HH__
