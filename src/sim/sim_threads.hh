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
#include <cassert>
#include <csignal>
#include <cstdint>
#include <functional>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

#include "base/barrier.hh"
#include "base/logging.hh"
#include "base/thread.hh"
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
                     QueueServiceFn service_queue)
        : terminate(false),
          numQueues(num_queues),
          numThreads(selectNumThreads(num_queues, num_threads)),
          serviceQueue(std::move(service_queue)),
          nextQueue(0),
          phaseEvents(num_queues, nullptr),
          phaseExitEvent(nullptr),
          phaseExit(false),
          barrier(numThreads),
          phaseBarrier(numThreads, FinishPhase{this})
    {
        threads.reserve(numThreads - 1);

        // sim quantum is only needed a non-zero quantum only
        use_sim_quantum = num_queues > 1 && simQuantum > 0;

        if (use_sim_quantum) {
            if (sim_quantum_event == nullptr) {
                sim_quantum_event =
                    new GlobalFastSyncEvent(EventBase::Minimum_Pri, 0);
            }
            for (uint32_t i = 0; i < num_queues; i++) {
                sim_quantum_barriers.emplace_back(
                    sim_quantum_event->getBarrierEvent(i));
            }
        }
    }

    ~SimulatorThreads()
    {
        // This should only happen after exit has been
        // called. Subordinate event queues should normally (assuming
        // exit is called from Python) be waiting on the barrier when
        // this happens.
        //
        // N.B.: Not terminating here would make it impossible to
        // safely destroy the barrier.
        terminateThreads();
    }

    Event *
    runUntilLocalExit()
    {
        assert(!terminate);

        next_sim_quantum = curTick() + simQuantum;

        if (use_sim_quantum) {
            schedule_sim_quantum_event = true;
        }

        // Start subordinate threads if needed.
        if (threads.empty() && numThreads > 1) {
            // The main thread (the one running Python) is worker 0 and
            // participates alongside numThreads - 1 subordinate workers.
            for (uint32_t i = 1; i < numThreads; i++) {
                threads.emplace_back([this, i]() { thread_main(i); });
            }
        }

        nextQueue = 0;
        phaseExitEvent = nullptr;
        phaseExit = false;

        // This method is called from the main thread. All subordinate
        // threads are waiting on the barrier when the function is called.
        // The arrival of the main thread here satisfies the barrier and
        // starts another simulation pass across the worker pool.
        barrier.arrive_and_wait();
        runPhases(0);
        barrier.arrive_and_wait();

        // Do not leave quantum barriers scheduled between simulate()
        // calls; the next call schedules a fresh quantum from its own
        // curTick().
        descheduleSimQuantum();

        curEventQueue(mainEventQueue[0]);
        return phaseExitEvent;
    }

    void
    terminateThreads()
    {
        assert(!terminate);
        if (threads.empty()) {
            return;
        }

        /* This function should only be called when the simulator is
         * handling a global exit event (typically from Python). This
         * means that the helper threads will be waiting on the
         * barrier. Tell the helper threads to exit and release them from
         * their barrier. */
        terminate = true;
        barrier.arrive_and_wait();

        /* Wait for all of the threads to terminate */
        for (auto &t : threads) {
            t.join();
        }

        terminate = false;
        threads.clear();
    }

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

    static uint32_t
    selectNumThreads(uint32_t num_queues, uint32_t requested_threads)
    {
        assert(num_queues >= 1);
        if (requested_threads > num_queues) {
            warn("Requested %u simulator threads for %u main event queues; "
                 "clamping to %u\n",
                 requested_threads, num_queues, num_queues);
            return num_queues;
        }
        if (requested_threads == 0) {
            return num_queues;
        }
        return requested_threads;
    }

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
    void
    runPhases(uint32_t worker_id)
    {
        do {
            // Static 1:1 thread-to-queue mapping when numThreads == numQueues.
            // This is required for KVM CPUs (which bind vCPU signal delivery,
            // per-thread perf counters, and timers to the host OS thread that
            // services the CPU's event queue) to always keep the same thread
            // on the same queue across phases.
            if (numThreads == numQueues) {
                EventQueue *eventq = mainEventQueue[worker_id];
                scheduleSimQuantum(worker_id, eventq);
                phaseEvents[worker_id] = serviceQueue(eventq);
            } else {
                while (true) {
                    uint32_t q = nextQueue++;
                    if (q >= numQueues) {
                        break;
                    }
                    EventQueue *eventq = mainEventQueue[q];
                    scheduleSimQuantum(q, eventq);
                    phaseEvents[q] = serviceQueue(eventq);
                }
            }
            phaseBarrier.arrive_and_wait();
        } while (!phaseExit);
    }

    /**
     * Completion function for phaseBarrier. Runs single-threaded on the
     * last worker thread to arrive at the end of a phase while all other
     * worker threads are blocked at phaseBarrier.
     */
    void
    finishPhase() noexcept
    {
        // Reset the dynamic-dispatch queue counter for the next phase
        // before phaseBarrier releases the worker threads.
        nextQueue = 0;

        // schedule sim quantum event only if needed
        schedule_sim_quantum_event = false;

        // doSimLoop() only returns nullptr when mainEventQueue[0] exits
        // early due to an async_exception (e.g., a Python exception).
        // Signal all workers to exit runPhases() and return nullptr to
        // simulate().
        Event *first = phaseEvents[0];
        if (!first) {
            phaseExitEvent = nullptr;
            phaseExit = true;
            return;
        }

        // Global events are scheduled across all queues under
        // BaseGlobalEvent::globalQMutex, so every queue must have stopped
        // at its local BarrierEvent for the exact same BaseGlobalEvent.
        BaseGlobalEvent *global_event = first->globalEvent();
        panic_if(!global_event,
                 "Queue 0 stopped at non-global event '%s' (%s)",
                 first->name().c_str(), first->description());
#ifndef NDEBUG
        for (uint32_t i = 1; i < numQueues; ++i) {
            Event *ev = phaseEvents[i];
            panic_if(!ev || ev->globalEvent() != global_event,
                     "Queue %u stopped at '%s' (%s) instead of global event "
                     "'%s'",
                     i, ev ? ev->name().c_str() : "null",
                     ev ? ev->description() : "null",
                     global_event->description());
        }
#endif

        if (global_event->isSimQuantum()) {
            next_sim_quantum = first->when() + simQuantum;
            schedule_sim_quantum_event = true;
            return;
        }

        // Execute the global event action (e.g., GlobalSyncEvent::process()
        // to advance the quantum, or GlobalSimLoopExitEvent::process())
        // once on mainEventQueue[0]. Acquire mainEventQueue[0]'s service
        // lock because global event process() implementations (such as
        // DistIface::SyncEvent::process()) may construct
        // EventQueue::ScopedRelease(curEventQueue()).
        curEventQueue(mainEventQueue[0]);
        {
            std::lock_guard<EventQueue> lock(*mainEventQueue[0]);
            global_event->process();
        }

        if (first->isExitEvent()) {
            // Retain the exit event for simulate() to inspect and return;
            // simulate() cleans it up on the next entry.
            phaseExitEvent = first;
            phaseExit = true;
        } else {
            // Release each queue's local BarrierEvent in reverse order so
            // slot 0 (whose BarrierEvent::~BarrierEvent deletes an
            // AutoDelete BaseGlobalEvent) runs after slots 1..numQueues-1
            // have finished accessing their BarrierEvents.
            for (uint32_t i = numQueues; i-- > 0;) {
                BaseGlobalEvent::releaseBarrierEvent(phaseEvents[i]);
            }
        }
    }

    // Deschedule the sim quantum barrier from every event queue that still
    // has it pending. Only called from the main thread while all
    // subordinate threads are parked on barrier.
    void
    descheduleSimQuantum()
    {
        for (uint32_t i = 0; i < sim_quantum_barriers.size(); i++) {
            Event *sim_quantum_barrier = sim_quantum_barriers[i].data;
            if (sim_quantum_barrier->scheduled()) {
                EventQueue *eventq = mainEventQueue[i];
                curEventQueue(eventq);
                std::lock_guard<EventQueue> lock(*eventq);
                eventq->deschedule(sim_quantum_barrier);
            }
        }
    }

    // non-async schedule sim quantum on eventq
    void
    scheduleSimQuantum(uint32_t eventq_idx, EventQueue *eventq)
    {
        curEventQueue(eventq);

        if (schedule_sim_quantum_event) {
            Event *sim_quantum_barrier = sim_quantum_barriers[eventq_idx].data;
            eventq->setNextSimQuantum(next_sim_quantum);
            std::lock_guard<EventQueue> lock(*eventq);
            eventq->schedule(sim_quantum_barrier, next_sim_quantum);
        }
    }

    /**
     * The main function for all subordinate worker threads (workers
     * 1..numThreads-1). Each pass waits on barrier, services event queues
     * phase-by-phase alongside worker 0 until a global exit event is
     * reached, and synchronizes at barrier before returning to idle.
     */
    void
    thread_main(uint32_t worker_id)
    {
        // Keep SIGINT (ctrl-c) off the subordinate workers so that it is
        // always delivered to the main thread (worker 0), which handles it.
        blockSignal(SIGINT);

        while (true) {
            barrier.arrive_and_wait();
            if (terminate) {
                unblockSignal(SIGINT);
                break;
            }
            runPhases(worker_id);
            barrier.arrive_and_wait();
        }
    }

    std::atomic<bool> terminate;
    uint32_t numQueues;
    uint32_t numThreads;
    QueueServiceFn serviceQueue;
    std::atomic<uint32_t> nextQueue;
    std::vector<Event *> phaseEvents;
    Event *phaseExitEvent;
    bool phaseExit;
    std::vector<std::thread> threads;
    Barrier<> barrier;
    Barrier<FinishPhase> phaseBarrier;

    // queues schedule their own sim quantum if this flag is set
    bool schedule_sim_quantum_event = false;

    // true if this simulation uses a sim quantum (more than one event
    // queue and a non-zero simQuantum)
    bool use_sim_quantum = false;

    // global event whose barriers are used for sim quantum
    inline static GlobalFastSyncEvent *sim_quantum_event = nullptr;

    // shared variable for the next sim quantum tick
    Tick next_sim_quantum = 0;

    // barriers in the sim quantum global event, one per event queue
    std::vector<CacheAlignedData<Event *>> sim_quantum_barriers;
};

} // namespace gem5

#endif // __SIM_SIM_THREADS_HH__
