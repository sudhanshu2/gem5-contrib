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

#include "sim/sim_threads.hh"

#include <cassert>
#include <mutex>

#include "base/logging.hh"
#include "base/thread.hh"
#include "sim/global_event.hh"

namespace gem5
{

SimulatorThreads::SimulatorThreads()
    : terminate(false),
      phaseExit(false),
      numThreads(selectNumThreads()),
      nextQueue(1),
      barrier(numThreads),
      phaseBarrier(numThreads, FinishPhase{this})
{
    threads.reserve(numThreads - 1);

    // Initialize sim quantum (sync) event and store its barrier events
    simQuantumEvent = new GlobalFastSyncEvent(EventBase::Minimum_Pri, 0);
    simQuantumBarriers.reserve(numMainEventQueues);
    phaseEvents.assign(numMainEventQueues, nullptr);
    for (uint32_t i = 0; i < numMainEventQueues; i++) {
        simQuantumBarriers.push_back(simQuantumEvent->getBarrierEvent(i));
    }
}

SimulatorThreads::~SimulatorThreads()
{
    // This should only happen after exit has been
    // called. Subordinate event queues should normally (assuming
    // exit is called from Python) be waiting on the barrier when
    // this happens.
    //
    // N.B.: Not terminating here would make it impossible to
    // safely destroy the barrier.
    terminateThreads();
    delete simQuantumEvent;
}

Event *
SimulatorThreads::runUntilLocalExit()
{
    assert(!terminate.load(std::memory_order_relaxed));

    nextSimQuantum = curTick() + simQuantum;

    if (numMainEventQueues > 1 && simQuantum > 0) {
        scheduleSimQuantumEvent = true;
    }

    bool dynamic = numThreads != numMainEventQueues;

    // Start subordinate threads if needed.
    if (threads.empty() && numThreads > 1) {
        // The main thread (the one running Python) is worker 0 and
        // participates alongside numThreads - 1 subordinate workers.
        for (uint32_t i = 1; i < numThreads; i++) {
            threads.emplace_back(
                [this, dynamic, i]() { subordinateThreadMain(dynamic, i); });
        }
    }

    // Queue 0 is always processed by the main thread
    nextQueue = 1;
    phaseExit = false;

    // This method is called from the main thread. All subordinate
    // threads are waiting on the barrier when the function is called.
    // The arrival of the main thread here satisfies the barrier and
    // starts another simulation pass across the worker pool.
    barrier.arrive_and_wait();
    if (!dynamic) {
        runStatic(0);
    } else {
        runMainDynamic();
    }
    barrier.arrive_and_wait();

    // Do not leave quantum barriers scheduled between simulate()
    // calls; the next call schedules a fresh quantum from its own
    // curTick().
    descheduleSimQuantum();

    curEventQueue(mainEventQueue[0]);
    return phaseEvents[0];
}

void
SimulatorThreads::terminateThreads()
{
    assert(!terminate.load(std::memory_order_relaxed));
    if (threads.empty()) {
        return;
    }

    /* This function should only be called when the simulator is
     * handling a global exit event (typically from Python). This
     * means that the helper threads will be waiting on the
     * barrier. Tell the helper threads to exit and release them from
     * their barrier. */
    terminate.store(true, std::memory_order_relaxed);
    barrier.arrive_and_wait();

    /* Wait for all of the threads to terminate */
    for (auto &t : threads) {
        t.join();
    }

    terminate.store(false, std::memory_order_relaxed);
    threads.clear();
}

uint32_t
SimulatorThreads::selectNumThreads()
{
    assert(numMainEventQueues >= 1);
    if (numSimThreads > numMainEventQueues) {
        warn("Requested %u simulator threads for %u main event queues; "
             "clamping to %u\n",
             numSimThreads, numMainEventQueues, numMainEventQueues);
        return numMainEventQueues;
    }
    if (numSimThreads == 0) {
        return numMainEventQueues;
    }
    return numSimThreads;
}

void
SimulatorThreads::finishPhase() noexcept
{
    // Reset the dynamic-dispatch queue counter for the next phase
    // before phaseBarrier releases the worker threads.
    nextQueue = 1;

    // schedule sim quantum event only if needed
    scheduleSimQuantumEvent = false;

    // doSimLoop() only returns nullptr when mainEventQueue[0] exits
    // early due to an async_exception (e.g., a Python exception).
    // Signal all workers to exit runPhases() and return nullptr to
    // simulate().
    Event *first = phaseEvents[0];
    if (!first) {
        phaseExit = true;
        return;
    }

    // Global events are scheduled across all queues under
    // BaseGlobalEvent::globalQMutex, so every queue must have stopped
    // at its local BarrierEvent for the exact same BaseGlobalEvent.
    BaseGlobalEvent *global_event = first->globalEvent();
    panic_if(!global_event, "Queue 0 stopped at non-global event '%s' (%s)",
             first->name().c_str(), first->description());
#ifndef NDEBUG
    for (uint32_t i = 1; i < numMainEventQueues; ++i) {
        Event *ev = phaseEvents[i];
        panic_if(!ev || ev->globalEvent() != global_event,
                 "Queue %u stopped at '%s' (%s) instead of global event "
                 "'%s'",
                 i, ev ? ev->name().c_str() : "null",
                 ev ? ev->description() : "null", global_event->description());
    }
#endif

    if (global_event->isSimQuantum()) {
        nextSimQuantum = first->when() + simQuantum;
        scheduleSimQuantumEvent = true;
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
        phaseExit = true;
    } else {
        // Release each queue's local BarrierEvent in reverse order so
        // slot 0 (whose BarrierEvent::~BarrierEvent deletes an
        // AutoDelete BaseGlobalEvent) runs after slots 1..numQueues-1
        // have finished accessing their BarrierEvents.
        for (uint32_t i = numMainEventQueues; i-- > 0;) {
            BaseGlobalEvent::releaseBarrierEvent(phaseEvents[i]);
        }
    }
}

void
SimulatorThreads::descheduleSimQuantum()
{
    for (uint32_t i = 0; i < simQuantumBarriers.size(); i++) {
        Event *simQuantumBarrier = simQuantumBarriers[i];
        // Descheduling simQuantumBarrier event preserves a clean state
        if (simQuantumBarrier->scheduled()) {
            EventQueue *eventq = mainEventQueue[i];
            curEventQueue(eventq);
            std::lock_guard<EventQueue> lock(*eventq);
            eventq->deschedule(simQuantumBarrier);
        }
    }
}

void
SimulatorThreads::scheduleSimQuantum(uint32_t eventq_idx)
{
    EventQueue *eventq = mainEventQueue[eventq_idx];
    // This is executed frequently and so should be fast
    curEventQueue(eventq);
    if (scheduleSimQuantumEvent) {
        Event *simQuantumBarrier = simQuantumBarriers[eventq_idx];
        eventq->setNextSimQuantum(nextSimQuantum);
        // schedule() calls insert() without lock while other threads can also
        // be scheduling events to this queue using ScopedMigration
        std::lock_guard<EventQueue> lock(*eventq);
        eventq->schedule(simQuantumBarrier, nextSimQuantum);
    }
}

void
SimulatorThreads::subordinateThreadMain(bool dynamic, uint32_t eventq_idx)
{
    // Keep SIGINT (ctrl-c) off the subordinate workers so that it is
    // always delivered to the main thread (worker 0), which handles it.
    blockSignal(SIGINT);

    while (true) {
        barrier.arrive_and_wait();
        if (terminate.load(std::memory_order_relaxed)) {
            unblockSignal(SIGINT);
            break;
        }
        if (!dynamic) {
            panic_if(eventq_idx == 0,
                     "cannot execute eventq 0 on subordinate threads");
            runStatic(eventq_idx);
        } else {
            runSubordinateDynamic();
        }
        barrier.arrive_and_wait();
    }
}

void
SimulatorThreads::runStatic(uint32_t eventq_idx)
{
    EventQueue *eventq = mainEventQueue[eventq_idx];
    while (!phaseExit) {
        scheduleSimQuantum(eventq_idx);
        phaseEvents[eventq_idx] = doSimLoop(eventq);
        phaseBarrier.arrive_and_wait();
    }
}

void
SimulatorThreads::runSubordinateDynamic()
{
    while (!phaseExit) {
        uint32_t eventq_idx =
            nextQueue.fetch_add(1, std::memory_order_relaxed);
        if (eventq_idx >= numMainEventQueues) {
            phaseBarrier.arrive_and_wait();
        } else {
            EventQueue *eventq = mainEventQueue[eventq_idx];
            scheduleSimQuantum(eventq_idx);
            phaseEvents[eventq_idx] = doSimLoop(eventq);
        }
    }
}

void
SimulatorThreads::runMainDynamic()
{
    uint32_t eventq_idx = 0;
    while (!phaseExit) {
        if (eventq_idx >= numMainEventQueues) {
            phaseBarrier.arrive_and_wait();
            eventq_idx = 0;
        } else {
            EventQueue *eventq = mainEventQueue[eventq_idx];
            scheduleSimQuantum(eventq_idx);
            phaseEvents[eventq_idx] = doSimLoop(eventq);
            eventq_idx = nextQueue.fetch_add(1, std::memory_order_relaxed);
        }
    }
}

} // namespace gem5
