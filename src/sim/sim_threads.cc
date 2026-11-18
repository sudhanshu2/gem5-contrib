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
#include <csignal>
#include <mutex>
#include <utility>

#include "base/logging.hh"
#include "base/thread.hh"
#include "sim/global_event.hh"

namespace gem5
{

Event *doSimLoop(EventQueue *);
Event *doFastSimLoop(EventQueue *);

GlobalFastSyncEvent *SimulatorThreads::sim_quantum_event = nullptr;

void
SimulatorThreads::SimThread::start()
{
  thread = std::thread([this] { thread_main(); });
}

void
SimulatorThreads::SimThread::thread_main()
{
  blockSignal(SIGINT);
  // no fixed event queue if eventq_idx is not specified
  if (run_dynamic == true) {
    runDynamic();
  } else {
    runStatic();
  }
  unblockSignal(SIGINT);
}

void
SimulatorThreads::SimThread::join()
{
  if (thread.joinable()) {
        thread.join();
    }
}

SimulatorThreads::SimulatorThreads()
    : terminate(false),
      schedule_sim_quantum_event(false)
{
    if (numSimThreads > numMainEventQueues) {
        warn("Requested %u simulator threads for %u main event queues; "
             "using %u instead\n",
             numSimThreads, numMainEventQueues, numMainEventQueues);
        num_threads = numMainEventQueues;
    } else {
      if (numSimThreads == 0) {
        num_threads = numMainEventQueues;
      } else {
        num_threads = numSimThreads;
      }
    }

    barrier = std::make_unique<Barrier<BarrierFunction>>(
        num_threads, BarrierFunction{ this });

    inform("Using %u simulator threads for %u main event queues\n", num_threads, numMainEventQueues);

    // main event queue 0 is not scheduled on a subordinate thread
    num_subordinate_threads = num_threads - 1;

    if (num_threads == numMainEventQueues) {
      // Assign a fixed event queue to a thread
      for (uint32_t i = 0; i < num_subordinate_threads; i++) {
        threads.emplace_back(this, i + 1);
      }
    } else {
      // Allow threads to dynamically assign event queues to themselves
      for (uint32_t i = 0; i < num_subordinate_threads; i++) {
        threads.emplace_back(this);
      }
    }

    if (sim_quantum_event == nullptr) {
      sim_quantum_event =
          new GlobalFastSyncEvent(EventBase::Minimum_Pri, 0);
    }
    for (uint32_t i = 0; i < numMainEventQueues; i++) {
      sim_quantum_barriers.emplace_back(
          sim_quantum_event->getBarrierEvent(i));
      phase_events.emplace_back(nullptr);
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
}

void
SimulatorThreads::SimThread::runStatic()
{
  panic_if(eventq_idx == 0, "cannot run main event queue on subordinate threads");
  panic_if(eventq_idx >= numMainEventQueues, "event queue does not exist");
  EventQueue *eventq = mainEventQueue[eventq_idx];
  while (!ctrl->terminate) {
    ctrl->scheduleSimQuantum(eventq_idx, eventq);
    ctrl->phase_events[eventq_idx].data = doFastSimLoop(eventq);
    ctrl->barrier->arrive_and_wait();
  }
}


void
SimulatorThreads::SimThread::runDynamic()
{
  while (!ctrl->terminate) {
    uint32_t curr_eventq =
        ctrl->next_queue.fetch_add(1, std::memory_order_relaxed);
    panic_if(curr_eventq == 0, "cannot run main event queue on subordinate threads");
    if (curr_eventq >= numMainEventQueues) {
       ctrl->barrier->arrive_and_wait();
    } else {
      panic_if(curr_eventq >= numMainEventQueues, "event queue does not exist");
      EventQueue *eventq = mainEventQueue[curr_eventq];
      ctrl->scheduleSimQuantum(curr_eventq, eventq);
      ctrl->phase_events[curr_eventq].data = doFastSimLoop(eventq);
    }
  }
}

void 
SimulatorThreads::runMainStatic() {
  EventQueue *eventq = mainEventQueue[0];
  while (!terminate) {
    scheduleSimQuantum(0, eventq);
    phase_events[0].data = doSimLoop(eventq);
    barrier->arrive_and_wait();
  }  
}

void 
SimulatorThreads::runMainDynamic() {  
  uint32_t curr_eventq = 0;
  while (!terminate) {
    if (curr_eventq >= numMainEventQueues) {
       barrier->arrive_and_wait();
       curr_eventq = 0;
    } else {
      EventQueue *eventq = mainEventQueue[curr_eventq];
      scheduleSimQuantum(curr_eventq, eventq);
      phase_events[curr_eventq].data = doSimLoop(eventq);
      curr_eventq = next_queue.fetch_add(1, std::memory_order_relaxed);
    }
  }
}

void
SimulatorThreads::scheduleSimQuantum(uint32_t eventq_idx, EventQueue *eventq) {
  curEventQueue(eventq);

  if (schedule_sim_quantum_event) {
    Event *sim_quantum_barrier = sim_quantum_barriers[eventq_idx].data;
    if (sim_quantum_barrier->scheduled()) {
      // still pending from a previous simulate() call that exited
      eventq->setNextSimQuantum(sim_quantum_barrier->when());
    } else {
      eventq->setNextSimQuantum(next_sim_quantum);
      eventq->schedule(sim_quantum_barrier, next_sim_quantum, true);
    }
  }
}

Event *
SimulatorThreads::runUntilLocalExit()
{
    assert(!terminate);

    next_queue.store(1, std::memory_order_seq_cst);
    next_sim_quantum = curTick() + simQuantum;

    if (numMainEventQueues > 1) {
      schedule_sim_quantum_event = true;
    }

    for (auto &thread : threads) {
      thread.start();
    }

    if (num_threads == numMainEventQueues) {
      runMainStatic();
    } else {
      runMainDynamic();
    }

    terminateThreads();

    curEventQueue(mainEventQueue[0]);

    return phase_events[0].data;
}

void
SimulatorThreads::processBarrier()
{
    // Reset the dynamic-dispatch queue counter for the next phase
    // before phaseBarrier releases the worker threads.
    next_queue.store(1, std::memory_order_seq_cst);

    // schedule sim quantum event only if needed
    schedule_sim_quantum_event = false;

    // sim loop only returns nullptr when mainEventQueue[0] exits
    // early due to an async_exception (e.g., a Python exception).
    // Signal all workers to exit runPhases() and return nullptr to
    // simulate().
    Event *main_eventq_event = phase_events[0].data;
    if (main_eventq_event == nullptr) {
      terminate = true;
      return;
    }

    BaseGlobalEvent *global_event = main_eventq_event->globalEvent();
#ifndef NDEBUG
    // Global events are scheduled across all queues under
    // BaseGlobalEvent::globalQMutex, so every queue must have stopped
    // at its local BarrierEvent for the exact same BaseGlobalEvent.
    panic_if(!global_event,
             "Queue 0 stopped at non-global event '%s' (%s)",
             main_eventq_event->name().c_str(),
             main_eventq_event->description());

    for (uint32_t i = 1; i < numMainEventQueues; ++i) {
        Event *ev = phase_events[i].data;
        panic_if(!ev || ev->globalEvent() != global_event,
                 "Queue %u stopped at '%s' (%s) instead of global event "
                 "'%s'",
                 i, ev ? ev->name().c_str() : "null",
                 ev ? ev->description() : "null",
                 global_event->description());
    }
#endif

    if (global_event->isSimQuantum()) {
      next_sim_quantum = curTick() + simQuantum;
      if (numMainEventQueues > 1) {
        schedule_sim_quantum_event = true;
      }
      return;
    } else {
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
    }

    if (main_eventq_event->isExitEvent()) {
        terminate = true;
    } else {
        // Release each queue's local BarrierEvent in reverse order so
        // slot 0 (whose BarrierEvent::~BarrierEvent deletes an
        // AutoDelete BaseGlobalEvent) runs after slots 1..numQueues-1
        // have finished accessing their BarrierEvents.
        for (uint32_t i = numMainEventQueues; i-- > 0;) {
            BaseGlobalEvent::releaseBarrierEvent(phase_events[i].data);
        }
    }
}

void
SimulatorThreads::terminateThreads()
{
    terminate = true;
    for (auto &thread : threads) {
      thread.join();
    }
}

} // namespace gem5
