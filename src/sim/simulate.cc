/*
 * Copyright (c) 2021 Arm Limited
 * All rights reserved
 *
 * The license below extends only to copyright in the software and shall
 * not be construed as granting a license to any other intellectual
 * property including but not limited to intellectual property relating
 * to a hardware implementation of the functionality of the software
 * licensed hereunder.  You may use the software subject to the license
 * terms below provided that you ensure that this notice is replicated
 * unmodified and in its entirety in all distributions of the software,
 * modified or unmodified, in source code or in binary form.
 *
 * Copyright (c) 2006 The Regents of The University of Michigan
 * Copyright (c) 2013 Advanced Micro Devices, Inc.
 * Copyright (c) 2013 Mark D. Hill and David A. Wood
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

#include "sim/simulate.hh"

#include <barrier>
#include <csignal>
#include <stop_token>
#include <thread>

#include "base/logging.hh"
#include "base/pollevent.hh"
#include "base/trace.hh"
#include "base/types.hh"
#include "debug/EnteringEventQueue.hh"
#include "sim/async.hh"
#include "sim/control_queue.hh"
#include "sim/eventq.hh"
#include "sim/init_signals.hh"
#include "sim/sim_events.hh"
#include "sim/sim_exit.hh"
#include "sim/stat_control.hh"

namespace gem5
{

//! forward declaration
Event *doSimLoop(EventQueue *);

GlobalSimLoopExitEvent *simulate_limit_event = nullptr;

class SimulatorThreads
{
  public:
    SimulatorThreads() = delete;
    SimulatorThreads(const SimulatorThreads &) = delete;
    SimulatorThreads &operator=(SimulatorThreads &) = delete;

    /**
     * One worker thread runs each event queue, including queue 0. The
     * main thread (the one running Python) is an extra participant in
     * the barrier, so it counts num_queues + 1.
     */
    SimulatorThreads(uint32_t num_queues)
        : numQueues(num_queues), barrier(num_queues + 1)
    {
        threads.reserve(num_queues);
    }

    ~SimulatorThreads()
    {
        // This should only happen after exit has been
        // called. The worker threads should normally (assuming
        // exit is called from Python) be waiting on the barrier when
        // this happens.
        //
        // N.B.: Not terminating here would make it impossible to
        // safely destroy the barrier.
        terminateThreads();
    }

    /**
     * Run all event queues until they exit the simulation loop and
     * return the event that made queue 0 exit. Called from the main
     * thread, which acts as the control thread while the workers
     * simulate (see sim/control_queue.hh).
     */
    Event *
    runUntilLocalExit()
    {
        // Start the worker threads if needed.
        if (threads.empty()) {
            // The workers inherit this thread's signal mask. Block the
            // signals gem5 handles so they are delivered to this thread,
            // the control thread, even if the workers are stuck.
            sigset_t blocked, old;
            sigemptyset(&blocked);
            for (int sig : {SIGINT, SIGIO, SIGUSR1, SIGUSR2, SIGCONT}) {
                sigaddset(&blocked, sig);
            }
            pthread_sigmask(SIG_BLOCK, &blocked, &old);

            for (uint32_t i = 0; i < numQueues; i++) {
                threads.emplace_back(
                    [this](std::stop_token stop, EventQueue *eq) {
                        thread_main(stop, eq);
                    },
                    mainEventQueue[i]);
            }

            pthread_sigmask(SIG_SETMASK, &old, nullptr);
        }

        control::begin(numQueues);

        // All workers are waiting at the barrier. Arriving here starts
        // another iteration of their loop.
        barrier.arrive_and_wait();

        // Handle control work until every queue has left its
        // simulation loop.
        control::serveUntilDone();

        return exitEvent;
    }

    void
    terminateThreads()
    {
        if (threads.empty())
            return;

        /* This function should only be called when the simulator is
         * handling a global exit event (typically from Python). This
         * means that the worker threads will be waiting on the
         * barrier. A stop request does not wake a thread blocked on
         * the barrier, so release them from it as well. */
        for (auto &t : threads) {
            t.request_stop();
        }
        barrier.arrive_and_wait();

        /* Destroying a jthread joins it. */
        threads.clear();
    }

  protected:
    /**
     * The main function for all worker threads.  These threads wait on
     * the barrier until the main thread arrives, then run their event
     * queue concurrently.  When they exit the loop, they tell the main
     * thread and wait on the barrier for the next round.  This repeats
     * until the main thread requests a stop.
     */
    void
    thread_main(std::stop_token stop, EventQueue *queue)
    {
        while (true) {
            barrier.arrive_and_wait();
            if (stop.stop_requested()) {
                return;
            }

            Event *event = doSimLoop(queue);
            if (queue == mainEventQueue[0]) {
                // Fail here rather than at the next barrier; the main
                // thread would otherwise wait for this thread forever.
                panic_if(!event, "Event queue 0 left the simulation "
                                 "loop without an exit event.");
                exitEvent = event;
            }

            control::workerDone();
        }
    }

    uint32_t numQueues;
    std::vector<std::jthread> threads;
    std::barrier<> barrier;

    /** The event that made queue 0 exit, read by the main thread. */
    Event *exitEvent = nullptr;
};

static std::unique_ptr<SimulatorThreads> simulatorThreads;

struct DescheduleDeleter
{
    void
    operator()(GlobalSyncEvent *event)
    {
        if (!event)
            return;

        event->deschedule();
        delete event;
    }
};

/** Simulate for num_cycles additional cycles.  If num_cycles is -1
 * (the default), we simulate to MAX_TICKS unless the max ticks has been set
 * via the 'set_max_tick' function prior. This function is exported to Python.
 * @return The SimLoopExitEvent that caused the loop to exit.
 */
GlobalSimLoopExitEvent *global_exit_event= nullptr;
GlobalSimLoopExitEvent *
simulate(Tick num_cycles)
{
    // install the sigint handler to catch ctrl-c and exit the sim loop cleanly
    // Note: This should be done before initializing the threads
    initSigInt();
    initSigCont();

    if (global_exit_event)//cleaning last global exit event
        global_exit_event->clean();
    std::unique_ptr<GlobalSyncEvent, DescheduleDeleter> quantum_event;

    DPRINTF(EnteringEventQueue, "Entering event queue @ %d. Starting "
        "simulation...\n", curTick());

    if (!simulatorThreads)
        simulatorThreads.reset(new SimulatorThreads(numMainEventQueues));

    if (!simulate_limit_event) {
        // If the simulate_limit_event is not set, we set it to MaxTick.
        set_max_tick(MaxTick);
    }

    if (num_cycles != -1) {
        // If the user has specified an exit event after X cycles, do so here.
        // Note: This will override any prior set max_tick behaviour (such as
        // that above when it is set to MAxTick).
        const Tick max_tick = num_cycles < MaxTick - curTick() ?
                                    curTick() + num_cycles : MaxTick;

        // This is kept to `set_max_tick` instead of `schedule_tick_exit` to
        // preserve backwards functionality. It may be better to deprecate this
        // behaviour at some point in favor of `schedule_tick_exit`.
        set_max_tick(max_tick);
    }

    if (numMainEventQueues > 1) {
        fatal_if(simQuantum == 0,
                 "Quantum for multi-eventq simulation not specified");

        quantum_event.reset(
            new GlobalSyncEvent(curTick() + simQuantum, simQuantum,
                                EventBase::Progress_Event_Pri, 0));

        inParallelMode = true;
    }

    Event *local_event;
    if (numMainEventQueues > 1) {
        local_event = simulatorThreads->runUntilLocalExit();
        // The workers own the queues while they run. Make sure the
        // main thread sees queue 0's time when it returns to Python.
        curEventQueue(mainEventQueue[0]);
    } else {
        local_event = doSimLoop(mainEventQueue[0]);
    }
    assert(local_event);

    // Restore normal ctrl-c operation as soon as the event queue is done
    restoreSigInt();

    inParallelMode = false;

    // Finish control work and hand Python events back to event queue 0.
    // This rethrows an exception raised by control work.
    if (numMainEventQueues > 1) {
        control::end();
    }

    // locate the global exit event and return it to Python
    BaseGlobalEvent *global_event = local_event->globalEvent();
    assert(global_event);

    global_exit_event =
        dynamic_cast<GlobalSimLoopExitEvent *>(global_event);
    assert(global_exit_event);

    return global_exit_event;
}

void set_max_tick(Tick tick)
{
    if (!simulate_limit_event) {
        simulate_limit_event = new GlobalSimLoopExitEvent(
            mainEventQueue[0]->getCurTick(), "simulate() limit reached", 0, 0,
            static_cast<uint64_t>(ExitHypercall::CLASSIC_GENERATOR),
            classicGeneratorPayload("simulate() limit reached"));
    }
    simulate_limit_event->reschedule(tick);
}


Tick get_max_tick()
{
    if (!simulate_limit_event) {
        /* If the GlobalSimLoopExitEvent has not been setup, the maximum tick
         * is `MaxTick` as declared in "src/base/types.hh".
         */
        return MaxTick;
    }

    return simulate_limit_event->when();
}

void
terminateEventQueueThreads()
{
    simulatorThreads->terminateThreads();
}

/**
 * Handle the asynchronous requests flagged by signal handlers (stat
 * dump/reset, I/O, user interrupt and hypercalls). Run by event queue 0
 * in serial mode, and by the control thread with every simulation thread
 * parked in parallel mode.
 * @return True if a Python exception was flagged.
 */
bool
serviceAsyncEvents()
{
    async_event = false;
    if (async_statdump || async_statreset) {
        statistics::schedStatEvent(async_statdump, async_statreset);
        async_statdump = false;
        async_statreset = false;
    }

    if (async_io) {
        async_io = false;
        pollQueue.service();
    }

    if (async_exit) {
        async_exit = false;
        async_abort = false;
        exitSimulationLoopClassic("user interrupt received");
    }

    if (async_exception) {
        async_exception = false;
        return true;
    }
    if (async_hypercall) {
        async_hypercall = false;
        processExternalSignal();
    }
    return false;
}

/**
 * The main per-thread simulation loop. This loop is executed by all
 * simulation threads in parallel. In serial mode, the main thread runs
 * it for the only event queue.
 */
Event *
doSimLoop(EventQueue *eventq)
{
    // set the per thread current eventq pointer
    curEventQueue(eventq);
    eventq->handleAsyncInsertions();

    bool mainQueue = eventq == getEventQueue(0);

    while (1) {
        // there should always be at least one event (the SimLoopExitEvent
        // we just scheduled) in the queue
        assert(!eventq->empty());
        assert(curTick() <= eventq->nextTick() &&
               "event scheduled in the past");

        // In parallel mode the control thread handles asynchronous
        // requests while all queues are parked (see control_queue.hh).
        if (mainQueue && async_event && !inParallelMode) {
            // Take the event queue lock in case any of the service
            // routines want to schedule new events.
            std::lock_guard<EventQueue> lock(*eventq);
            if (serviceAsyncEvents()) {
                return NULL;
            }
        }

        Event *exit_event = eventq->serviceOne();
        if (exit_event != NULL) {
            return exit_event;
        }
    }

    // not reached... only exit is return on SimLoopExitEvent
}

} // namespace gem5
