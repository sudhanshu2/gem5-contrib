/*
 * Copyright (c) 2026 Sudhanshu Agarwal
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

#include "sim/control_queue.hh"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <exception>
#include <mutex>

#include "base/logging.hh"
#include "sim/async.hh"
#include "sim/eventq.hh"
#include "sim/global_event.hh"
#include "sim/sim_exit.hh"
#include "sim/simulate.hh"

namespace gem5
{

namespace control
{

namespace
{

EventQueue &
controlQueue()
{
    static EventQueue eq("ControlEventQueue");
    return eq;
}

EventMover eventMover = nullptr;

// Guards everything below, and is used with `cv` for the hand-offs
// between the control thread and the workers.
std::mutex mutex;
std::condition_variable cv;

// True while a parallel simulate() call is in progress.
std::atomic<bool> active{false};
// Workers still inside their simulation loop.
uint32_t running = 0;
// Set by relay() and cleared by the control thread once it is done.
bool relayRequested = false;
Tick relayTick = 0;
// An exception raised by control work, rethrown by end().
std::exception_ptr pendingException;

// Explicit relay requests (signal-safe).
std::atomic<bool> relayAsked{false};

/**
 * Run the control work for tick `now` on the control thread. Every
 * simulation thread is parked, so nothing else touches simulator state.
 * Events scheduled here go straight into their queues instead of
 * through the asynchronous insertion path.
 */
void
serviceRelay(Tick now)
{
    EventQueue &cq = controlQueue();
    bool parallel = inParallelMode;
    inParallelMode = false;
    // Every main queue is parked at `now`. Make queue 0 current so that
    // curTick() reads `now` throughout the relay; servicing the control
    // queue would otherwise move it back to each event's own time.
    EventQueue *prev = curEventQueue();
    curEventQueue(mainEventQueue[0]);
    cq.setCurTick(now);

    relayAsked = false;
    try {
        if (async_event) {
            serviceAsyncEvents();
        }
        cq.serviceEvents(now);
    } catch (...) {
        // Stop the simulation and report the error once the workers
        // have stopped; the workers are waiting for this relay.
        if (!pendingException) {
            pendingException = std::current_exception();
        }
        exitSimulationLoopClassicNow("exception in control work");
    }

    cq.setCurTick(now);
    curEventQueue(prev);
    inParallelMode = parallel;
}

} // anonymous namespace

EventQueue *
queue()
{
    return &controlQueue();
}

EventQueue *
targetQueue(EventQueue *queue)
{
    if (active && numMainEventQueues > 0 && queue == mainEventQueue[0]) {
        return &controlQueue();
    }
    return queue;
}

void
setEventMover(EventMover mover)
{
    eventMover = mover;
}

void
begin(uint32_t num_workers)
{
    GlobalSyncEvent::setQuantumHook([](Tick now) {
        if (relayNeeded(now)) {
            relay(now);
        }
    });

    // The workers have not started yet, so move Python events directly
    // rather than through the asynchronous insertion path.
    EventQueue &cq = controlQueue();
    cq.setCurTick(mainEventQueue[0]->getCurTick());
    bool parallel = inParallelMode;
    inParallelMode = false;
    if (eventMover) {
        eventMover(mainEventQueue[0], &cq);
    }
    inParallelMode = parallel;

    std::lock_guard<std::mutex> lock(mutex);
    active = true;
    running = num_workers;
    relayRequested = false;
    pendingException = nullptr;
}

void
serveUntilDone()
{
    using namespace std::chrono_literals;

    std::unique_lock<std::mutex> lock(mutex);
    while (true) {
        // Wake up regularly to check for a second user interrupt, which
        // means the first one is not being handled.
        bool woken = cv.wait_for(
            lock, 100ms, [] { return relayRequested || running == 0; });
        if (!woken) {
            if (async_abort) {
                warn("Interrupted twice before the simulation reached a "
                     "quantum barrier. Dumping the event queues and "
                     "aborting.");
                dumpMainQueue();
                std::abort();
            }
            continue;
        }
        if (!relayRequested) {
            break;
        }

        Tick now = relayTick;
        lock.unlock();
        serviceRelay(now);
        lock.lock();

        relayRequested = false;
        cv.notify_all();
    }
}

void
end()
{
    // Every worker has stopped. Run the control events that came due
    // after the last relay, then hand the rest back to event queue 0.
    Tick now = mainEventQueue[0]->getCurTick();
    serviceRelay(now);

    EventQueue &cq = controlQueue();
    if (eventMover) {
        eventMover(&cq, mainEventQueue[0]);
    }
    panic_if(!cq.empty(), "Events left on the control queue.");

    std::exception_ptr error;
    {
        std::lock_guard<std::mutex> lock(mutex);
        active = false;
        error = pendingException;
        pendingException = nullptr;
    }
    if (error) {
        std::rethrow_exception(error);
    }
}

void
workerDone()
{
    std::lock_guard<std::mutex> lock(mutex);
    if (--running == 0) {
        cv.notify_all();
    }
}

bool
relayNeeded(Tick now)
{
    if (!active) {
        return false;
    }
    if (relayAsked.load(std::memory_order_relaxed) || async_event) {
        return true;
    }
    EventQueue &cq = controlQueue();
    return !cq.empty() && cq.nextTick() <= now;
}

void
relay(Tick now)
{
    std::unique_lock<std::mutex> lock(mutex);
    panic_if(!active, "Control relay outside a parallel simulation.");
    relayTick = now;
    relayRequested = true;
    cv.notify_all();
    cv.wait(lock, [] { return !relayRequested; });
}

void
requestRelay()
{
    relayAsked.store(true, std::memory_order_relaxed);
}

} // namespace control
} // namespace gem5
