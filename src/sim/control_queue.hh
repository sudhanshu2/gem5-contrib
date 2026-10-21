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

/* @file
 * The control queue and relay used in parallel (multi-queue) simulation.
 *
 * In parallel mode every main event queue runs on its own worker thread.
 * The thread that called simulate() (the one running Python) becomes the
 * control thread. It owns a separate event queue, the control queue,
 * which is not one of the main event queues. Python events and
 * asynchronous requests (signals, I/O, stat requests, hypercalls) are
 * handled on the control thread.
 *
 * Work on the control thread only runs while every simulation thread is
 * parked at a quantum barrier: the completion step of the quantum barrier
 * hands control to the control thread (the relay) and waits for it to
 * finish. During a relay the control thread may touch any simulator
 * state, and events it schedules are inserted into their queues directly.
 *
 * Control work is therefore delayed by up to one simulation quantum. A
 * Python event runs at the first quantum barrier at or after its time,
 * and curTick() inside it reads the time of that barrier, which is the
 * time of every main event queue. Anything that must happen at an exact
 * tick should be a GlobalEvent.
 *
 * Signals gem5 handles are blocked in the worker threads, so they reach
 * the control thread. A second user interrupt before the first has been
 * handled (for example, because a worker never reaches the next quantum
 * barrier) makes the control thread dump the event queues and abort.
 *
 * In serial mode (a single event queue) none of this is used.
 */

#ifndef __SIM_CONTROL_QUEUE_HH__
#define __SIM_CONTROL_QUEUE_HH__

#include <cstdint>

#include "base/types.hh"

namespace gem5
{

class Event;
class EventQueue;

namespace control
{

/** The event queue owned by the control thread. */
EventQueue *queue();

/**
 * The queue a Python event aimed at `queue` should be scheduled on: the
 * control queue for event queue 0 while a parallel simulation is running,
 * otherwise `queue` itself.
 */
EventQueue *targetQueue(EventQueue *queue);

/**
 * Moves the Python events scheduled on one queue to another, keeping
 * their times. Installed by the Python bindings, which own those events.
 * Only called while no simulation thread is running.
 */
using EventMover = void (*)(EventQueue *from, EventQueue *to);
void setEventMover(EventMover mover);

/**
 * @{
 * Used by simulate() in parallel mode. The control thread calls begin()
 * before the workers start, serveUntilDone() while they run, and end()
 * once they have all stopped. Each worker calls workerDone() after it
 * leaves its simulation loop.
 *
 * begin() moves Python events from event queue 0 to the control queue,
 * and end() runs any control events that are due and moves the rest back,
 * so Python sees its events on event queue 0 between simulate() calls.
 * end() rethrows an exception raised by control work during the run.
 */
void begin(uint32_t num_workers);
void serveUntilDone();
void end();
void workerDone();
/** @} */

/**
 * @{
 * Called from the completion step of the quantum barrier, with every
 * simulation thread parked at tick `now`. If there is control work
 * pending, relay() hands it to the control thread and returns once it is
 * done.
 */
bool relayNeeded(Tick now);
void relay(Tick now);
/** @} */

/**
 * Ask for a relay at the next quantum barrier. Safe to call from a
 * signal handler.
 */
void requestRelay();

} // namespace control
} // namespace gem5

#endif // __SIM_CONTROL_QUEUE_HH__
