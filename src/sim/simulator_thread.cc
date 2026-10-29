#include "sim/simulator_thread.hh"

#include <cassert>

#include "base/pollevent.hh"
#include "sim/async.hh"
#include "sim/cur_tick.hh"
#include "sim/eventq.hh"
#include "sim/init_signals.hh"
#include "sim/sim_exit.hh"
#include "sim/stat_control.hh"

namespace gem5
{

void
SimulatorThread::run()
{
    thread = std::jthread([this](std::stop_token token) {
        stop_token = token;
        exit_event = simulate();
    });
}

void
SimulatorThread::stop()
{
    if (thread.joinable()) {
        thread.request_stop();
    }
}

void
SimulatorThread::wait()
{
    if (thread.joinable()) {
        thread.join();
    }
}


Event *
SimulatorThread::simulate()
{
    // set the per thread current eventq pointer
    curEventQueue(queue);
    queue->handleAsyncInsertions();

    while (!stop_token.stop_requested()) {
        // there should always be at least one event (the SimLoopExitEvent
        // we just scheduled) in the queue
        assert(!queue->empty());
        assert(curTick() <= queue->nextTick() &&
               "event scheduled in the past");

        if (main_queue && async_event) {
            async_event = false;
            // Take the event queue lock in case any of the service
            // routines want to schedule new events.
            std::lock_guard<EventQueue> lock(*queue);
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
                exitSimulationLoopClassic("user interrupt received");
            }

            if (async_exception) {
                async_exception = false;
                return NULL;
            }
            if (async_hypercall) {
                async_hypercall = false;
                processExternalSignal();
            }
        }

        Event *event = queue->serviceOne();
        if (event != nullptr) {
            return event;
        }
    }

    return nullptr;
}

SimulatorThreadController::SimulatorThreadController(uint32_t _num_queues)
    : num_queues(_num_queues), started(false)
{
    for (uint32_t i = 0; i < num_queues; i++) {
        threads.push_back(SimulatorThread(mainEventQueue[i], i == 0));
    }
}

Event *
SimulatorThreadController::run()
{
    assert(!started);
    for (auto &t : threads) {
        t.run();
    }
    started = true;
    for (auto &t : threads) {
        t.wait();
    }
    Event *exit_event = threads.front().exitEvent();
    assert(exit_event != nullptr);
    return exit_event;
}

void
SimulatorThreadController::terminate()
{
    for (auto &t : threads) {
        t.stop();
    }
    for (auto &t : threads) {
        t.wait();
    }
    started = false;
}

} // namespace gem5
