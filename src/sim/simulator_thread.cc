#include "sim/simulator_thread.hh"

#include <cassert>

#include "sim/eventq.hh"

namespace gem5
{
Event *doSimLoop(EventQueue *);

SimulatorThread::SimulatorThread(EventQueue *_queue, std::barrier<> &_barrier,
                                 std::atomic<bool> &_terminate)
    : queue(_queue), barrier(_barrier), terminate(_terminate)
{}

void
SimulatorThread::start()
{
    if (!thread.joinable()) {
        thread = std::jthread([this]() { execute(); });
    }
}

void
SimulatorThread::join()
{
    if (thread.joinable()) {
        thread.join();
    }
}

SimulatorThreads::SimulatorThreads(uint32_t num_queues)
    : terminate(false), barrier(num_queues)
{
    for (uint32_t i = 1; i < num_queues; i++) {
        threads.emplace_back(mainEventQueue[i], barrier, terminate);
    }
}

SimulatorThreads::~SimulatorThreads()
{
    terminateThreads();
}

void
SimulatorThreads::runUntilLocalExit()
{
    assert(!terminate);
    for (auto &t : threads) {
        t.start();
    }
    started = true;
    barrier.arrive_and_wait();
}

void
SimulatorThreads::terminateThreads()
{
    assert(!terminate);
    if (!started) {
        return;
    }
    terminate = true;
    barrier.arrive_and_wait();
    for (auto &t : threads) {
        t.join();
    }
    terminate = false;
    started = false;
}

void
SimulatorThread::execute()
{
    barrier.arrive_and_wait();

    while (!terminate) {
        doSimLoop(queue);
        barrier.arrive_and_wait();
    }
}

} // namespace gem5
